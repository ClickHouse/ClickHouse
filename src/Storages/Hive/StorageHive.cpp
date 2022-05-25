#include <Storages/Hive/StorageHive.h>

#if USE_HIVE

#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/case_conv.hpp>
#include <fmt/core.h>
#include <Poco/URI.h>
#include <Common/logger_useful.h>

#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeString.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <IO/ReadBufferFromString.h>
#include <Storages/Cache/ExternalDataSourceCache.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <QueryPipeline/Pipe.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <Storages/HDFS/ReadBufferFromHDFS.h>
#include <Storages/Hive/HiveSettings.h>
#include <Storages/Hive/StorageHiveMetadata.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/StorageFactory.h>
#include <Storages/Hive/LocalHiveSourceTask.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_OPEN_FILE;
    extern const int LOGICAL_ERROR;
    extern const int TOO_MANY_PARTITIONS;
}

class StorageHiveSource : public SourceWithProgress, WithContext
{
public:
    using FileFormat = StorageHive::FileFormat;
    struct SourcesInfo
    {
        HiveMetastoreClientPtr hive_metastore_client;
        std::string database_name;
        std::string table_name;
        HiveFiles hive_files;
        NamesAndTypesList partition_name_types;

        std::atomic<size_t> next_uri_to_read = 0;

        bool need_path_column = false;
        bool need_file_column = false;
    };

    using SourcesInfoPtr = std::shared_ptr<SourcesInfo>;

    static Block getHeader(Block header, const SourcesInfoPtr & source_info)
    {
        if (source_info->need_path_column)
            header.insert(
                {DataTypeLowCardinality{std::make_shared<DataTypeString>()}.createColumn(),
                 std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),
                 "_path"});
        if (source_info->need_file_column)
            header.insert(
                {DataTypeLowCardinality{std::make_shared<DataTypeString>()}.createColumn(),
                 std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),
                 "_file"});

        return header;
    }

    static ColumnsDescription getColumnsDescription(Block header, const SourcesInfoPtr & source_info)
    {
        ColumnsDescription columns_description{header.getNamesAndTypesList()};
        if (source_info->need_path_column)
            columns_description.add({"_path", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())});
        if (source_info->need_file_column)
            columns_description.add({"_file", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())});
        return columns_description;
    }

    StorageHiveSource(
        SourcesInfoPtr source_info_,
        String hdfs_namenode_url_,
        String format_,
        String compression_method_,
        Block sample_block_,
        ContextPtr context_,
        UInt64 max_block_size_,
        const Names & text_input_field_names_ = {})
        : SourceWithProgress(getHeader(sample_block_, source_info_))
        , WithContext(context_)
        , source_info(std::move(source_info_))
        , hdfs_namenode_url(std::move(hdfs_namenode_url_))
        , format(std::move(format_))
        , compression_method(std::move(compression_method_))
        , max_block_size(max_block_size_)
        , sample_block(std::move(sample_block_))
        , columns_description(getColumnsDescription(sample_block, source_info))
        , text_input_field_names(text_input_field_names_)
        , format_settings(getFormatSettings(getContext()))
    {
        to_read_block = sample_block;

        /// Initialize to_read_block, which is used to read data from HDFS.
        for (const auto & name_type : source_info->partition_name_types)
        {
            if (to_read_block.has(name_type.name))
                to_read_block.erase(name_type.name);
        }
    }

    FormatSettings updateFormatSettings(const HiveFilePtr & hive_file)
    {
        auto updated = format_settings;
        if (format == "HiveText")
            updated.hive_text.input_field_names = text_input_field_names;
        else if (format == "ORC")
            updated.orc.skip_stripes = hive_file->getSkipSplits();
        else if (format == "Parquet")
            updated.parquet.skip_row_groups = hive_file->getSkipSplits();
        return updated;
    }

    String getName() const override { return "Hive"; }

    Chunk generate() override
    {
        while (true)
        {
            if (!reader)
            {
                if (current_file_remained_rows)
                {
                    return generateChunkByPartitionKeys();
                }

                current_idx = source_info->next_uri_to_read.fetch_add(1);
                if (current_idx >= source_info->hive_files.size())
                    return {};

                current_file = source_info->hive_files[current_idx];
                current_path = current_file->getPath();

                if (!to_read_block.columns() && current_file->getRows())
                {
                    /// this is the case that all columns to read are partition keys. We can construct const columns
                    /// directly without reading from hive files.
                    current_file_remained_rows = *(current_file->getRows());
                    return generateChunkByPartitionKeys();
                }

                String uri_with_path = hdfs_namenode_url + current_path;
                auto compression = chooseCompressionMethod(current_path, compression_method);
                std::unique_ptr<ReadBuffer> raw_read_buf;
                try
                {
                    raw_read_buf = std::make_unique<ReadBufferFromHDFS>(
                        hdfs_namenode_url, current_path, getContext()->getGlobalContext()->getConfigRef());
                }
                catch (Exception & e)
                {
                    if (e.code() == ErrorCodes::CANNOT_OPEN_FILE)
                    {
                        source_info->hive_metastore_client->clearTableMetadata(source_info->database_name, source_info->table_name);
                        throw;
                    }
                }

                /// Use local cache for remote storage if enabled.
                std::unique_ptr<ReadBuffer> remote_read_buf;
                if (ExternalDataSourceCache::instance().isInitialized()
                    && getContext()->getSettingsRef().use_local_cache_for_remote_storage)
                {
                    size_t buff_size = raw_read_buf->internalBuffer().size();
                    if (buff_size == 0)
                        buff_size = DBMS_DEFAULT_BUFFER_SIZE;
                    remote_read_buf = RemoteReadBuffer::create(
                        getContext(),
                        std::make_shared<StorageHiveMetadata>(
                            "Hive", getNameNodeCluster(hdfs_namenode_url), uri_with_path, current_file->getSize(), current_file->getLastModifiedTimestamp()),
                        std::move(raw_read_buf),
                        buff_size,
                        format == "Parquet" || format == "ORC");
                }
                else
                    remote_read_buf = std::move(raw_read_buf);

                if (current_file->getFormat() == FileFormat::TEXT)
                    read_buf = wrapReadBufferWithCompressionMethod(std::move(remote_read_buf), compression);
                else
                    read_buf = std::move(remote_read_buf);

                auto input_format = FormatFactory::instance().getInputFormat(
                    format, *read_buf, to_read_block, getContext(), max_block_size, updateFormatSettings(current_file));

                QueryPipelineBuilder builder;
                builder.init(Pipe(input_format));
                if (columns_description.hasDefaults())
                {
                    builder.addSimpleTransform([&](const Block & header)
                    {
                        return std::make_shared<AddingDefaultsTransform>(header, columns_description, *input_format, getContext());
                    });
                }
                pipeline = std::make_unique<QueryPipeline>(QueryPipelineBuilder::getPipeline(std::move(builder)));
                reader = std::make_unique<PullingPipelineExecutor>(*pipeline);
            }

            Block res;
            if (reader->pull(res))
            {
                Columns columns = res.getColumns();
                UInt64 num_rows = res.rows();

                /// Enrich with partition columns.
                auto types = source_info->partition_name_types.getTypes();
                auto names = source_info->partition_name_types.getNames();
                auto fields = source_info->hive_files[current_idx]->getPartitionValues();
                for (size_t i = 0; i < types.size(); ++i)
                {
                    // Only add the required partition columns. partition columns are not read from readbuffer
                    // the column must be in sample_block, otherwise sample_block.getPositionByName(names[i]) will throw an exception
                    if (!sample_block.has(names[i]))
                        continue;
                    auto column = types[i]->createColumnConst(num_rows, fields[i]);
                    auto previous_idx = sample_block.getPositionByName(names[i]);
                    columns.insert(columns.begin() + previous_idx, column);
                }

                /// Enrich with virtual columns.
                if (source_info->need_path_column)
                {
                    auto column = DataTypeLowCardinality{std::make_shared<DataTypeString>()}.createColumnConst(num_rows, current_path);
                    columns.push_back(column->convertToFullColumnIfConst());
                }

                if (source_info->need_file_column)
                {
                    size_t last_slash_pos = current_path.find_last_of('/');
                    auto file_name = current_path.substr(last_slash_pos + 1);

                    auto column
                        = DataTypeLowCardinality{std::make_shared<DataTypeString>()}.createColumnConst(num_rows, std::move(file_name));
                    columns.push_back(column->convertToFullColumnIfConst());
                }
                return Chunk(std::move(columns), num_rows);
            }
            reader.reset();
            pipeline.reset();
            read_buf.reset();
        }
    }

    Chunk generateChunkByPartitionKeys()
    {
        size_t max_rows = getContext()->getSettings().max_block_size;
        size_t rows = 0;
        if (max_rows > current_file_remained_rows)
        {
            rows = current_file_remained_rows;
            current_file_remained_rows = 0;
        }
        else
        {
            rows = max_rows;
            current_file_remained_rows -= max_rows;
        }

        Columns cols;
        auto types = source_info->partition_name_types.getTypes();
        auto names = source_info->partition_name_types.getNames();
        auto fields = current_file->getPartitionValues();
        for (size_t i = 0, sz = types.size(); i < sz; ++i)
        {
            if (!sample_block.has(names[i]))
                continue;
            auto col = types[i]->createColumnConst(rows, fields[i]);
            auto col_idx = sample_block.getPositionByName(names[i]);
            cols.insert(cols.begin() + col_idx, col);
        }
        return Chunk(std::move(cols), rows);
    }

private:
    std::unique_ptr<ReadBuffer> read_buf;
    std::unique_ptr<QueryPipeline> pipeline;
    std::unique_ptr<PullingPipelineExecutor> reader;
    SourcesInfoPtr source_info;
    String hdfs_namenode_url;
    String format;
    String compression_method;
    UInt64 max_block_size;
    Block sample_block;
    Block to_read_block;
    ColumnsDescription columns_description;
    const Names & text_input_field_names;
    FormatSettings format_settings;

    HiveFilePtr current_file;
    String current_path;
    size_t current_idx = 0;
    size_t current_file_remained_rows = 0;

    Poco::Logger * log = &Poco::Logger::get("StorageHive");
};


StorageHive::StorageHive(
    const String & hive_metastore_url_,
    const String & hive_database_,
    const String & hive_table_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment_,
    const ASTPtr & partition_by_ast_,
    std::unique_ptr<HiveSettings> storage_settings_,
    ContextPtr context_,
    std::shared_ptr<HiveSourceFilesCollectorBuilder> hive_task_files_collector_builder_,
    bool is_distributed_mode_)
    : IStorage(table_id_)
    , WithContext(context_)
    , hive_metastore_url(hive_metastore_url_)
    , hive_database(hive_database_)
    , hive_table(hive_table_)
    , partition_by_ast(partition_by_ast_)
    , storage_settings(std::move(storage_settings_))
    , hive_task_files_collector_builder(hive_task_files_collector_builder_)
    , is_distributed_mode(is_distributed_mode_)
{
    /// Check hive metastore url.
    getContext()->getRemoteHostFilter().checkURL(Poco::URI(hive_metastore_url));

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment_);
    storage_metadata.partition_key = KeyDescription::getKeyFromAST(partition_by_ast, storage_metadata.columns, getContext());

    setInMemoryMetadata(storage_metadata);
}

ASTPtr StorageHive::extractKeyExpressionList(const ASTPtr & node)
{
    if (!node)
        return std::make_shared<ASTExpressionList>();

    const auto * expr_func = node->as<ASTFunction>();
    if (expr_func && expr_func->name == "tuple")
    {
        /// Primary key is specified in tuple, extract its arguments.
        return expr_func->arguments->clone();
    }
    /// Primary key consists of one column.
    auto res = std::make_shared<ASTExpressionList>();
    res->children.push_back(node);
    return res;
}

void StorageHive::lazyInitialize()
{
    std::lock_guard lock{init_mutex};
    if (has_initialized)
        return;

    auto hive_metastore_client = HiveMetastoreClientFactory::instance().getOrCreate(hive_metastore_url);
    auto hive_table_metadata = hive_metastore_client->getHiveTable(hive_database, hive_table);

    hdfs_namenode_url = getNameNodeUrl(hive_table_metadata->sd.location);
    /// Check HDFS namenode url.
    getContext()->getRemoteHostFilter().checkURL(Poco::URI(hdfs_namenode_url));

    table_schema = hive_table_metadata->sd.cols;

    format_name = IHiveFile::toCHFormat(hive_table_metadata->sd.inputFormat);
    /// Need to specify text_input_fields_names from table_schema for TextInputFormated Hive table
    if (format_name == "HiveText")
    {
        size_t i = 0;
        text_input_field_names.resize(table_schema.size());
        for (const auto & field : table_schema)
        {
            String name{field.name};
            boost::to_lower(name);
            text_input_field_names[i++] = std::move(name);
        }
    }

    ASTPtr partition_key_expr_list = extractKeyExpressionList(partition_by_ast);
    NamesAndTypesList all_name_and_types = getInMemoryMetadata().getColumns().getAllPhysical();
    if (!partition_key_expr_list->children.empty())
    {
        auto syntax_result = TreeRewriter(getContext()).analyze(partition_key_expr_list, all_name_and_types);
        auto partition_key_expr = ExpressionAnalyzer(partition_key_expr_list, syntax_result, getContext()).getActions(false);
        partition_name_types = partition_key_expr->getRequiredColumnsWithTypes();
    }

    has_initialized = true;
}

bool StorageHive::isColumnOriented() const
{
    return format_name == "Parquet" || format_name == "ORC";
}

Pipe StorageHive::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context_,
    QueryProcessingStage::Enum /* processed_stage */,
    size_t max_block_size,
    unsigned num_streams)
{
    lazyInitialize();

    auto hive_metastore_client = HiveMetastoreClientFactory::instance().getOrCreate(hive_metastore_url);
    auto hive_files_collector = getHiveFilesCollector(query_info);
    auto hive_files = hive_files_collector->collect(PruneLevel::Max);
    if (hive_files.empty())
        return {};

    auto sources_info = std::make_shared<StorageHiveSource::SourcesInfo>();
    sources_info->hive_files = std::move(hive_files);
    sources_info->database_name = hive_database;
    sources_info->table_name = hive_table;
    sources_info->hive_metastore_client = hive_metastore_client;
    sources_info->partition_name_types = partition_name_types;

    const auto & header_block = storage_snapshot->metadata->getSampleBlock();
    Block sample_block;
    for (const auto & column : column_names)
    {
        sample_block.insert(header_block.getByName(column));
        if (column == "_path")
            sources_info->need_path_column = true;
        if (column == "_file")
            sources_info->need_file_column = true;
    }

    if (num_streams > sources_info->hive_files.size())
        num_streams = sources_info->hive_files.size();

    Pipes pipes;
    for (size_t i = 0; i < num_streams; ++i)
    {
        pipes.emplace_back(std::make_shared<StorageHiveSource>(
            sources_info,
            hdfs_namenode_url,
            format_name,
            compression_method,
            sample_block,
            context_,
            max_block_size,
            text_input_field_names));
    }
    return Pipe::unitePipes(std::move(pipes));
}

SinkToStoragePtr StorageHive::write(const ASTPtr & /*query*/, const StorageMetadataPtr & /* metadata_snapshot*/, ContextPtr /*context*/)
{
    throw Exception("Method write is not implemented for StorageHive", ErrorCodes::NOT_IMPLEMENTED);
}

NamesAndTypesList StorageHive::getVirtuals() const
{
    return NamesAndTypesList{
        {"_path", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
        {"_file", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())}};
}

std::optional<UInt64> StorageHive::totalRows(const Settings & /*settings*/) const
{
    // In hive cluster query, this cannot work
    if (is_distributed_mode)
        return {};
    /// query_info is not used when prune_level == PruneLevel::None
    SelectQueryInfo query_info;
    return totalRowsImpl(query_info, PruneLevel::None);
}

std::optional<UInt64> StorageHive::totalRowsByPartitionPredicate(const SelectQueryInfo & query_info, ContextPtr /*context_*/) const
{
    return totalRowsImpl(query_info, PruneLevel::Partition);
}

std::optional<UInt64>
StorageHive::totalRowsImpl(const SelectQueryInfo & query_info, PruneLevel prune_level) const
{
    /// Row-based format like Text doesn't support totalRowsByPartitionPredicate
    if (!isColumnOriented())
        return {};

    auto hive_files_collector = getHiveFilesCollector(query_info);
    auto hive_files = hive_files_collector->collect(prune_level);

    UInt64 total_rows = 0;
    for (const auto & hive_file : hive_files)
    {
        auto file_rows = hive_file->getRows();
        if (!file_rows)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Rows of hive file:{} with format:{} not initialized", hive_file->getPath(), format_name);
        total_rows += *file_rows;
    }
    return total_rows;
}

std::shared_ptr<IHiveSourceFilesCollector> StorageHive::getHiveFilesCollector(const SelectQueryInfo & query_info) const
{
    std::shared_ptr<IHiveSourceFilesCollector> hive_task_files_collector;
    /**
     * Hdfs files collection action is wrapped into IHiveSourceFilesCollector.
     * On Hive() engine, hive_task_files_collector_builder is nullptr.
     * LocalHiveSourceFilesCollector will collect all files.
     *
     */
    if (!hive_task_files_collector_builder)
    {
        hive_task_files_collector = std::make_shared<LocalHiveSourceFilesCollector>();
        IHiveSourceFilesCollector::Arguments args
            = {.context = getContext(),
               .query_info = &query_info,
               .hive_metastore_url = hive_metastore_url,
               .hive_database = hive_database,
               .hive_table = hive_table,
               .storage_settings = storage_settings,
               .columns = getInMemoryMetadata().getColumns(),
               .num_streams = getContext()->getSettingsRef().max_threads,
               .partition_by_ast = partition_by_ast};
        hive_task_files_collector->initialize(args);
    }
    else
        hive_task_files_collector = (*hive_task_files_collector_builder)();
    return hive_task_files_collector;
}


void registerStorageHive(StorageFactory & factory)
{
    factory.registerStorage(
        "Hive",
        [](const StorageFactory::Arguments & args)
        {
            bool have_settings = args.storage_def->settings;
            std::unique_ptr<HiveSettings> hive_settings = std::make_unique<HiveSettings>();
            if (have_settings)
                hive_settings->loadFromQuery(*args.storage_def);

            ASTs & engine_args = args.engine_args;
            if (engine_args.size() != 3)
                throw Exception(
                    "Storage Hive requires 3 arguments: hive metastore address, hive database and hive table",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

            auto * partition_by = args.storage_def->partition_by;
            if (!partition_by)
                throw Exception("Storage Hive requires partition by clause", ErrorCodes::BAD_ARGUMENTS);

            for (auto & engine_arg : engine_args)
                engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, args.getLocalContext());

            const String & hive_metastore_url = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();
            const String & hive_database = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
            const String & hive_table = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
            return std::make_shared<StorageHive>(
                hive_metastore_url,
                hive_database,
                hive_table,
                args.table_id,
                args.columns,
                args.constraints,
                args.comment,
                partition_by->ptr(),
                std::move(hive_settings),
                args.getContext());
        },
        StorageFactory::StorageFeatures{
            .supports_settings = true,
            .supports_sort_order = true,
            .source_access_type = AccessType::HIVE,
        });
}

}
#endif
