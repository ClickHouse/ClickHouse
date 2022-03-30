#include <Storages/Hive/StorageHive.h>

#if USE_HIVE

#include <fmt/core.h>
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/case_conv.hpp>
#include <Poco/URI.h>

#include <base/logger_useful.h>
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
#include <Storages/Hive/SingleHiveQueryTaskBuilder.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_OPEN_FILE;
}
class StorageHiveSource : public SourceWithProgress, WithContext
{
public:
    using FileFormat = StorageHive::FileFormat;
    struct SourcesInfo
    {
        HiveMetastoreClientPtr hive_metastore_client;
        std::string database;
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
        , hdfs_namenode_url(hdfs_namenode_url_)
        , format(std::move(format_))
        , compression_method(compression_method_)
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

        /// Initialize format settings
        format_settings.hive_text.input_field_names = text_input_field_names;
    }

    String getName() const override { return "Hive"; }

    String getID() const
    {
        return std::to_string(reinterpret_cast<std::uintptr_t>(this));
    }

    Chunk generate() override
    {
        while (!isCancelled())
        {
            if (!reader)
            {
                current_idx = source_info->next_uri_to_read.fetch_add(1);
                if (current_idx >= source_info->hive_files.size())
                    return {};

                const auto & curr_file = source_info->hive_files[current_idx];
                current_path = curr_file->getPath();

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
                        source_info->hive_metastore_client->clearTableMetadata(source_info->database, source_info->table_name);
                        throw;
                    }
                }

                /// Use local cache for remote storage if enabled.
                std::unique_ptr<ReadBuffer> remote_read_buf;
                if (ExternalDataSourceCache::instance().isInitialized() && getContext()->getSettingsRef().use_local_cache_for_remote_storage)
                {
                    size_t buff_size = raw_read_buf->internalBuffer().size();
                    if (buff_size == 0)
                        buff_size = DBMS_DEFAULT_BUFFER_SIZE;
                    remote_read_buf = RemoteReadBuffer::create(
                        getContext(),
                        std::make_shared<StorageHiveMetadata>(
                            "Hive", getNameNodeCluster(hdfs_namenode_url), uri_with_path, curr_file->getSize(), curr_file->getLastModTs()),
                        std::move(raw_read_buf),
                        buff_size,
                        format == "Parquet" || format == "ORC");
                }
                else
                    remote_read_buf = std::move(raw_read_buf);

                if (curr_file->getFormat() == FileFormat::TEXT)
                    read_buf = wrapReadBufferWithCompressionMethod(std::move(remote_read_buf), compression);
                else
                    read_buf = std::move(remote_read_buf);

                auto input_format = FormatFactory::instance().getInputFormat(
                    format, *read_buf, to_read_block, getContext(), max_block_size, format_settings);

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

            {
                std::lock_guard lock(reader_mutex);
                reader.reset();
                pipeline.reset();
                read_buf.reset();
            }
        }
        return {};
    }

    void onCancel() override
    {
        std::lock_guard lock(reader_mutex);
        if (reader)
            reader->cancel();
    }

private:
    std::unique_ptr<ReadBuffer> read_buf;
    std::unique_ptr<QueryPipeline> pipeline;
    std::unique_ptr<PullingPipelineExecutor> reader;
    /// onCancel and generate can be called concurrently
    std::mutex reader_mutex;

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

    String current_path;
    size_t current_idx = 0;

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
    std::shared_ptr<HiveQueryTaskFilesCollectorBuilder> hive_task_files_collector_builder_)
    : IStorage(table_id_)
    , WithContext(context_)
    , hive_metastore_url(hive_metastore_url_)
    , hive_database(hive_database_)
    , hive_table(hive_table_)
    , partition_by_ast(partition_by_ast_)
    , storage_settings(std::move(storage_settings_))
    , hive_task_files_collector_builder(hive_task_files_collector_builder_)
{
    getContext()->getRemoteHostFilter().checkURL(Poco::URI(hive_metastore_url));

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment_);
    setInMemoryMetadata(storage_metadata);
}

void StorageHive::lazyInitialize()
{
    std::lock_guard lock{init_mutex};
    if (has_initialized)
        return;


    auto hive_metastore_client = HiveMetastoreClientFactory::instance().getOrCreate(hive_metastore_url, getContext());
    auto hive_table_metadata = hive_metastore_client->getHiveTable(hive_database, hive_table);

    hdfs_namenode_url = getNameNodeUrl(hive_table_metadata->sd.location);
    table_schema = hive_table_metadata->sd.cols;

    FileFormat hdfs_file_format = IHiveFile::toFileFormat(hive_table_metadata->sd.inputFormat);
    switch (hdfs_file_format)
    {
        case FileFormat::TEXT:
        case FileFormat::LZO_TEXT:
            format_name = "HiveText";
            break;
        case FileFormat::RC_FILE:
            /// TODO to be implemented
            throw Exception("Unsopported hive format rc_file", ErrorCodes::NOT_IMPLEMENTED);
        case FileFormat::SEQUENCE_FILE:
            /// TODO to be implemented
            throw Exception("Unsopported hive format sequence_file", ErrorCodes::NOT_IMPLEMENTED);
        case FileFormat::AVRO:
            format_name = "Avro";
            break;
        case FileFormat::PARQUET:
            format_name = "Parquet";
            break;
        case FileFormat::ORC:
            format_name = "ORC";
            break;
    }

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

bool StorageHive::isColumnOriented() const
{
    return format_name == "Parquet" || format_name == "ORC";
}

void StorageHive::getActualColumnsToRead(Block & sample_block, const Block & header_block, const NameSet & partition_columns) const
{
    if (!isColumnOriented())
        sample_block = header_block;
    UInt32 erased_columns = 0;
    for (const auto & column : partition_columns)
    {
        if (sample_block.has(column))
            erased_columns++;
    }
    if (erased_columns == sample_block.columns())
    {
        for (size_t i = 0; i < header_block.columns(); ++i)
        {
            const auto & col = header_block.getByPosition(i);
            if (!partition_columns.count(col.name))
            {
                sample_block.insert(col);
                break;
            }
        }
    }
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
    std::shared_ptr<IHiveQueryTaskFilesCollector> hive_task_files_collector;
    IHiveQueryTaskFilesCollector::Arguments args
        = {.context = getContext(),
           .query_info = &query_info,
           .hive_metastore_url = hive_metastore_url,
           .hive_database = hive_database,
           .hive_table = hive_table,
           .storage_settings = storage_settings,
           .columns = getInMemoryMetadata().getColumns(),
           .num_streams = num_streams,
           .partition_by_ast = partition_by_ast};
    /**
     * Hdfs files collection action is wrapped into IHiveQueryTaskFilesCollector.
     * On Hive() engine, hive_task_files_collector_builder is nullptr.
     * SingleHiveQueryTaskFilesCollector will collect all files.
     *
     */
    if (!hive_task_files_collector_builder)
    {
        hive_task_files_collector = std::make_shared<SingleHiveQueryTaskFilesCollector>();
    }
    else
        hive_task_files_collector = (*hive_task_files_collector_builder)();

    // Pass all infos into hive_task_files_collector that be needed to collect hdfs files
    hive_task_files_collector->setupArgs(args);
    /// Hive files to read
    HiveFiles hive_files = hive_task_files_collector->collectHiveFiles();

    auto hive_metastore_client = HiveMetastoreClientFactory::instance().getOrCreate(hive_metastore_url, getContext());

    auto sources_info = std::make_shared<StorageHiveSource::SourcesInfo>();
    sources_info->hive_files = std::move(hive_files);
    sources_info->database = hive_database;
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

    auto partition_names = partition_name_types.getNames();
    getActualColumnsToRead(sample_block, header_block, NameSet{partition_names.begin(), partition_names.end()});

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
            return StorageHive::create(
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
        });
}

}
#endif
