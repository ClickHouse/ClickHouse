#include <mutex>
#include <Storages/Hive/StorageHive.h>
#include "IHiveTaskPolicy.h"
#include "SingleHiveTaskFilesCollector.h"

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
#include <Storages/Hive/SingleHiveTaskFilesCollector.h>

#include </usr/local/include/gperftools/profiler.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int NOT_IMPLEMENTED;
    extern const int INVALID_PARTITION_VALUE;
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
        , sample_block(sample_block_)
        , to_read_block(sample_block_)
        , columns_description(getColumnsDescription(sample_block, source_info))
        , text_input_field_names(text_input_field_names_)
        , format_settings(getFormatSettings(getContext()))
    {
        /// Initialize to_read_block, which is used to read data from HDFS.
        for (const auto & name_type : source_info->partition_name_types)
        {
            if (to_read_block.has(name_type.name))
            {
                to_read_block.erase(name_type.name);
            }
        }
        if (!to_read_block.columns())
        {
            auto & col = sample_block.getByPosition(0);
            to_read_block.insert(0, col);
            all_to_read_are_partition_columns = true;
        }
        /// Initialize format settings
        format_settings.hive_text.input_field_names = text_input_field_names;
    }

    String getName() const override { return "Hive"; }

    Chunk generate() override
    {
        while (true)
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
                if (ExternalDataSourceCache::instance().isInitialized()
                    && getContext()->getSettingsRef().use_local_cache_for_remote_storage)
                {
                    size_t buff_size = raw_read_buf->internalBuffer().size();
                    if (buff_size == 0)
                        buff_size = DBMS_DEFAULT_BUFFER_SIZE;
                    remote_read_buf = RemoteReadBuffer::create(
                        getContext(),
                        std::make_shared<StorageHiveMetadata>(
                            "Hive", getNameNodeCluster(hdfs_namenode_url), uri_with_path, curr_file->getSize(), curr_file->getLastModTs()),
                        std::move(raw_read_buf),
                        buff_size);
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
                    builder.addSimpleTransform([&](const Block & header) {
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
                if (all_to_read_are_partition_columns)
                    columns.clear();

                /// Enrich with partition columns.
                auto types = source_info->partition_name_types.getTypes();
                for (size_t i = 0; i < types.size(); ++i)
                {
                    if (!sample_block.has(source_info->partition_name_types.getNames()[i]))
                        continue;
                    auto column = types[i]->createColumnConst(num_rows, source_info->hive_files[current_idx]->getPartitionValues()[i]);
                    auto previous_idx = sample_block.getPositionByName(source_info->partition_name_types.getNames()[i]);
                    /* The original implemetation is :
                     *     columns.insert(columns.begin() + previous_idx, column->convertToFullColumnIfConst());
                     * On profiling, We found that the cost of convertToFullColumnIfConst is too high. If we have confirmed that
                     * this column is const, there is no need to call this
                     */
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
    bool all_to_read_are_partition_columns = false;
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
    std::shared_ptr<HiveTaskFilesCollectorBuilder> hive_task_files_collector_builder_)
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
    auto hive_table_info = hive_metastore_client->getHiveTable(hive_database, hive_table);

    hdfs_namenode_url = getNameNodeUrl(hive_table_info->sd.location);
    table_schema = hive_table_info->sd.cols;

    FileFormat hdfs_file_format = IHiveFile::toFileFormat(hive_table_info->sd.inputFormat);
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
        return expr_func->arguments->clone();

    auto res = std::make_shared<ASTExpressionList>();
    res->children.push_back(node);
    return res;
}

Pipe StorageHive::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context_,
    QueryProcessingStage::Enum /* processed_stage */,
    size_t max_block_size,
    unsigned num_streams)
{
    lazyInitialize();
    // If the StorageHive is created in StorageHiveCluster, hive_files_collector must not be null
    std::shared_ptr<IHiveTaskFilesCollector> hive_task_files_collector;
    IHiveTaskFilesCollector::Arguments args
        = {.context = getContext(),
           .query_info = &query_info,
           .hive_metastore_url = hive_metastore_url,
           .hive_database = hive_database,
           .hive_table = hive_table,
           .storage_settings = storage_settings,
           .columns = getInMemoryMetadata().getColumns(),
           .num_streams = num_streams,
           .partition_by_ast = partition_by_ast};
    if (!hive_task_files_collector_builder)
    {
        hive_task_files_collector = std::make_shared<SingleHiveTaskFilesCollector>();
    }
    else
        hive_task_files_collector = (*hive_task_files_collector_builder)();
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
    Block to_read_block;
    const auto & sample_block = metadata_snapshot->getSampleBlock();
    for (const auto & column : column_names)
    {
        to_read_block.insert(sample_block.getByName(column));
        if (column == "_path")
            sources_info->need_path_column = true;
        if (column == "_file")
            sources_info->need_file_column = true;
    }

    if (format_name != "Parquet" && format_name != "ORC")
        to_read_block = sample_block;

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
            to_read_block,
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
        [](const StorageFactory::Arguments & args) {
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
