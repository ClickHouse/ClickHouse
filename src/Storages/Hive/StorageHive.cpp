#include <Storages/Hive/StorageHive.h>

#if USE_HIVE

#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/case_conv.hpp>
#include <fmt/core.h>
#include <Poco/URI.h>
#include <Common/logger_useful.h>
#include <Common/CurrentMetrics.h>
#include <Common/RemoteHostFilter.h>

#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <Core/Settings.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/NestedUtils.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/Context.h>
#include <IO/ReadBufferFromString.h>
#include <Disks/IO/getThreadPoolReader.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/ISource.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/Sources/NullSource.h>
#include <Storages/AlterCommands.h>
#include <Storages/ObjectStorage/HDFS/ReadBufferFromHDFS.h>
#include <Storages/ObjectStorage/HDFS/AsynchronousReadBufferFromHDFS.h>
#include <Storages/Hive/HiveSettings.h>
#include <Storages/Hive/StorageHiveMetadata.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/StorageFactory.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/VirtualColumnUtils.h>

namespace CurrentMetrics
{
    extern const Metric StorageHiveThreads;
    extern const Metric StorageHiveThreadsActive;
    extern const Metric StorageHiveThreadsScheduled;
}

namespace DB
{
namespace Setting
{
    extern const SettingsBool input_format_parquet_case_insensitive_column_matching;
    extern const SettingsBool input_format_orc_case_insensitive_column_matching;
    extern const SettingsNonZeroUInt64 max_block_size;
    extern const SettingsInt64 max_partitions_to_read;
    extern const SettingsMaxThreads max_threads;
}

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int NOT_IMPLEMENTED;
    extern const int INVALID_PARTITION_VALUE;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_OPEN_FILE;
    extern const int LOGICAL_ERROR;
    extern const int TOO_MANY_PARTITIONS;
    extern const int THERE_IS_NO_COLUMN;
}


static std::string getBaseName(const String & path)
{
    size_t basename_start = path.rfind('/');
    return path.substr(basename_start + 1);
}

class StorageHiveSource : public ISource, WithContext
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
        const StorageHive & storage_,
        const Names & text_input_field_names_ = {})
        : ISource(getHeader(sample_block_, source_info_))
        , WithContext(context_)
        , source_info(std::move(source_info_))
        , hdfs_namenode_url(std::move(hdfs_namenode_url_))
        , format(std::move(format_))
        , compression_method(std::move(compression_method_))
        , max_block_size(max_block_size_)
        , sample_block(std::move(sample_block_))
        , columns_description(getColumnsDescription(sample_block, source_info))
        , storage(storage_)
        , text_input_field_names(text_input_field_names_)
        , format_settings(getFormatSettings(getContext()))
        , read_settings(getContext()->getReadSettings())
    {
        to_read_block = sample_block;

        /// Initialize to_read_block, which is used to read data from HDFS.
        for (const auto & name_type : source_info->partition_name_types)
        {
            if (to_read_block.has(name_type.name))
                to_read_block.erase(name_type.name);
        }

        /// Apply read buffer prefetch for HiveText format, because it is read sequentially
        if (read_settings.remote_fs_prefetch)
            read_settings.remote_fs_prefetch = format == "HiveText";

        /// Decide if we could generate blocks from partition values
        /// Only for ORC or Parquet format file, we could get number of rows from metadata without scanning the whole file
        generate_chunk_from_metadata = (format == "ORC" || format == "Parquet") && !to_read_block.columns();

        /// Make sure to_read_block is not empty. Otherwise input format would always return empty chunk.
        /// See issue: https://github.com/ClickHouse/ClickHouse/issues/37671
        if (!generate_chunk_from_metadata && !to_read_block.columns())
        {
            const auto & metadata = storage.getInMemoryMetadataPtr();
            for (const auto & column : metadata->getColumns().getAllPhysical())
            {
                bool is_partition_column = false;
                for (const auto & partition_column : source_info->partition_name_types)
                {
                    if (partition_column.name == column.name)
                    {
                        is_partition_column = true;
                        break;
                    }
                }

                if (!is_partition_column)
                    to_read_block.insert(ColumnWithTypeAndName(column.type, column.name));
            }
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
            bool need_next_file
                = (!generate_chunk_from_metadata && !reader) || (generate_chunk_from_metadata && !current_file_remained_rows);
            if (need_next_file)
            {
                current_idx = source_info->next_uri_to_read.fetch_add(1);
                if (current_idx >= source_info->hive_files.size())
                    return {};

                current_file = source_info->hive_files[current_idx];
                current_path = current_file->getPath();

                /// This is the case that all columns to read are partition keys. We can construct const columns
                /// directly without reading from hive files.
                if (generate_chunk_from_metadata && current_file->getRows())
                {
                    current_file_remained_rows = *(current_file->getRows());
                    return generateChunkFromMetadata();
                }

                auto compression = chooseCompressionMethod(current_path, compression_method);
                std::unique_ptr<ReadBuffer> raw_read_buf;
                try
                {
                    auto get_raw_read_buf = [&]() -> std::unique_ptr<ReadBuffer>
                    {
                        bool thread_pool_read = read_settings.remote_fs_method == RemoteFSReadMethod::threadpool;
                        if (thread_pool_read)
                        {
                            auto buf = std::make_unique<ReadBufferFromHDFS>(
                                hdfs_namenode_url,
                                current_path,
                                getContext()->getGlobalContext()->getConfigRef(),
                                getContext()->getReadSettings(),
                                /* read_until_position */0,
                                /* use_external_buffer */true);

                            return std::make_unique<AsynchronousReadBufferFromHDFS>(
                                getThreadPoolReader(FilesystemReaderType::ASYNCHRONOUS_REMOTE_FS_READER), read_settings, std::move(buf));
                        }

                        return std::make_unique<ReadBufferFromHDFS>(
                            hdfs_namenode_url,
                            current_path,
                            getContext()->getGlobalContext()->getConfigRef(),
                            getContext()->getReadSettings());

                    };

                    raw_read_buf = get_raw_read_buf();
                    if (read_settings.remote_fs_prefetch)
                        raw_read_buf->prefetch(DEFAULT_PREFETCH_PRIORITY);
                }
                catch (const Exception & e)
                {
                    if (e.code() == ErrorCodes::CANNOT_OPEN_FILE)
                        source_info->hive_metastore_client->clearTableMetadata(source_info->database_name, source_info->table_name);
                    throw;
                }

                if (current_file->getFormat() == FileFormat::TEXT)
                    read_buf = wrapReadBufferWithCompressionMethod(std::move(raw_read_buf), compression);
                else
                    read_buf = std::move(raw_read_buf);

                ContextPtr context = getContext();

                auto input_format = FormatFactory::instance().getInput(
                    format,
                    *read_buf,
                    to_read_block,
                    context,
                    max_block_size,
                    updateFormatSettings(current_file),
                    FormatParserGroup::singleThreaded(context->getSettingsRef()));

                Pipe pipe(input_format);
                if (columns_description.hasDefaults())
                {
                    pipe.addSimpleTransform([&](const Block & header)
                    {
                        return std::make_shared<AddingDefaultsTransform>(header, columns_description, *input_format, context);
                    });
                }
                pipeline = std::make_unique<QueryPipeline>(std::move(pipe));
                reader = std::make_unique<PullingPipelineExecutor>(*pipeline);
            }

            if (generate_chunk_from_metadata)
                return generateChunkFromMetadata();

            Block source_block;
            if (reader->pull(source_block))
            {
                auto num_rows = source_block.rows();
                return getResultChunk(source_block, num_rows);
            }

            reader.reset();
            pipeline.reset();
            read_buf.reset();
        }
    }

    Chunk generateChunkFromMetadata()
    {
        size_t num_rows = std::min(current_file_remained_rows, UInt64(getContext()->getSettingsRef()[Setting::max_block_size]));
        current_file_remained_rows -= num_rows;

        Block source_block;
        return getResultChunk(source_block, num_rows);
    }

    Chunk getResultChunk(const Block & source_block, UInt64 num_rows) const
    {
        Columns source_columns = source_block.getColumns();

        const auto & result_header = getPort().getHeader();
        Columns result_columns;
        result_columns.reserve(result_header.columns());
        for (const auto & column : result_header)
        {
            if (source_block.has(column.name))
            {
                result_columns.emplace_back(std::move(source_columns[source_block.getPositionByName(column.name)]));
                continue;
            }

            // Enrich virtual column _path
            if (column.name == "_path")
            {
                auto path_column = DataTypeLowCardinality{std::make_shared<DataTypeString>()}.createColumnConst(num_rows, current_path);
                result_columns.emplace_back(path_column->convertToFullColumnIfConst());
                continue;
            }

            /// Enrich virtual column _file
            if (column.name == "_file")
            {
                size_t last_slash_pos = current_path.find_last_of('/');
                auto file_name = current_path.substr(last_slash_pos + 1);

                auto file_column
                    = DataTypeLowCardinality{std::make_shared<DataTypeString>()}.createColumnConst(num_rows, std::move(file_name));
                result_columns.emplace_back(file_column->convertToFullColumnIfConst());
                continue;
            }

            /// Enrich partition columns
            const auto names = source_info->partition_name_types.getNames();
            size_t pos = names.size();
            for (size_t i = 0; i < names.size(); ++i)
            {
                if (column.name == names[i])
                {
                    pos = i;
                    break;
                }
            }
            if (pos != names.size())
            {
                const auto types = source_info->partition_name_types.getTypes();
                const auto & fields = current_file->getPartitionValues();
                auto partition_column = types[pos]->createColumnConst(num_rows, fields[pos]);
                result_columns.emplace_back(partition_column->convertToFullColumnIfConst());
                continue;
            }

            throw Exception{ErrorCodes::THERE_IS_NO_COLUMN, "Column '{}' is not presented in input data.", column.name};
        }
        return Chunk(std::move(result_columns), num_rows);
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
    const StorageHive & storage;
    const Names & text_input_field_names;
    FormatSettings format_settings;
    ReadSettings read_settings;

    HiveFilePtr current_file;
    String current_path;
    size_t current_idx = 0;

    bool generate_chunk_from_metadata{false};
    UInt64 current_file_remained_rows = 0;

    LoggerPtr log = getLogger("StorageHive");
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
    ContextPtr context_)
    : IStorage(table_id_)
    , WithContext(context_)
    , hive_metastore_url(hive_metastore_url_)
    , hive_database(hive_database_)
    , hive_table(hive_table_)
    , partition_by_ast(partition_by_ast_)
    , storage_settings(std::move(storage_settings_))
{
    /// Check hive metastore url.
    getContext()->getRemoteHostFilter().checkURL(Poco::URI(hive_metastore_url));

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment_);
    storage_metadata.partition_key = KeyDescription::getKeyFromAST(partition_by_ast, storage_metadata.columns, getContext());

    setVirtuals(VirtualColumnUtils::getVirtualsForFileLikeStorage(storage_metadata.columns, getContext()));
    setInMemoryMetadata(storage_metadata);
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

    FileFormat hdfs_file_format = IHiveFile::toFileFormat(hive_table_metadata->sd.inputFormat);
    switch (hdfs_file_format)
    {
        case FileFormat::TEXT:
        case FileFormat::LZO_TEXT:
            format_name = "HiveText";
            break;
        case FileFormat::RC_FILE:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsopported hive format rc_file");
        case FileFormat::SEQUENCE_FILE:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsopported hive format sequence_file");
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

    initMinMaxIndexExpression();
    has_initialized = true;
}

void StorageHive::initMinMaxIndexExpression()
{
    auto metadata_snapshot = getInMemoryMetadataPtr();
    ASTPtr partition_key_expr_list = extractKeyExpressionList(partition_by_ast);
    if (!partition_key_expr_list->children.empty())
    {
        auto syntax_result = TreeRewriter(getContext()).analyze(partition_key_expr_list, metadata_snapshot->getColumns().getAllPhysical());
        partition_key_expr = ExpressionAnalyzer(partition_key_expr_list, syntax_result, getContext()).getActions(false);

        /// Add all columns used in the partition key to the min-max index.
        partition_name_types = partition_key_expr->getRequiredColumnsWithTypes();
        partition_names = partition_name_types.getNames();
        partition_types = partition_name_types.getTypes();
        partition_minmax_idx_expr = std::make_shared<ExpressionActions>(
            ActionsDAG(partition_name_types), ExpressionActionsSettings(getContext()));
    }

    NamesAndTypesList all_name_types = metadata_snapshot->getColumns().getAllPhysical();
    for (const auto & column : all_name_types)
    {
        if (!partition_name_types.contains(column.name))
            hivefile_name_types.push_back(column);
    }
    hivefile_minmax_idx_expr = std::make_shared<ExpressionActions>(
        ActionsDAG(hivefile_name_types), ExpressionActionsSettings(getContext()));
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


static HiveFilePtr createHiveFile(
    const String & format_name,
    const FieldVector & fields,
    const String & namenode_url,
    const String & path,
    UInt64 ts,
    size_t size,
    const NamesAndTypesList & index_names_and_types,
    const std::shared_ptr<HiveSettings> & hive_settings,
    const ContextPtr & context)
{
    HiveFilePtr hive_file;
    if (format_name == "HiveText")
    {
        hive_file = std::make_shared<HiveTextFile>(fields, namenode_url, path, ts, size, index_names_and_types, hive_settings, context);
    }
    else if (format_name == "ORC")
    {
        hive_file = std::make_shared<HiveORCFile>(fields, namenode_url, path, ts, size, index_names_and_types, hive_settings, context);
    }
    else if (format_name == "Parquet")
    {
        hive_file = std::make_shared<HiveParquetFile>(fields, namenode_url, path, ts, size, index_names_and_types, hive_settings, context);
    }
    else
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "IHiveFile not implemented for format {}", format_name);
    }
    return hive_file;
}

HiveFiles StorageHive::collectHiveFilesFromPartition(
    const Apache::Hadoop::Hive::Partition & partition,
    const ActionsDAG * filter_actions_dag,
    const HiveTableMetadataPtr & hive_table_metadata,
    const HDFSFSPtr & fs,
    const ContextPtr & context_,
    PruneLevel prune_level) const
{
    LOG_DEBUG(
        log, "Collect hive files from partition {}, prune_level:{}", boost::join(partition.values, ","), pruneLevelToString(prune_level));

    /// Skip partition "__HIVE_DEFAULT_PARTITION__"
    bool has_default_partition = false;
    for (const auto & value : partition.values)
    {
        if (value == "__HIVE_DEFAULT_PARTITION__")
        {
            has_default_partition = true;
            break;
        }
    }
    if (has_default_partition)
        return {};

    /// Check partition values
    if (partition.values.size() != partition_names.size())
        throw Exception(ErrorCodes::INVALID_PARTITION_VALUE,
            "Partition value size not match, expect {}, but got {}", partition_names.size(), partition.values.size());

    /// Join partition values in CSV format
    WriteBufferFromOwnString wb;
    for (size_t i = 0; i < partition.values.size(); ++i)
    {
        if (i != 0)
            writeString(",", wb);
        writeString(partition.values[i], wb);
    }
    writeString("\n", wb);

    ReadBufferFromString buffer(wb.str());
    ContextPtr context = getContext();
    auto format = FormatFactory::instance().getInput(
        "CSV",
        buffer,
        partition_key_expr->getSampleBlock(),
        context,
        context->getSettingsRef()[Setting::max_block_size],
        std::nullopt,
        FormatParserGroup::singleThreaded(context->getSettingsRef()));
    auto pipeline = QueryPipeline(std::move(format));
    auto reader = std::make_unique<PullingPipelineExecutor>(pipeline);
    Block block;
    if (!reader->pull(block) || !block.rows())
        throw Exception(ErrorCodes::INVALID_PARTITION_VALUE, "Could not parse partition value: {}", wb.str());

    /// Get partition values
    FieldVector fields(partition_names.size());
    for (size_t i = 0; i < partition_names.size(); ++i)
        block.getByPosition(i).column->get(0, fields[i]);

    if (prune_level >= PruneLevel::Partition)
    {
        std::vector<Range> ranges;
        ranges.reserve(partition_names.size());
        for (size_t i = 0; i < partition_names.size(); ++i)
            ranges.emplace_back(fields[i]);

        ActionsDAGWithInversionPushDown inverted_dag(filter_actions_dag->getOutputs().front(), context_);
        const KeyCondition partition_key_condition(inverted_dag, context, partition_names, partition_minmax_idx_expr);
        if (!partition_key_condition.checkInHyperrectangle(ranges, partition_types).can_be_true)
            return {};
    }

    HiveFiles hive_files;
    auto file_infos = listDirectory(partition.sd.location, hive_table_metadata, fs);
    hive_files.reserve(file_infos.size());
    for (const auto & file_info : file_infos)
    {
        auto hive_file = getHiveFileIfNeeded(file_info, fields, filter_actions_dag, hive_table_metadata, context_, prune_level);
        if (hive_file)
        {
            LOG_TRACE(
                log,
                "Append hive file {} from partition {}, prune_level:{}",
                hive_file->getPath(),
                boost::join(partition.values, ","),
                pruneLevelToString(prune_level));
            hive_files.push_back(hive_file);
        }
    }
    return hive_files;
}

std::vector<StorageHive::FileInfo>
StorageHive::listDirectory(const String & path, const HiveTableMetadataPtr & hive_table_metadata, const HDFSFSPtr & fs)
{
    return hive_table_metadata->getFilesByLocation(fs, path);
}

HiveFilePtr StorageHive::getHiveFileIfNeeded(
    const FileInfo & file_info,
    const FieldVector & fields,
    const ActionsDAG * filter_actions_dag,
    const HiveTableMetadataPtr & hive_table_metadata,
    const ContextPtr & context_,
    PruneLevel prune_level) const
{
    String filename = getBaseName(file_info.path);
    /// Skip temporary files starts with '.'
    if (startsWith(filename, "."))
        return {};

    auto cache = hive_table_metadata->getHiveFilesCache();
    auto hive_file = cache->get(file_info.path);
    if (!hive_file || hive_file->getLastModTs() < file_info.last_modify_time)
    {
        LOG_TRACE(log, "Create hive file {}, prune_level {}", file_info.path, pruneLevelToString(prune_level));
        hive_file = createHiveFile(
            format_name,
            fields,
            hdfs_namenode_url,
            file_info.path,
            file_info.last_modify_time,
            file_info.size,
            hivefile_name_types,
            storage_settings,
            context_->getGlobalContext());
        cache->set(file_info.path, hive_file);
    }
    else
    {
        LOG_TRACE(log, "Get hive file {} from cache, prune_level {}", file_info.path, pruneLevelToString(prune_level));
    }

    if (prune_level >= PruneLevel::File)
    {
        ActionsDAGWithInversionPushDown inverted_dag(filter_actions_dag->getOutputs().front(), context_);
        const KeyCondition hivefile_key_condition(inverted_dag, getContext(), hivefile_name_types.getNames(), hivefile_minmax_idx_expr);
        if (hive_file->useFileMinMaxIndex())
        {
            /// Load file level minmax index and apply
            hive_file->loadFileMinMaxIndex();
            if (!hivefile_key_condition.checkInHyperrectangle(hive_file->getMinMaxIndex()->hyperrectangle, hivefile_name_types.getTypes())
                     .can_be_true)
            {
                LOG_TRACE(
                    log,
                    "Skip hive file {} by index {}",
                    hive_file->getPath(),
                    hive_file->describeMinMaxIndex(hive_file->getMinMaxIndex()));
                return {};
            }
        }

        if (prune_level >= PruneLevel::Split)
        {
            if (hive_file->useSplitMinMaxIndex())
            {
                /// Load sub-file level minmax index and apply
                std::unordered_set<int> skip_splits;
                hive_file->loadSplitMinMaxIndexes();
                const auto & sub_minmax_idxes = hive_file->getSubMinMaxIndexes();
                for (size_t i = 0; i < sub_minmax_idxes.size(); ++i)
                {
                    if (!hivefile_key_condition.checkInHyperrectangle(sub_minmax_idxes[i]->hyperrectangle, hivefile_name_types.getTypes())
                             .can_be_true)
                    {
                        LOG_TRACE(
                            log,
                            "Skip split {} of hive file {} by index {}",
                            i,
                            hive_file->getPath(),
                            hive_file->describeMinMaxIndex(sub_minmax_idxes[i]));

                        skip_splits.insert(static_cast<int>(i));
                    }
                }
                hive_file->setSkipSplits(skip_splits);
            }
        }
    }
    return hive_file;
}

bool StorageHive::supportsSubsetOfColumns() const
{
    return format_name == "Parquet" || format_name == "ORC";
}

class ReadFromHive : public SourceStepWithFilter
{
public:
    std::string getName() const override { return "ReadFromHive"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    ReadFromHive(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        Block header,
        std::shared_ptr<StorageHive> storage_,
        std::shared_ptr<StorageHiveSource::SourcesInfo> sources_info_,
        HDFSBuilderWrapper builder_,
        HDFSFSPtr fs_,
        HiveMetastoreClient::HiveTableMetadataPtr hive_table_metadata_,
        Block sample_block_,
        LoggerPtr log_,
        size_t max_block_size_,
        size_t num_streams_)
        : SourceStepWithFilter(std::move(header), column_names_, query_info_, storage_snapshot_, context_)
        , storage(std::move(storage_))
        , sources_info(std::move(sources_info_))
        , builder(std::move(builder_))
        , fs(std::move(fs_))
        , hive_table_metadata(std::move(hive_table_metadata_))
        , sample_block(std::move(sample_block_))
        , log(log_)
        , max_block_size(max_block_size_)
        , num_streams(num_streams_)
    {
    }

private:
    std::shared_ptr<StorageHive> storage;
    std::shared_ptr<StorageHiveSource::SourcesInfo> sources_info;
    HDFSBuilderWrapper builder;
    HDFSFSPtr fs;
    HiveMetastoreClient::HiveTableMetadataPtr hive_table_metadata;
    Block sample_block;
    LoggerPtr log;

    size_t max_block_size;
    size_t num_streams;

    std::optional<HiveFiles> hive_files;

    void createFiles();
};

void ReadFromHive::createFiles()
{
    if (hive_files)
        return;

    hive_files = storage->collectHiveFiles(num_streams, filter_actions_dag ? &*filter_actions_dag : nullptr, hive_table_metadata, fs, context);
    LOG_INFO(log, "Collect {} hive files to read", hive_files->size());
}

void StorageHive::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context_,
    QueryProcessingStage::Enum /* processed_stage */,
    size_t max_block_size,
    size_t num_streams)
{
    lazyInitialize();

    HDFSBuilderWrapper builder = createHDFSBuilder(hdfs_namenode_url, context_->getGlobalContext()->getConfigRef());
    HDFSFSPtr fs = createHDFSFS(builder.get());
    auto hive_metastore_client = HiveMetastoreClientFactory::instance().getOrCreate(hive_metastore_url);
    auto hive_table_metadata = hive_metastore_client->getTableMetadata(hive_database, hive_table);

    auto sources_info = std::make_shared<StorageHiveSource::SourcesInfo>();
    sources_info->database_name = hive_database;
    sources_info->table_name = hive_table;
    sources_info->hive_metastore_client = hive_metastore_client;
    sources_info->partition_name_types = partition_name_types;

    const auto header_block = storage_snapshot->metadata->getSampleBlock();

    auto settings = context_->getSettingsRef();
    auto case_insensitive_matching = [&]() -> bool
    {
        if (format_name == "Parquet")
            return settings[Setting::input_format_parquet_case_insensitive_column_matching];
        if (format_name == "ORC")
            return settings[Setting::input_format_orc_case_insensitive_column_matching];
        return false;
    };
    Block sample_block;
    NestedColumnExtractHelper nested_columns_extractor(header_block, case_insensitive_matching());
    for (const auto & column : column_names)
    {
        if (header_block.has(column))
        {
            sample_block.insert(header_block.getByName(column));
            continue;
        }

        auto subset_column = nested_columns_extractor.extractColumn(column);
        if (subset_column)
        {
            sample_block.insert(std::move(*subset_column));
            continue;
        }

        if (column == "_path")
            sources_info->need_path_column = true;
        if (column == "_file")
            sources_info->need_file_column = true;
    }

    auto this_ptr = std::static_pointer_cast<StorageHive>(shared_from_this());

    auto reading = std::make_unique<ReadFromHive>(
        column_names,
        query_info,
        storage_snapshot,
        context_,
        StorageHiveSource::getHeader(sample_block, sources_info),
        std::move(this_ptr),
        std::move(sources_info),
        std::move(builder),
        std::move(fs),
        std::move(hive_table_metadata),
        std::move(sample_block),
        log,
        max_block_size,
        num_streams);

    query_plan.addStep(std::move(reading));
}

void ReadFromHive::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    createFiles();

    if (hive_files->empty())
    {
        pipeline.init(Pipe(std::make_shared<NullSource>(getOutputHeader())));
        return;
    }

    sources_info->hive_files = std::move(*hive_files);
    num_streams = std::min(num_streams, sources_info->hive_files.size());

    Pipes pipes;
    for (size_t i = 0; i < num_streams; ++i)
    {
        pipes.emplace_back(std::make_shared<StorageHiveSource>(
            sources_info,
            storage->hdfs_namenode_url,
            storage->format_name,
            storage->compression_method,
            sample_block,
            context,
            max_block_size,
            *storage,
            storage->text_input_field_names));
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));
    if (pipe.empty())
        pipe = Pipe(std::make_shared<NullSource>(getOutputHeader()));

    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));
}

HiveFiles StorageHive::collectHiveFiles(
    size_t max_threads,
    const ActionsDAG * filter_actions_dag,
    const HiveTableMetadataPtr & hive_table_metadata,
    const HDFSFSPtr & fs,
    const ContextPtr & context_,
    PruneLevel prune_level) const
{
    std::vector<Apache::Hadoop::Hive::Partition> partitions = hive_table_metadata->getPartitions();
    /// Hive table have no partition
    if (!partition_name_types.empty() && partitions.empty())
        return {};

    /// Hive files to collect
    HiveFiles hive_files;
    Int64 hit_parttions_num = 0;
    Int64 hive_max_query_partitions = context_->getSettingsRef()[Setting::max_partitions_to_read];
    /// Mutext to protect hive_files, which maybe appended in multiple threads
    std::mutex hive_files_mutex;
    ThreadPool pool{
        CurrentMetrics::StorageHiveThreads,
        CurrentMetrics::StorageHiveThreadsActive,
        CurrentMetrics::StorageHiveThreadsScheduled,
        max_threads};
    if (!partitions.empty())
    {
        for (const auto & partition : partitions)
        {
            pool.scheduleOrThrowOnError(
                [&]()
                {
                    auto hive_files_in_partition
                        = collectHiveFilesFromPartition(partition, filter_actions_dag, hive_table_metadata, fs, context_, prune_level);
                    if (!hive_files_in_partition.empty())
                    {
                        std::lock_guard lock(hive_files_mutex);
                        hit_parttions_num += 1;
                        if (hive_max_query_partitions > 0 && hit_parttions_num > hive_max_query_partitions)
                        {
                            throw Exception(ErrorCodes::TOO_MANY_PARTITIONS,
                                            "Too many partitions "
                                            "to query for table {}.{} . Maximum number of partitions "
                                            "to read is limited to {}",
                                            hive_database, hive_table, hive_max_query_partitions);
                        }
                        hive_files.insert(std::end(hive_files), std::begin(hive_files_in_partition), std::end(hive_files_in_partition));
                    }
                });
        }
    }
    else /// Partition keys is empty but still have files
    {
        auto file_infos = listDirectory(hive_table_metadata->getTable()->sd.location, hive_table_metadata, fs);
        for (const auto & file_info : file_infos)
        {
            pool.scheduleOrThrowOnError(
                [&]()
                {
                    auto hive_file = getHiveFileIfNeeded(file_info, {}, filter_actions_dag, hive_table_metadata, context_, prune_level);
                    if (hive_file)
                    {
                        std::lock_guard lock(hive_files_mutex);
                        hive_files.push_back(hive_file);
                    }
                });
        }
    }
    pool.wait();
    return hive_files;
}

SinkToStoragePtr StorageHive::write(const ASTPtr & /*query*/, const StorageMetadataPtr & /* metadata_snapshot*/, ContextPtr /*context*/, bool /*async_insert*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method write is not implemented for StorageHive");
}

std::optional<UInt64> StorageHive::totalRowsByPartitionPredicate(const ActionsDAG & filter_actions_dag, ContextPtr context_) const
{
    return totalRowsImpl(context_->getSettingsRef(), &filter_actions_dag, context_, PruneLevel::Partition);
}

void StorageHive::checkAlterIsPossible(const AlterCommands & commands, ContextPtr /*local_context*/) const
{
    for (const auto & command : commands)
    {
        if (command.type != AlterCommand::Type::ADD_COLUMN && command.type != AlterCommand::Type::MODIFY_COLUMN
            && command.type != AlterCommand::Type::DROP_COLUMN && command.type != AlterCommand::Type::COMMENT_COLUMN
            && command.type != AlterCommand::Type::COMMENT_TABLE)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Alter of type '{}' is not supported by storage {}", command.type, getName());
    }
}

std::optional<UInt64>
StorageHive::totalRowsImpl(const Settings & settings, const ActionsDAG * filter_actions_dag, ContextPtr context_, PruneLevel prune_level) const
{
    /// Row-based format like Text doesn't support totalRowsByPartitionPredicate
    if (!supportsSubsetOfColumns())
        return {};

    auto hive_metastore_client = HiveMetastoreClientFactory::instance().getOrCreate(hive_metastore_url);
    auto hive_table_metadata = hive_metastore_client->getTableMetadata(hive_database, hive_table);
    HDFSBuilderWrapper builder = createHDFSBuilder(hdfs_namenode_url, getContext()->getGlobalContext()->getConfigRef());
    HDFSFSPtr fs = createHDFSFS(builder.get());
    HiveFiles hive_files = collectHiveFiles(settings[Setting::max_threads], filter_actions_dag, hive_table_metadata, fs, context_, prune_level);

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
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                                "Storage Hive requires 3 arguments: "
                                "hive metastore address, hive database and hive table");

            auto * partition_by = args.storage_def->partition_by;
            if (!partition_by)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Storage Hive requires partition by clause");

            for (auto & engine_arg : engine_args)
                engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, args.getLocalContext());

            const String & hive_metastore_url = checkAndGetLiteralArgument<String>(engine_args[0], "hive_metastore_url");
            const String & hive_database = checkAndGetLiteralArgument<String>(engine_args[1], "hive_database");
            const String & hive_table = checkAndGetLiteralArgument<String>(engine_args[2], "hive_table");
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
            .has_builtin_setting_fn = HiveSettings::hasBuiltin,
        });
}

}
#endif
