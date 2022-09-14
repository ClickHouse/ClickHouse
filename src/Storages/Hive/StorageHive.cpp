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


static std::string getBaseName(const String & path)
{
    size_t basename_start = path.rfind('/');
    return path.substr(basename_start + 1);
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
            to_read_block.erase(name_type.name);
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
    ContextPtr context_)
    : IStorage(table_id_)
    , WithContext(context_)
    , hive_metastore_url(hive_metastore_url_)
    , hive_database(hive_database_)
    , hive_table(hive_table_)
    , partition_by_ast(partition_by_ast_)
    , storage_settings(std::move(storage_settings_))
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
            std::make_shared<ActionsDAG>(partition_name_types), ExpressionActionsSettings::fromContext(getContext()));
    }

    NamesAndTypesList all_name_types = metadata_snapshot->getColumns().getAllPhysical();
    for (const auto & column : all_name_types)
    {
        if (!partition_name_types.contains(column.name))
            hivefile_name_types.push_back(column);
    }
    hivefile_minmax_idx_expr = std::make_shared<ExpressionActions>(
        std::make_shared<ActionsDAG>(hivefile_name_types), ExpressionActionsSettings::fromContext(getContext()));
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
    else
    {
        /// Primary key consists of one column.
        auto res = std::make_shared<ASTExpressionList>();
        res->children.push_back(node);
        return res;
    }
}


HiveFilePtr createHiveFile(
    const String & format_name,
    const FieldVector & fields,
    const String & namenode_url,
    const String & path,
    UInt64 ts,
    size_t size,
    const NamesAndTypesList & index_names_and_types,
    const std::shared_ptr<HiveSettings> & hive_settings,
    ContextPtr context)
{
    HiveFilePtr hive_file;
    if (format_name == "HiveText")
    {
        hive_file = std::make_shared<HiveTextFile>(fields, namenode_url, path, ts, size, index_names_and_types, hive_settings, context);
    }
    else if (format_name == "ORC")
    {
        hive_file = std::make_shared<HiveOrcFile>(fields, namenode_url, path, ts, size, index_names_and_types, hive_settings, context);
    }
    else if (format_name == "Parquet")
    {
        hive_file = std::make_shared<HiveParquetFile>(fields, namenode_url, path, ts, size, index_names_and_types, hive_settings, context);
    }
    else
    {
        throw Exception("IHiveFile not implemented for format " + format_name, ErrorCodes::NOT_IMPLEMENTED);
    }
    return hive_file;
}

std::vector<HiveFilePtr> StorageHive::collectHiveFilesFromPartition(
    const Apache::Hadoop::Hive::Partition & partition,
    SelectQueryInfo & query_info,
    HiveTableMetadataPtr hive_table_metadata,
    const HDFSFSPtr & fs,
    ContextPtr context_)
{
      LOG_DEBUG(log, "Collect hive files from partition {}", boost::join(partition.values, ","));

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
        throw Exception(
            fmt::format("Partition value size not match, expect {}, but got {}", partition_names.size(), partition.values.size()),
            ErrorCodes::INVALID_PARTITION_VALUE);

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
    auto format = FormatFactory::instance().getInputFormat(
        "CSV", buffer, partition_key_expr->getSampleBlock(), getContext(), getContext()->getSettingsRef().max_block_size);
    auto pipeline = QueryPipeline(std::move(format));
    auto reader = std::make_unique<PullingPipelineExecutor>(pipeline);
    Block block;
    if (!reader->pull(block) || !block.rows())
        throw Exception("Could not parse partition value: " + wb.str(), ErrorCodes::INVALID_PARTITION_VALUE);

    std::vector<Range> ranges;
    ranges.reserve(partition_names.size());
    FieldVector fields(partition_names.size());
    for (size_t i = 0; i < partition_names.size(); ++i)
    {
        block.getByPosition(i).column->get(0, fields[i]);
        ranges.emplace_back(fields[i]);
    }

    const KeyCondition partition_key_condition(query_info, getContext(), partition_names, partition_minmax_idx_expr);
    if (!partition_key_condition.checkInHyperrectangle(ranges, partition_types).can_be_true)
        return {};

    auto file_infos = listDirectory(partition.sd.location, hive_table_metadata, fs);
    std::vector<HiveFilePtr> hive_files;
    hive_files.reserve(file_infos.size());
    for (const auto & file_info : file_infos)
    {
        auto hive_file = createHiveFileIfNeeded(file_info, fields, query_info, context_);
        if (hive_file)
            hive_files.push_back(hive_file);
    }
    return hive_files;
}

std::vector<StorageHive::FileInfo>
StorageHive::listDirectory(const String & path, HiveTableMetadataPtr hive_table_metadata, const HDFSFSPtr & fs)
{
    return hive_table_metadata->getFilesByLocation(fs, path);
}

HiveFilePtr StorageHive::createHiveFileIfNeeded(
    const FileInfo & file_info, const FieldVector & fields, SelectQueryInfo & query_info, ContextPtr context_)
{
    LOG_TRACE(log, "Append hive file {}", file_info.path);
    String filename = getBaseName(file_info.path);
    /// Skip temporary files starts with '.'
    if (filename.find('.') == 0)
        return {};

    auto hive_file = createHiveFile(
        format_name,
        fields,
        hdfs_namenode_url,
        file_info.path,
        file_info.last_modify_time,
        file_info.size,
        hivefile_name_types,
        storage_settings,
        context_);

    /// Load file level minmax index and apply
    const KeyCondition hivefile_key_condition(query_info, getContext(), hivefile_name_types.getNames(), hivefile_minmax_idx_expr);
    if (hive_file->hasMinMaxIndex())
    {
        hive_file->loadMinMaxIndex();
        if (!hivefile_key_condition.checkInHyperrectangle(hive_file->getMinMaxIndex()->hyperrectangle, hivefile_name_types.getTypes())
                 .can_be_true)
        {
            LOG_TRACE(log, "Skip hive file {} by index {}", hive_file->getPath(), hive_file->describeMinMaxIndex(hive_file->getMinMaxIndex()));
            return {};
        }
    }

    /// Load sub-file level minmax index and apply
    if (hive_file->hasSubMinMaxIndex())
    {
        std::set<int> skip_splits;
        hive_file->loadSubMinMaxIndex();
        const auto & sub_minmax_idxes = hive_file->getSubMinMaxIndexes();
        for (size_t i = 0; i < sub_minmax_idxes.size(); ++i)
        {
            if (!hivefile_key_condition.checkInHyperrectangle(sub_minmax_idxes[i]->hyperrectangle, hivefile_name_types.getTypes())
                     .can_be_true)
            {
                LOG_TRACE(log, "Skip split {} of hive file {}", i, hive_file->getPath());
                skip_splits.insert(i);
            }
        }
        hive_file->setSkipSplits(skip_splits);
    }
    return hive_file;
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

    HDFSBuilderWrapper builder = createHDFSBuilder(hdfs_namenode_url, context_->getGlobalContext()->getConfigRef());
    HDFSFSPtr fs = createHDFSFS(builder.get());
    auto hive_metastore_client = HiveMetastoreClientFactory::instance().getOrCreate(hive_metastore_url, getContext());
    auto hive_table_metadata = hive_metastore_client->getTableMetadata(hive_database, hive_table);

    std::vector<Apache::Hadoop::Hive::Partition> partitions = hive_table_metadata->getPartitions();
    /// Hive files to read
    HiveFiles hive_files;
    /// Mutext to protect hive_files, which maybe appended in multiple threads
    std::mutex hive_files_mutex;

    ThreadPool pool{num_streams};
    if (!partitions.empty())
    {
        for (const auto & partition : partitions)
        {
            pool.scheduleOrThrowOnError([&]()
            {
                auto hive_files_in_partition = collectHiveFilesFromPartition(partition, query_info, hive_table_metadata, fs, context_);
                if (!hive_files_in_partition.empty())
                {
                    std::lock_guard<std::mutex> lock(hive_files_mutex);
                    hive_files.insert(std::end(hive_files), std::begin(hive_files_in_partition), std::end(hive_files_in_partition));
                }
            });
        }
        pool.wait();
    }
    else if (partition_name_types.empty()) /// Partition keys is empty
    {
        auto file_infos = listDirectory(hive_table_metadata->getTable()->sd.location, hive_table_metadata, fs);
        for (const auto & file_info : file_infos)
        {
            pool.scheduleOrThrowOnError([&]
            {
                auto hive_file = createHiveFileIfNeeded(file_info, {}, query_info, context_);
                if (hive_file)
                {
                    std::lock_guard<std::mutex> lock(hive_files_mutex);
                    hive_files.push_back(hive_file);
                }
            });
        }
        pool.wait();
    }
    else /// Partition keys is not empty but partitions is empty
        return {};

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
