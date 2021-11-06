#include <Common/config.h>
#include "Interpreters/TreeRewriter.h"

#if USE_HDFS

#include <boost/algorithm/string/join.hpp>
#include <Columns/IColumn.h>
#include <base/logger_useful.h>
#include <Common/parseAddress.h>
#include <Common/parseGlobs.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeString.h>
#include <fmt/core.h>
#include <Formats/FormatFactory.h>
#include <hdfs/hdfs.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/RemoteReadBufferCache.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTCreateQuery.h>
#include <Poco/URI.h>
#include <QueryPipeline/Pipe.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/Impl/ArrowBlockInputFormat.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Storages/HDFS/HDFSCommon.h>
#include <Storages/HDFS/ReadBufferFromHDFS.h>
#include <Storages/Hive/HiveFile.h>
#include <Storages/Hive/HiveSettings.h>
#include <Storages/Hive/StorageHive.h>
#include <Storages/Hive/HiveCommon.h>
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

using HiveFilePtr = std::shared_ptr<IHiveFile>;
using HiveFiles = std::vector<HiveFilePtr>;

static std::string getBaseName(const String & path)
{
    size_t basename_start = path.rfind('/');
    return std::string{&path[basename_start + 1], path.length() - basename_start - 1};
}

class HiveSource : public SourceWithProgress, WithContext
{
public:
    struct SourcesInfo
    {
        HMSClientPtr hms_client;
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
            header.insert({DataTypeString().createColumn(), std::make_shared<DataTypeString>(), "_path"});
        if (source_info->need_file_column)
            header.insert({DataTypeString().createColumn(), std::make_shared<DataTypeString>(), "_file"});

        return header;
    }

    HiveSource(
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
        , to_read_block(sample_block)
        , text_input_field_names(text_input_field_names_)
        , format_settings(getFormatSettings(getContext()))
    {
        to_read_block = sample_block;
        for (const auto & name_type : source_info->partition_name_types)
        {
            to_read_block.erase(name_type.name);
        }
        format_settings.csv.delimiter = '\x01';
        format_settings.csv.input_field_names = text_input_field_names;
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
                        source_info->hms_client->clearTableMeta(source_info->database, source_info->table_name);
                        throw;
                    }
                }

                std::unique_ptr<ReadBuffer> remote_read_buf = RemoteReadBuffer::create(
                    "Hive",
                    getNameNodeCluster(hdfs_namenode_url),
                    uri_with_path,
                    curr_file->getLastModTs(),
                    curr_file->getSize(),
                    std::move(raw_read_buf));
                // std::unique_ptr<ReadBuffer> remote_read_buf = std::move(raw_read_buf);
                if (curr_file->getFormat() == StorageHive::FileFormat::TEXT)
                    read_buf = wrapReadBufferWithCompressionMethod(std::move(remote_read_buf), compression);
                else
                    read_buf = std::move(remote_read_buf);

                auto input_format = FormatFactory::instance().getInputFormat(
                    format, *read_buf, to_read_block, getContext(), max_block_size, format_settings);
                pipeline = QueryPipeline(std::move(input_format));
                reader = std::make_unique<PullingPipelineExecutor>(pipeline);
            }

            Block res;
            if (reader->pull(res))
            {
                Columns columns = res.getColumns();
                UInt64 num_rows = res.rows();
                auto types = source_info->partition_name_types.getTypes();
                for (size_t i = 0; i < types.size(); ++i)
                {
                    auto column = types[i]->createColumnConst(num_rows, source_info->hive_files[current_idx]->getPartitionValues()[i]);
                    auto previous_idx = sample_block.getPositionByName(source_info->partition_name_types.getNames()[i]);
                    columns.insert(columns.begin() + previous_idx, column->convertToFullColumnIfConst());
                }

                /// Enrich with virtual columns.
                if (source_info->need_path_column)
                {
                    auto column = DataTypeString().createColumnConst(num_rows, current_path);
                    columns.push_back(column->convertToFullColumnIfConst());
                }

                if (source_info->need_file_column)
                {
                    size_t last_slash_pos = current_path.find_last_of('/');
                    auto file_name = current_path.substr(last_slash_pos + 1);

                    auto column = DataTypeString().createColumnConst(num_rows, std::move(file_name));
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
    QueryPipeline pipeline;
    std::unique_ptr<PullingPipelineExecutor> reader;
    SourcesInfoPtr source_info;
    String hdfs_namenode_url;
    String format;
    String compression_method;
    UInt64 max_block_size;
    Block sample_block;
    Block to_read_block;
    const Names & text_input_field_names;
    FormatSettings format_settings;

    String current_path;
    size_t current_idx = 0;

    Poco::Logger * log = &Poco::Logger::get("StorageHive");
};


StorageHive::StorageHive(
    const String & hms_url_,
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
    , hms_url(hms_url_)
    , hive_database(hive_database_)
    , hive_table(hive_table_)
    , partition_by_ast(partition_by_ast_)
    , storage_settings(std::move(storage_settings_))
{
    getContext()->getRemoteHostFilter().checkURL(Poco::URI(hms_url));

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment_);
    setInMemoryMetadata(storage_metadata);

    auto hms_client = getContext()->getHMSClient(hms_url);
    auto table_meta = hms_client->getTableMeta(hive_database, hive_table);

    hdfs_namenode_url = getNameNodeUrl(table_meta->getTable()->sd.location);
    table_schema = table_meta->getTable()->sd.cols;

    FileFormat hdfs_file_format = toFileFormat(table_meta->getTable()->sd.inputFormat);
    switch (hdfs_file_format)
    {
        case FileFormat::TEXT:
            format_name = "CSVWithNames";
            break;
        case FileFormat::LZO_TEXT:
            format_name = "CSVWithNames";
            break;
        case FileFormat::RC_FILE:
            // TODO to be implemented
            throw Exception("Unsopported hive format rc_file", ErrorCodes::NOT_IMPLEMENTED);
        case FileFormat::SEQUENCE_FILE:
            // TODO to be implemented
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

    // Need to specify text_input_fields_names from table_schema for TextInputFormated Hive table
    if (format_name == "CSVWithNames")
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
    if (format_name == "CSVWithNames")
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

Pipe StorageHive::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context_,
    QueryProcessingStage::Enum /* processed_stage */,
    size_t max_block_size,
    unsigned num_streams)
{
    HDFSBuilderWrapper builder = createHDFSBuilder(hdfs_namenode_url, context_->getGlobalContext()->getConfigRef());
    HDFSFSPtr fs = createHDFSFS(builder.get());
    auto hms_client = context_->getHMSClient(hms_url);
    auto table_meta_cntrl = hms_client->getTableMeta(hive_database, hive_table);

    // List files under partition directory in HDFS
    auto list_paths = [table_meta_cntrl, &fs](const String & path) { return table_meta_cntrl->getLocationFiles(fs, path); };

    std::vector<Apache::Hadoop::Hive::Partition> partitions = table_meta_cntrl->getPartitions();
    HiveFiles hive_files; // hive files to read
    std::mutex hive_files_mutex; // Mutext to protect hive_files, which maybe appended in multiple threads

    auto append_hive_files = [&](const HMSClient::FileInfo & hfile, const FieldVector & fields)
    {
        String filename = getBaseName(hfile.path);

        // Skip temporary files starts with '.'
        if (filename.find('.') == 0)
            return;

        auto file = createHiveFile(
            format_name,
            fields,
            hdfs_namenode_url,
            hfile.path,
            hfile.last_mod_ts,
            hfile.size,
            hivefile_name_types,
            storage_settings,
            context_);

        // Load file level minmax index and apply
        const KeyCondition hivefile_key_condition(query_info, getContext(), hivefile_name_types.getNames(), hivefile_minmax_idx_expr);
        if (file->hasMinMaxIndex())
        {
            file->loadMinMaxIndex();
            if (!hivefile_key_condition.checkInHyperrectangle(file->getMinMaxIndex()->hyperrectangle, hivefile_name_types.getTypes())
                     .can_be_true)
            {
                LOG_DEBUG(log, "skip file:{} index:{}", file->getPath(), file->describeMinMaxIndex(file->getMinMaxIndex()));
                return;
            }
        }

        // Load sub-file level minmax index and appy
        std::set<int> skip_splits;
        if (file->hasSubMinMaxIndex())
        {
            file->loadSubMinMaxIndex();
            const auto & sub_minmax_idxes = file->getSubMinMaxIndexes();
            for (size_t i = 0; i < sub_minmax_idxes.size(); ++i)
            {
                if (!hivefile_key_condition.checkInHyperrectangle(sub_minmax_idxes[i]->hyperrectangle, hivefile_name_types.getTypes())
                         .can_be_true)
                {
                    LOG_DEBUG(log, "skip split:{} in file {}", i, file->getPath());
                    skip_splits.insert(i);
                }
            }
            file->setSkipSplits(skip_splits);
        }

        {
            std::lock_guard lock{hive_files_mutex};
            hive_files.push_back(file);
        }
    };

    ThreadPool pool{num_streams};
    if (!partitions.empty())
    {
        const auto partition_names = partition_name_types.getNames();
        const auto partition_types = partition_name_types.getTypes();

        for (const auto & p : partitions)
        {
            auto f = [&]()
            {
                // Skip partition "__HIVE_DEFAULT_PARTITION__"
                bool has_default_partition = false;
                for (const auto & value : p.values)
                {
                    if (value == "__HIVE_DEFAULT_PARTITION__")
                    {
                        has_default_partition = true;
                        break;
                    }
                }
                if (has_default_partition)
                {
                    LOG_DEBUG(log, "skip partition:__HIVE_DEFAULT_PARTITION__");
                    return;
                }

                std::vector<Range> ranges;
                WriteBufferFromOwnString wb;
                if (p.values.size() != partition_names.size())
                    throw Exception(
                        fmt::format("Partition value size not match, expect {}, but got {}", partition_names.size(), p.values.size()),
                        ErrorCodes::INVALID_PARTITION_VALUE);

                for (size_t i = 0; i < p.values.size(); ++i)
                {
                    if (i != 0)
                        writeString(",", wb);
                    writeString(p.values[i], wb);
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

                FieldVector fields(partition_names.size());
                for (size_t i = 0; i < partition_names.size(); ++i)
                {
                    block.getByPosition(i).column->get(0, fields[i]);
                    ranges.emplace_back(fields[i]);
                }

                const KeyCondition partition_key_condition(query_info, getContext(), partition_names, partition_minmax_idx_expr);
                if (!partition_key_condition.checkInHyperrectangle(ranges, partition_types).can_be_true)
                {
                    LOG_DEBUG(log, "skip partition:{}", boost::algorithm::join(p.values, "|"));
                    return;
                }

                auto paths = list_paths(p.sd.location);
                for (const auto & path : paths)
                {
                    append_hive_files(path, fields);
                }
            };
            pool.scheduleOrThrowOnError(f);
        }
        pool.wait();
    }
    else if (partition_name_types.empty()) // Partition keys is empty
    {
        auto paths = list_paths(table_meta_cntrl->getTable()->sd.location);
        for (const auto & path : paths)
        {
            pool.scheduleOrThrowOnError([&] { append_hive_files(path, {}); });
        }
        pool.wait();
    }
    else // Partition keys is not empty but partitions is empty
    {
        return {};
    }

    auto sources_info = std::make_shared<HiveSource::SourcesInfo>();
    sources_info->hive_files = std::move(hive_files);
    sources_info->database = hive_database;
    sources_info->table_name = hive_table;
    sources_info->hms_client = hms_client;
    sources_info->partition_name_types = partition_name_types;
    for (const auto & column : column_names)
    {
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
        pipes.emplace_back(std::make_shared<HiveSource>(
            sources_info,
            hdfs_namenode_url,
            format_name,
            compression_method,
            metadata_snapshot->getSampleBlock(),
            getContext(),
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
    return NamesAndTypesList{{"_path", std::make_shared<DataTypeString>()}, {"_file", std::make_shared<DataTypeString>()}};
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

            const String & hms_url = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();
            const String & hive_database = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
            const String & hive_table = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
            return StorageHive::create(
                hms_url,
                hive_database,
                hive_table,
                args.table_id,
                args.columns,
                args.constraints,
                args.comment,
                partition_by ? partition_by->ptr() : nullptr,
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
