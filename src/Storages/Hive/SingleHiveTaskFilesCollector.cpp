#include <Storages/Hive/SingleHiveTaskFilesCollector.h>
#if USE_HIVE
#    include <mutex>
#    include <Core/Field.h>
#    include <Formats/FormatFactory.h>
#    include <IO/ReadBufferFromString.h>
#    include <IO/WriteBufferFromString.h>
#    include <Interpreters/ExpressionAnalyzer.h>
#    include <Parsers/ASTFunction.h>
#    include <Processors/Executors/PullingPipelineExecutor.h>
#    include <Processors/Formats/IInputFormat.h>
#    include <QueryPipeline/Pipe.h>
#    include <base/logger_useful.h>
#    include <Common/ErrorCodes.h>
#    include <Common/ThreadPool.h>
namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_PARTITION_VALUE;
}

void SingleHiveTaskFilesCollector::setupArgs(const Arguments & args_)
{
    args = args_;

    ASTPtr partition_key_expr_list = extractKeyExpressionList(args.partition_by_ast);
    NamesAndTypesList all_name_and_types = args.columns.getAllPhysical();
    if (!partition_key_expr_list->children.empty())
    {
        auto syntax_result = TreeRewriter(args.context).analyze(partition_key_expr_list, all_name_and_types);
        partition_key_expr = ExpressionAnalyzer(partition_key_expr_list, syntax_result, args.context).getActions(false);
        partition_name_and_types = partition_key_expr->getRequiredColumnsWithTypes();
        partition_minmax_idx_expr = std::make_shared<ExpressionActions>(
            std::make_shared<ActionsDAG>(partition_name_and_types), ExpressionActionsSettings::fromContext(args.context));
    }

    for (const auto & column : all_name_and_types)
    {
        if (partition_name_and_types.contains(column.name))
            hive_file_name_and_types.push_back(column);
    }

    hive_file_minmax_idx_expr = std::make_shared<ExpressionActions>(
        std::make_shared<ActionsDAG>(hive_file_name_and_types), ExpressionActionsSettings::fromContext(args.context));
}

ASTPtr SingleHiveTaskFilesCollector::extractKeyExpressionList(const ASTPtr & node)
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

HiveFiles SingleHiveTaskFilesCollector::collectHiveFiles()
{
    auto hive_metastore_client = HiveMetastoreClientFactory::instance().getOrCreate(args.hive_metastore_url, args.context);
    auto hive_table_metadata = hive_metastore_client->getTableMetadata(args.hive_database, args.hive_table);
    auto partitions = hive_table_metadata->getPartitions();
    hdfs_namenode_url = getNameNodeUrl(hive_table_metadata->getTable()->sd.location);
    auto hdfs_builder = createHDFSBuilder(hdfs_namenode_url, args.context->getGlobalContext()->getConfigRef());
    auto hdfs_fs = createHDFSFS(hdfs_builder.get());

    format_name = IHiveFile::toHiveFileFormat(hive_table_metadata->getTable()->sd.inputFormat);

    HiveFiles hive_files;
    std::mutex hive_files_mutex;
    ThreadPool thread_pool{args.num_streams};
    if (!partitions.empty())
    {
        for (const auto & partition : partitions)
        {
            thread_pool.scheduleOrThrowOnError([&]()
            {
                auto hive_files_in_partition
                    = collectHiveFilesFromPartition(partition, *args.query_info, hive_table_metadata, hdfs_fs, args.context);
                if (!hive_files_in_partition.empty())
                {
                    std::lock_guard<std::mutex> lock(hive_files_mutex);
                    hive_files.insert(std::end(hive_files), std::begin(hive_files_in_partition), std::end(hive_files_in_partition));
                }
            });
        }
        thread_pool.wait();
    }
    else if (partition_name_and_types.empty())
    {
        auto file_infos = hive_table_metadata->getFilesByLocation(hdfs_fs, hive_table_metadata->getTable()->sd.location);
        for (const auto & file_info : file_infos)
        {
            thread_pool.scheduleOrThrow([&] {
                auto hive_file = createHiveFileIfNeeded(file_info, {}, *args.query_info, args.context);
                if (hive_file)
                {
                    std::lock_guard<std::mutex> lock(hive_files_mutex);
                    hive_files.push_back(hive_file);
                }
            });
        }
        thread_pool.wait();
    }
    else
    {
        throw Exception(
            ErrorCodes::INVALID_PARTITION_VALUE,
            "Invalid hive partition settings. partitions size:{}, partition_name_and_types size:{}",
            partitions.size(),
            partition_name_and_types.size());
    }
    return hive_files;
}
HiveFiles SingleHiveTaskFilesCollector::collectHiveFilesFromPartition(
    const Apache::Hadoop::Hive::Partition & partition_,
    SelectQueryInfo & query_info_,
    HiveMetastoreClient::HiveTableMetadataPtr hive_table_metadata_,
    const HDFSFSPtr & fs_,
    ContextPtr context_)
{
    bool has_default_partition = false;
    for (const auto & value : partition_.values)
    {
        if (value == "__HIVE_DEFAULT_PARTTION_")
        {
            has_default_partition = true;
            break;
        }
    }
    if (has_default_partition)
        return {};

    if (partition_.values.size() != partition_name_and_types.size())
        throw Exception(
            fmt::format("Partition value size not match, expect {}, but got {}", partition_name_and_types.size(), partition_.values.size()),
            ErrorCodes::INVALID_PARTITION_VALUE);

    WriteBufferFromOwnString write_buf;
    for (size_t i = 0; i < partition_.values.size(); ++i)
    {
        if (i)
            writeString(",", write_buf);
        writeString(partition_.values[i], write_buf);
    }
    writeString("\n", write_buf);

    ReadBufferFromString read_buf(write_buf.str());
    auto format = FormatFactory::instance().getInputFormat(
        "CSV", read_buf, partition_key_expr->getSampleBlock(), context_, context_->getSettingsRef().max_block_size);

    auto pipeline = QueryPipeline(std::move(format));
    auto reader = std::make_unique<PullingPipelineExecutor>(pipeline);
    Block block;
    if (!reader->pull(block) || !block.rows())
        throw Exception("Could not parse partition value: " + write_buf.str(), ErrorCodes::INVALID_PARTITION_VALUE);
    std::vector<Range> ranges;
    ranges.reserve(partition_name_and_types.size());
    FieldVector fields(partition_name_and_types.size());
    for (size_t i = 0; i < partition_name_and_types.size(); ++i)
    {
        block.getByPosition(i).column->get(0, fields[i]);
        ranges.emplace_back(fields[i]);
    }

    const KeyCondition partition_key_condition(query_info_, args.context, partition_name_and_types.getNames(), partition_minmax_idx_expr);
    if (!partition_key_condition.checkInHyperrectangle(ranges, partition_name_and_types.getTypes()).can_be_true)
    {
        LOG_DEBUG(logger, "Partition condition check failed. partition:{}", write_buf.str());
        return {};
    }
    auto file_infos = hive_table_metadata_->getFilesByLocation(fs_, partition_.sd.location);
    std::vector<HiveFilePtr> hive_files;
    hive_files.reserve(file_infos.size());
    for (const auto & file_info : file_infos)
    {
        auto hive_file = createHiveFileIfNeeded(file_info, fields, query_info_, args.context);
        if (hive_file)
            hive_files.emplace_back(hive_file);
    }
    return hive_files;
}
static std::string getBaseName(const String & path)
{
    size_t basename_start = path.rfind('/');
    return path.substr(basename_start + 1);
}


HiveFilePtr SingleHiveTaskFilesCollector::createHiveFileIfNeeded(
    const HiveMetastoreClient::FileInfo & file_info_, const FieldVector & fields_, SelectQueryInfo & query_info_, ContextPtr context_)
{
    String filename = getBaseName(file_info_.path);
    if (filename.find('.') == 0)
        return {};

    auto hive_file = createHiveFile(
        format_name,
        fields_,
        hdfs_namenode_url,
        file_info_.path,
        file_info_.last_modify_time,
        file_info_.size,
        hive_file_name_and_types,
        args.storage_settings,
        args.context);

    const KeyCondition hive_file_key_condition(query_info_, context_, hive_file_name_and_types.getNames(), hive_file_minmax_idx_expr);
    if (hive_file->hasMinMaxIndex())
    {
        hive_file->loadMinMaxIndex();
        if (!hive_file_key_condition.checkInHyperrectangle(hive_file->getMinMaxIndex()->hyperrectangle, hive_file_name_and_types.getTypes())
                 .can_be_true)
        {
            LOG_TRACE(
                logger, "Skip hive file {} by index {}", hive_file->getPath(), hive_file->describeMinMaxIndex(hive_file->getMinMaxIndex()));
            return {};
        }
    }

    if (hive_file->hasSubMinMaxIndex())
    {
        std::set<int> skip_splits;
        hive_file->loadSubMinMaxIndex();
        const auto & sub_minmax_idxes = hive_file->getSubMinMaxIndexes();
        for (size_t i = 0; i < sub_minmax_idxes.size(); ++i)
        {
            if (!hive_file_key_condition.checkInHyperrectangle(sub_minmax_idxes[i]->hyperrectangle, hive_file_name_and_types.getTypes())
                     .can_be_true)
            {
                LOG_TRACE(logger, "Skip split {} of hive file {}", i, hive_file->getPath());
                skip_splits.insert(i);
            }
        }
        hive_file->setSkipSplits(skip_splits);
    }
    return hive_file;
}
}
#endif
