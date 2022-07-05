#include <cstdint>
#include <Storages/Hive/HiveFilesCollector.h>
#if USE_HIVE
#include <Formats/FormatFactory.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTFunction.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Common/ErrorCodes.h>
#include <Processors/Formats/IInputFormat.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/logger_useful.h>
#include <IO/ReadBufferFromString.h>
namespace DB
{
namespace ErrorCodes
{
    extern const int INVALID_PARTITION_VALUE;
    extern const int TOO_MANY_PARTITIONS;
}
ASTPtr HiveFilesCollector::extractKeyExpressionList(const ASTPtr & node)
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
void HiveFilesCollector::prepare()
{
    ASTPtr partition_key_expr_list = extractKeyExpressionList(partition_by_ast);
    NamesAndTypesList all_name_and_types = columns.getAllPhysical();
    if (!partition_key_expr_list->children.empty())
    {
        auto syntax_result = TreeRewriter(context).analyze(partition_key_expr_list, all_name_and_types);
        partition_key_expr = ExpressionAnalyzer(partition_key_expr_list, syntax_result, context).getActions(false);
        partition_name_and_types = partition_key_expr->getRequiredColumnsWithTypes();
        partition_minmax_idx_expr = std::make_shared<ExpressionActions>(
            std::make_shared<ActionsDAG>(partition_name_and_types), ExpressionActionsSettings::fromContext(context));
    }

    for (const auto & column : all_name_and_types)
    {
        if (partition_name_and_types.contains(column.name))
            hive_file_name_and_types.push_back(column);
    }

    hive_file_minmax_idx_expr = std::make_shared<ExpressionActions>(
        std::make_shared<ActionsDAG>(hive_file_name_and_types), ExpressionActionsSettings::fromContext(context));
}

HiveFiles HiveFilesCollector::collect(HivePruneLevel prune_level)
{
    auto hive_metastore_client = HiveMetastoreClientFactory::instance().getOrCreate(hive_metastore_url);
    auto hive_table_metadata = hive_metastore_client->getTableMetadata(hive_database, hive_table);
    auto partitions = hive_table_metadata->getPartitions();
    hdfs_namenode_url = getNameNodeUrl(hive_table_metadata->getTable()->sd.location);
    auto hdfs_builder = createHDFSBuilder(hdfs_namenode_url, context->getGlobalContext()->getConfigRef());
    auto hdfs_fs = createHDFSFS(hdfs_builder.get());
    format_name = IHiveFile::toCHFormat(hive_table_metadata->getTable()->sd.inputFormat);

    if (!partition_name_and_types.empty() && partitions.empty())
        return {};

    HiveFiles hive_files;
    Int64 hit_partitions_num = 0;
    Int64 max_partition_to_read = context->getSettings().max_partitions_to_read;
    std::mutex hive_files_mutex;
    ThreadPool thread_pool{num_streams};
    if (!partitions.empty())
    {
        for (const auto & partition : partitions)
        {
            thread_pool.scheduleOrThrowOnError([&]()
            {
                auto hive_files_in_partition = collectHiveFilesFromPartition(partition, hive_table_metadata, hdfs_fs, prune_level);
                if (!hive_files_in_partition.empty())
                {
                    hit_partitions_num += 1;
                    if (max_partition_to_read > 0 && hit_partitions_num > max_partition_to_read)
                    {
                        throw Exception(ErrorCodes::TOO_MANY_PARTITIONS, "Too many partitions to query for table {}.{} . Maximum number of partitions to read is limited to {}", hive_database, hive_table, max_partition_to_read);
                    }
                    std::lock_guard<std::mutex> lock(hive_files_mutex);
                    hive_files.insert(std::end(hive_files), std::begin(hive_files_in_partition), std::end(hive_files_in_partition));
                }
            });
        }
    }
    else
    {
        auto file_infos = hive_table_metadata->getFilesByLocation(hdfs_fs, hive_table_metadata->getTable()->sd.location);
        for (const auto & file_info : file_infos)
        {
            thread_pool.scheduleOrThrow([&]()
            {
                auto hive_file = getHiveFileIfNeeded(file_info, {}, hive_table_metadata, prune_level);
                if (hive_file)
                {
                    std::lock_guard<std::mutex> lock(hive_files_mutex);
                    hive_files.emplace_back(hive_file);
                }
            });
        }
    }
    thread_pool.wait();
    return hive_files;
}
static std::string getBaseName(const String & path)
{
    size_t basename_start = path.rfind('/');
    return path.substr(basename_start + 1);
}

HiveFiles HiveFilesCollector::collectHiveFilesFromPartition(
    const Apache::Hadoop::Hive::Partition & partition_,
    HiveMetastoreClient::HiveTableMetadataPtr hive_table_metadata_,
    const HDFSFSPtr & fs_,
    HivePruneLevel prune_level)
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
    {
        return {};
    }

    if (partition_.values.size() != partition_name_and_types.size())
        throw Exception(
            fmt::format("Partition value size not match, expect {}, but got {}", partition_name_and_types.size(), partition_.values.size()),
            ErrorCodes::INVALID_PARTITION_VALUE);

    Strings partition_values;
    partition_values.reserve(partition_.values.size());
    WriteBufferFromOwnString write_buf;
    for (size_t i = 0; i < partition_.values.size(); ++i)
    {
        partition_values.emplace_back(partition_.values[i]);
        if (i)
            writeString(",", write_buf);
        writeString(partition_.values[i], write_buf);
    }
    writeString("\n", write_buf);

    ReadBufferFromString read_buf(write_buf.str());
    auto format = FormatFactory::instance().getInputFormat(
        "CSV", read_buf, partition_key_expr->getSampleBlock(), context, context->getSettingsRef().max_block_size);

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

    const KeyCondition partition_key_condition(*query_info, context, partition_name_and_types.getNames(), partition_minmax_idx_expr);
    if (!partition_key_condition.checkInHyperrectangle(ranges, partition_name_and_types.getTypes()).can_be_true)
    {
        return {};
    }

    auto file_infos = hive_table_metadata_->getFilesByLocation(fs_, partition_.sd.location);

    HiveFiles hive_files;
    hive_files.reserve(file_infos.size());
    for (const auto & file_info : file_infos)
    {
        auto hive_file = getHiveFileIfNeeded(file_info, fields, hive_table_metadata_, prune_level);
        if (hive_file)
            hive_files.emplace_back(hive_file);
    }
    return hive_files;

}

HiveFilePtr HiveFilesCollector::getHiveFileIfNeeded(
    const HiveMetastoreClient::FileInfo & file_info,
    const FieldVector & fields,
    const HiveTableMetadataPtr & hive_table_metadata,
    HivePruneLevel prune_level) const
{
    String filename = getBaseName(file_info.path);
    /// Skip temporary files starts with '.'
    if (startsWith(filename, "."))
        return {};

    auto cache = hive_table_metadata->getHiveFilesCache();
    auto hive_file = cache->get(file_info.path);
    if (!hive_file || hive_file->getLastModifiedTimestamp() < file_info.last_modify_time)
    {
        LOG_TRACE(logger, "Create hive file {}, prune_level {}", file_info.path, pruneLevelToString(prune_level));
        hive_file = HiveFileFactory::instance().createFile(
            format_name,
            fields,
            hdfs_namenode_url,
            file_info.path,
            file_info.last_modify_time,
            file_info.size,
            hive_file_name_and_types,
            storage_settings,
            context->getGlobalContext());
        cache->set(file_info.path, hive_file);
    }
    else
    {
        LOG_TRACE(logger, "Get hive file {} from cache, prune_level {}", file_info.path, pruneLevelToString(prune_level));
    }

    if (prune_level >= PruneLevel::File)
    {
        const KeyCondition hivefile_key_condition(*query_info, context, hive_file_name_and_types.getNames(), hive_file_minmax_idx_expr);
        if (hive_file->useFileMinMaxIndex())
        {
            /// Load file level minmax index and apply
            hive_file->loadFileMinMaxIndex();
            if (!hivefile_key_condition.checkInHyperrectangle(hive_file->getMinMaxIndex()->hyperrectangle, hive_file_name_and_types.getTypes())
                     .can_be_true)
            {
                LOG_TRACE(
                    logger,
                    "Skip hive file {} by index {}",
                    hive_file->getPath(),
                    hive_file->describeMinMaxIndex(hive_file->getMinMaxIndex()));
                return {};
            }
        }

        if (prune_level >= HivePruneLevel::Split)
        {
            if (hive_file->useSplitMinMaxIndex())
            {
                /// Load sub-file level minmax index and apply
                std::unordered_set<int> skip_splits;
                hive_file->loadSplitMinMaxIndexes();
                const auto & sub_minmax_idxes = hive_file->getSubMinMaxIndexes();
                for (size_t i = 0; i < sub_minmax_idxes.size(); ++i)
                {
                    if (!hivefile_key_condition.checkInHyperrectangle(sub_minmax_idxes[i]->hyperrectangle, hive_file_name_and_types.getTypes())
                             .can_be_true)
                    {
                        LOG_TRACE(
                            logger,
                            "Skip split {} of hive file {} by index {}",
                            i,
                            hive_file->getPath(),
                            hive_file->describeMinMaxIndex(sub_minmax_idxes[i]));

                        skip_splits.insert(i);
                    }
                }
                hive_file->setSkipSplits(skip_splits);
            }
        }
    }
    return hive_file;
}
}
#endif
