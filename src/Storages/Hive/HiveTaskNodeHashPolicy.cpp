#include <sstream>
#include <Storages/Hive/HiveTaskNodeHashPolicy.h>

#if USE_HIVE
#include <consistent_hashing.h>
#include <mutex>
#include <base/logger_useful.h>
#include <Common/SipHash.h>
#include <Common/ThreadPool.h>
#include <Common/ErrorCodes.h>
#include <Core/Field.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Parsers/ASTFunction.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/IInputFormat.h>
#include <QueryPipeline/Pipe.h>
namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_PARTITION_VALUE;
    extern const int NOT_IMPLEMENTED;
    extern const int NOT_FOUND_NODE;
}

std::shared_ptr<TaskIterator> HiveTaskNodeHashIterateCallback::buildCallback(const Cluster::Address & address_)
{
    String key = address_.host_name + std::to_string(address_.port);
    auto iter = node_tasks.find(key);
    if (iter == node_tasks.end())
    {
        throw Exception(ErrorCodes::NOT_FOUND_NODE, "Not found node task({}:{})", address_.host_name, address_.port);
    }
    HiveTaskPackage package;
    package.policy_name = HIVE_TASK_NODE_HASH_POLICY;
    packageToString(iter->second, package.data);
    
    auto callback = [package](){
        String data;
        packageToString(package, data);
        return data;
    };

    return std::make_shared<TaskIterator>(callback);
}  


static ASTPtr extractKeyExpressionList(const ASTPtr & node)
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
void HiveTaskNodeHashIterateCallback::init(const Arguments &args_)
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
            std::make_shared<ActionsDAG>(partition_name_and_types),
            ExpressionActionsSettings::fromContext(args.context));
    }

    for (const auto & column : all_name_and_types)
    {
        if (partition_name_and_types.contains(column.name))
            hive_file_name_and_types.push_back(column);
    }

    hive_file_minmax_idx_expr = std::make_shared<ExpressionActions>(
        std::make_shared<ActionsDAG>(hive_file_name_and_types),
        ExpressionActionsSettings::fromContext(args.context));

    auto all_hive_files = collectHiveFiles();

    distributeHiveFilesToNodes(all_hive_files);
   
}

std::vector<HiveTaskFileInfo> HiveTaskNodeHashIterateCallback::collectHiveFiles()
{
    auto hive_metastore_client = HiveMetastoreClientFactory::instance().getOrCreate(args.hive_metastore_url, args.context);
    auto hive_table_metadata = hive_metastore_client->getTableMetadata(args.hive_database, args.hive_table);
    auto partitions = hive_table_metadata->getPartitions();
    hdfs_namenode_url = getNameNodeUrl(hive_table_metadata->getTable()->sd.location);
    auto hdfs_builder = createHDFSBuilder(hdfs_namenode_url, args.context->getGlobalContext()->getConfigRef());
    auto hdfs_fs = createHDFSFS(hdfs_builder.get());

    format_name = IHiveFile::toHiveFileFormat(hive_table_metadata->getTable()->sd.inputFormat);

    std::vector<HiveTaskFileInfo> hive_files;
    std::mutex hive_files_mutex;
    ThreadPool thread_pool{args.num_streams};
    if (!partitions.empty())
    {
        for (const auto & partition : partitions)
        {
            thread_pool.scheduleOrThrowOnError([&]()
            {
                auto hive_files_in_partition = collectHiveFilesFromPartition(partition, *args.query_info, hive_table_metadata, hdfs_fs, args.context);
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
            thread_pool.scheduleOrThrow([&]
            {
                auto hive_file = createHiveFileIfNeeded(file_info, {}, *args.query_info, args.context);
                if (hive_file)
                {
                    std::lock_guard<std::mutex> lock(hive_files_mutex);
                    hive_files.emplace_back(HiveTaskFileInfo{.file_info = file_info, .partition_values = {}});
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
            partitions.size(), partition_name_and_types.size());
    }
    return hive_files;
}

std::vector<HiveTaskFileInfo> HiveTaskNodeHashIterateCallback::collectHiveFilesFromPartition(
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
        "CSV",
        read_buf,
        partition_key_expr->getSampleBlock(),
        context_,
        context_->getSettingsRef().max_block_size);

    auto pipeline = QueryPipeline(std::move(format));
    auto reader = std::make_unique<PullingPipelineExecutor>(pipeline);
    Block block;
    if (!reader->pull(block) || !block.rows())
        throw Exception("Could not parse partition value: " + write_buf.str(), ErrorCodes::INVALID_PARTITION_VALUE);
    std::vector<Range> ranges;
    ranges.reserve(partition_name_and_types.size());
    FieldVector fields(partition_name_and_types.size());
    for (size_t i = 0; i < partition_name_and_types.size(); ++i){
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
    std::vector<HiveTaskFileInfo> hive_files;
    hive_files.reserve(file_infos.size());
    for (const auto & file_info : file_infos)
    {
        auto hive_file = createHiveFileIfNeeded(file_info, fields, query_info_, args.context);
        if (hive_file)
            hive_files.emplace_back(HiveTaskFileInfo{.file_info = file_info, .partition_values = partition_values});
    }
    return hive_files;

}

static std::string getBaseName(const String & path)
{
    size_t basename_start = path.rfind('/');
    return path.substr(basename_start + 1);
}

HiveFilePtr HiveTaskNodeHashIterateCallback::createHiveFileIfNeeded(
        const HiveMetastoreClient::FileInfo & file_info_,
        const FieldVector & fields_,
        SelectQueryInfo & query_info_,
        ContextPtr context_ )
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
        if (!hive_file_key_condition.checkInHyperrectangle(hive_file->getMinMaxIndex()->hyperrectangle, hive_file_name_and_types.getTypes()).can_be_true)
        {
            LOG_TRACE(logger, "Skip hive file {} by index {}", hive_file->getPath(), hive_file->describeMinMaxIndex(hive_file->getMinMaxIndex()));
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
            if (!hive_file_key_condition.checkInHyperrectangle(sub_minmax_idxes[i]->hyperrectangle, hive_file_name_and_types.getTypes()).can_be_true)
            {
                LOG_TRACE(logger, "Skip split {} of hive file {}", i, hive_file->getPath());
                skip_splits.insert(i);
            }
        }
        hive_file->setSkipSplits(skip_splits);
    }
    return hive_file;
}

Cluster::Addresses HiveTaskNodeHashIterateCallback::getSortedShardAddresses() const{
    auto cluster = args.context->getCluster(args.cluster_name)->getClusterWithReplicasAsShards(args.context->getSettings());
    Cluster::Addresses addresses;
    for (const auto & replicas : cluster->getShardsAddresses())
    {
        for (const auto & node : replicas)
        {
            addresses.emplace_back(node);
        }
    }
    std::sort(std::begin(addresses),
        std::end(addresses),
        [](const Cluster::Address & a, const Cluster::Address & b)
        {
            return a.host_name > b.host_name && a.port > b.port;
        }
    );
    return addresses;    
}

void HiveTaskNodeHashIterateCallback::distributeHiveFilesToNodes(const std::vector<HiveTaskFileInfo> & hive_file_infos_)
{
    auto sorted_addresses = getSortedShardAddresses();
    std::vector<String> nodes;
    nodes.reserve(sorted_addresses.size());
    for (const auto & node : sorted_addresses)
    {
        nodes.emplace_back(node.host_name + std::to_string(node.port));
    }

    size_t n = nodes.size();
    for (const auto & hive_file_info : hive_file_infos_)
    {
        auto path_hash = sipHash64(hive_file_info.file_info.path.c_str(), hive_file_info.file_info.path.size());
        std::ostringstream ostr;
        for (const auto & partition_value : hive_file_info.partition_values)
        {
            ostr << partition_value;
        }
        auto idx = ConsistentHashing(path_hash, n);
        if (!node_tasks.count(nodes[idx]))
        {
            node_tasks[nodes[idx]] = AssginedTaskMetadata{format_name, hdfs_namenode_url, {}};
        }
        
        auto partition_key = ostr.str();
        auto & task_metadata = node_tasks[nodes[idx]];
        if (!task_metadata.files.count(partition_key))
        {
            task_metadata.files[partition_key] = HiveTaskPartitionInfo{ hive_file_info.partition_values, {}};
        }

        auto & files = task_metadata.files[partition_key].files;
        files.emplace_back(hive_file_info.file_info);
    }
}

void HiveTaskNodeHashFilesCollector::initQueryEnv(const Arguments & args_)
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
            std::make_shared<ActionsDAG>(partition_name_and_types),
            ExpressionActionsSettings::fromContext(args.context));
    }

    for (const auto & column : all_name_and_types)
    {
        if (partition_name_and_types.contains(column.name))
            hive_file_name_and_types.push_back(column);
    }
}

void HiveTaskNodeHashFilesCollector::setupCallbackData(const String & data_)
{
    stringToPackage(data_, task_metadata);
    std::ostringstream ostr;
    ostr << task_metadata;
    LOG_TRACE(logger, "callback data : {}", ostr.str());
}

HiveFiles HiveTaskNodeHashFilesCollector::collectHiveFiles()
{
    HiveFiles hive_files;
    for (const auto & partition_info : task_metadata.files)
    {
        auto partition_hive_files = collectPartitionHiveFiles(task_metadata.file_format, partition_info.second);
        hive_files.insert(hive_files.end(), partition_hive_files.begin(), partition_hive_files.end());
    }
    return hive_files;
}

HiveFiles HiveTaskNodeHashFilesCollector::collectPartitionHiveFiles(const String & format_name_, const HiveTaskPartitionInfo & partition_info_)
{
    HiveFiles hive_files;
    const auto & partition_values = partition_info_.partition_values;
    for (const auto & partition_value : partition_values)
    {
        if (partition_value == "__HIVE_DEFAULT_PARTTION_")
            return {};
    }

    if (partition_values.size() != partition_name_and_types.size())
        throw Exception(
            fmt::format("Partition value size not match, expect {}, but got {}", partition_name_and_types.size(), partition_values.size()),
            ErrorCodes::INVALID_PARTITION_VALUE);

    WriteBufferFromOwnString write_buf;
    for(size_t i = 0; i < partition_values.size(); ++i)
    {
        if(i)
            writeString(",", write_buf);
        writeString(partition_values[i], write_buf);
    }
    writeString("\n", write_buf);
    ReadBufferFromString read_buf(write_buf.str());
    auto format = FormatFactory::instance().getInputFormat(
        "CSV",
        read_buf,
        partition_key_expr->getSampleBlock(),
        args.context,
        args.context->getSettingsRef().max_block_size);

    auto pipeline = QueryPipeline(std::move(format));
    auto reader = std::make_unique<PullingPipelineExecutor>(pipeline);
    Block block;
    if (!reader->pull(block) || !block.rows())
        throw Exception(ErrorCodes::INVALID_PARTITION_VALUE, "Could not parse parition values: {}", write_buf.str());
    
    FieldVector fields(partition_name_and_types.size());
    for (size_t i = 0; i < partition_name_and_types.size(); ++i)
    {
        block.getByPosition(i).column->get(0, fields[i]);
    }
    
    hive_files.reserve(partition_info_.files.size());
    for (const auto & file_info : partition_info_.files)
    {
        auto hive_file = createHiveFile(
            format_name_,
            fields,
            task_metadata.hdfs_namenode_url,
            file_info.path,
            file_info.last_modify_time,
            file_info.size,
            hive_file_name_and_types,
            args.storage_settings,
            args.context);
        hive_files.emplace_back(hive_file);
    }
    return hive_files;
}

void registerHiveTaskNodeHashPolicy(HiveTaskPolicyFactory & factory_)
{
    auto iterate_callback_builder = [] (){ return std::make_shared<HiveTaskNodeHashIterateCallback>(); };
    auto files_collector_builder = [] (){ return std::make_shared<HiveTaskNodeHashFilesCollector>(); };
    factory_.registerBuilders(
        HIVE_TASK_NODE_HASH_POLICY,
        iterate_callback_builder,
        files_collector_builder);
}

} // namespace DB
#endif
