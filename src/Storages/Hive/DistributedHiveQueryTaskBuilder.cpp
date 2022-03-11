#include <sstream>
#include <Storages/Hive/DistributedHiveQueryTaskBuilder.h>

#if USE_HIVE
#include <mutex>
#include <consistent_hashing.h>
#include <Core/Field.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Parsers/ASTFunction.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/IInputFormat.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/Hive/HiveQueryTaskBuilderFactory.h>
#include <base/logger_useful.h>
#include <Common/ErrorCodes.h>
#include <Common/SipHash.h>
#include <Common/ThreadPool.h>
#include "HiveFile.h"
namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_PARTITION_VALUE;
    extern const int NOT_FOUND_NODE;
}

std::shared_ptr<TaskIterator> DistributedHiveQueryTaskterateCallback::buildCallback(const Cluster::Address & address_)
{
    String key = address_.host_name + std::to_string(address_.port);
    auto iter = node_tasks.find(key);
    if (iter == node_tasks.end())
    {
        throw Exception(ErrorCodes::NOT_FOUND_NODE, "Not found node task({}:{})", address_.host_name, address_.port);
    }
    HiveQueryTaskPackage package;
    package.policy_name = HIVE_TASK_NODE_HASH_POLICY;
    packageToString(iter->second, package.data);

    auto callback = [package]()
    {
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

void DistributedHiveQueryTaskterateCallback::setupArgs(const Arguments & args_)
{
    args = args_;
    HiveFilesCollector files_collector(
        args.context,
        args.query_info,
        args.partition_by_ast,
        args.columns,
        args.hive_metastore_url,
        args.hive_database,
        args.hive_table,
        args.num_streams,
        args.storage_settings);
    auto total_hive_files = files_collector.collect();
    distributeHiveFilesToNodes(total_hive_files);
}

Cluster::Addresses DistributedHiveQueryTaskterateCallback::getSortedShardAddresses() const
{
    auto cluster = args.context->getCluster(args.cluster_name)->getClusterWithReplicasAsShards(args.context->getSettings());
    Cluster::Addresses addresses;
    for (const auto & replicas : cluster->getShardsAddresses())
    {
        for (const auto & node : replicas)
        {
            addresses.emplace_back(node);
        }
    }
    std::sort(std::begin(addresses), std::end(addresses), [](const Cluster::Address & a, const Cluster::Address & b)
    {
        return a.host_name > b.host_name && a.port > b.port;
    });
    return addresses;
}

void DistributedHiveQueryTaskterateCallback::distributeHiveFilesToNodes(const std::vector<HiveFilesCollector::FileInfo> & hive_file_infos_)
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
        WriteBufferFromOwnString ostr;
        for (const auto & partition_value : hive_file_info.partition_values)
        {
            ostr << partition_value;
        }
        auto idx = ConsistentHashing(path_hash, n);
        if (!node_tasks.count(nodes[idx]))
        {
            node_tasks[nodes[idx]] = DistributedHiveQueryTaskMetadata{hive_file_info.file_format, hive_file_info.hdfs_namenode_url, {}};
        }

        auto partition_key = ostr.str();
        auto & task_metadata = node_tasks[nodes[idx]];
        if (!task_metadata.files.count(partition_key))
        {
            task_metadata.files[partition_key] = DistributedHiveQueryTaskPartitionInfo{hive_file_info.partition_values, {}};
        }

        auto & files = task_metadata.files[partition_key].files;
        files.emplace_back(hive_file_info.file_info);
    }
    if (node_tasks.size() != n)
    {
        for (size_t i = 0; i < n; ++i)
        {
            if (node_tasks.count(nodes[i]))
                continue;
            node_tasks[nodes[i]] = DistributedHiveQueryTaskMetadata();
        }
    }
}

void DistributedHiveQueryTaskFilesCollector::setupArgs(const Arguments & args_)
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
}

void DistributedHiveQueryTaskFilesCollector::setupCallbackData(const String & data_)
{
    stringToPackage(data_, task_metadata);
}

HiveFiles DistributedHiveQueryTaskFilesCollector::collectHiveFiles()
{
    HiveFiles hive_files;
    for (const auto & partition_info : task_metadata.files)
    {
        auto partition_hive_files = collectPartitionHiveFiles(task_metadata.file_format, partition_info.second);
        hive_files.insert(hive_files.end(), partition_hive_files.begin(), partition_hive_files.end());
    }
    return hive_files;
}

HiveFiles
DistributedHiveQueryTaskFilesCollector::collectPartitionHiveFiles(const String & format_name_, const DistributedHiveQueryTaskPartitionInfo & partition_info_)
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
    for (size_t i = 0; i < partition_values.size(); ++i)
    {
        if (i)
            writeString(",", write_buf);
        writeString(partition_values[i], write_buf);
    }
    writeString("\n", write_buf);
    ReadBufferFromString read_buf(write_buf.str());
    auto format = FormatFactory::instance().getInputFormat(
        "CSV", read_buf, partition_key_expr->getSampleBlock(), args.context, args.context->getSettingsRef().max_block_size);

    auto pipeline = QueryPipeline(std::move(format));
    auto reader = std::make_unique<PullingPipelineExecutor>(pipeline);
    Block block;
    if (!reader->pull(block) || !block.rows())
        throw Exception(ErrorCodes::INVALID_PARTITION_VALUE, "Could not parse partition values: {}", write_buf.str());

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

void registerHiveTaskNodeHashPolicy(HiveQueryTaskBuilderFactory & factory_)
{
    DistributedHiveQueryTaskBuilderPtr builder = std::make_unique<DistributedHiveQueryTaskBuilder>();
    builder->task_iterator_callback = []() { return std::make_shared<DistributedHiveQueryTaskterateCallback>(); };
    builder->files_collector = []() { return std::make_shared<DistributedHiveQueryTaskFilesCollector>(); };
    factory_.registerBuilder(HIVE_TASK_NODE_HASH_POLICY, std::move(builder));
}

}
#endif
