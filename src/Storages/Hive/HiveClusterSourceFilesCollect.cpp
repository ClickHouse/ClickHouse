#include <memory>
#include <Storages/Hive/HiveClusterSourceFilesCollect.h>
#if USE_HIVE
#include <Storages/Hive/HiveFilesCollector.h>
#include <Common/SipHash.h>
#include <Poco/JSON/JSON.h>
#include <Parsers/ASTFunction.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <consistent_hashing.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_FOUND_NODE;
    extern const int INVALID_PARTITION_VALUE;
}
void HiveClusterSourceFilesCollectCallback::initialize(const Arguments & arguments)
{
    args = arguments;

    auto hive_files = HiveFilesCollector(
                          args.context,
                          args.query_info,
                          args.partition_by_ast,
                          args.columns,
                          args.hive_metastore_url,
                          args.hive_database,
                          args.hive_table,
                          args.num_streams,
                          args.storage_settings)
                          .collect();
    dispatchHiveFiles(hive_files);
}

void HiveClusterSourceFilesCollectCallback::dispatchHiveFiles(const HiveFiles & hive_files)
{
    auto cluster = args.context->getCluster(args.cluster_name)->getClusterWithReplicasAsShards(args.context->getSettings());
    Strings nodes;
    for (const auto & replicas : cluster->getShardsAddresses())
    {
        for (const auto & node : replicas)
        {
            nodes.emplace_back(node.host_name + std::to_string(node.port));
        }
    }
    std::sort(nodes.begin(), nodes.end());

    size_t nodes_num = nodes.size();
    std::vector<std::shared_ptr<Poco::JSON::Array>> nodes_json_obj(nodes_num, nullptr);
    for (const auto & hive_file : hive_files)
    {
        const auto & path = hive_file->getPath();
        auto path_hash = sipHash64(path.c_str(), path.size());
        Poco::JSON::Array partition_values;
        for (const auto & partition_value : hive_file->getPartitionValues())
        {
            partition_values.add(partition_value.dump());
        }

        auto node_index = ConsistentHashing(path_hash, nodes_num);
        if (!nodes_json_obj[node_index])
        {
            nodes_json_obj[node_index] = std::make_shared<Poco::JSON::Array>();
        }

        Poco::JSON::Object file_data;
        file_data.set("file_format", IHiveFile::toCHFormat(hive_file->getFormat()));
        file_data.set("hdfs_name_node_url", hive_file->getNamenodeUrl());
        file_data.set("path", hive_file->getPath());
        file_data.set("last_modified_timestamp", hive_file->getLastModifiedTimestamp());
        file_data.set("size", hive_file->getSize());
        file_data.set("partition_values", partition_values);
        nodes_json_obj[node_index]->add(file_data);
    }

    for (size_t i = 0, sz = nodes_json_obj.size(); i < sz; ++i)
    {
        auto & node_json_obj = nodes_json_obj[i];
        if (!node_json_obj)
        {
            nodes_callback_data[nodes[i]] = "[]";
        }
        else
        {
            std::stringstream buf; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
            node_json_obj->stringify(buf);
            nodes_callback_data[nodes[i]] = buf.str();
        }
    }
}

std::shared_ptr<TaskIterator> HiveClusterSourceFilesCollectCallback::buildCollectCallback(const Cluster::Address & address)
{
    String node_str = address.host_name + std::to_string(address.port);
    auto it = nodes_callback_data.find(node_str);
    if (it == nodes_callback_data.end())
        throw Exception(ErrorCodes::NOT_FOUND_NODE, "Not found callback data for node({}:{})", address.host_name, address.port);

    String data = it->second;

    auto res = std::make_shared<TaskIterator>();
    *res = [data]() { return data; };
    return res;
}

void registerNodeHashHiveSourceFilesCollectCallback(HiveSourceCollectCallbackFactory & factory)
{
    factory.registerBuilder(
        HiveClusterSourceFilesCollectCallback::NAME, []() { return std::make_shared<HiveClusterSourceFilesCollectCallback>(); });
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

void HiveClusterSourceFilesCollector::initialize(const Arguments & arguments)
{
    args = arguments;
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

    Poco::JSON::Parser files_json_parser;
    files_in_json = files_json_parser.parse(args.callback_data).extract<Poco::JSON::Array::Ptr>();
}

HiveFiles HiveClusterSourceFilesCollector::collect(HivePruneLevel /*prune_level*/)
{
    HiveFiles hive_files;
    for (const auto & file_obj : *files_in_json)
    {
        auto file_data = *file_obj.extract<Poco::JSON::Object::Ptr>();
        auto file_format = file_data.get("file_format").convert<String>();
        auto hdfs_name_node_url = file_data.get("hdfs_name_node_url").convert<String>();
        auto path = file_data.get("path").convert<String>();
        auto last_modified_timestamp = file_data.get("last_modified_timestamp").convert<UInt64>();
        auto file_size = file_data.get("size").convert<UInt64>();

        auto partition_fields_obj = *file_data.get("partition_values").extract<Poco::JSON::Array::Ptr>();
        FieldVector partition_fields;
        for (auto & partition_field_obj : partition_fields_obj)
        {
            auto value = partition_field_obj.convert<String>();
            if (value == "__HIVE_DEFAULT_PARTTION_")
                continue;
            partition_fields.emplace_back(Field::restoreFromDump(value));
        }

        if (partition_fields.size() != partition_name_and_types.size())
            throw Exception(
                ErrorCodes::INVALID_PARTITION_VALUE,
                "Partition value size not match. expected {}, but get {}",
                partition_name_and_types.size(),
                partition_fields.size());


        auto hive_file = HiveFileFactory::instance().createFile(
            file_format,
            partition_fields,
            hdfs_name_node_url,
            path,
            last_modified_timestamp,
            file_size,
            hive_file_name_and_types,
            args.storage_settings,
            args.context);

        hive_files.emplace_back(hive_file);
    }
    return hive_files;
}
void registerNodeHashHiveSourceFilesCollector(HiveSourceCollectorFactory & factory)
{
    factory.registerBuilder(HiveClusterSourceFilesCollector::NAME, []() { return std::make_shared<HiveClusterSourceFilesCollector>(); });
}
}
#endif

