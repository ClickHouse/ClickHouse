#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Storages/System/StorageSystemZooKeeper.h>
#include <Storages/SelectQueryInfo.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperRetries.h>
#include <Common/ZooKeeper/ZooKeeperWithFaultInjection.h>
#include <Common/typeid_cast.h>
#include <Columns/ColumnSet.h>
#include <Columns/ColumnConst.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Functions/IFunction.h>
#include <Parsers/ASTSubquery.h>
#include <Interpreters/Set.h>
#include <Interpreters/interpretSubquery.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string.hpp>
#include <algorithm>
#include <deque>


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_unrestricted_reads_from_keeper;
    extern const SettingsFloat insert_keeper_fault_injection_probability;
    extern const SettingsUInt64 insert_keeper_fault_injection_seed;
    extern const SettingsUInt64 insert_keeper_max_retries;
    extern const SettingsUInt64 insert_keeper_retry_initial_backoff_ms;
    extern const SettingsUInt64 insert_keeper_retry_max_backoff_ms;
    extern const SettingsMaxThreads max_download_threads;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

/** ZkNodeCache is a trie tree to cache all the zookeeper writes. The purpose of this struct is to avoid creating/setting nodes
  * repeatedly. For example, If we create path /a/b/c/d/e and path /a/b/d/f in the same transaction. We don't want to create
  * their common path "/a/b" twice. This data structure will cache this changes and generates the eventual requests within one pass.
  */
struct ZkNodeCache
{
    using ZkNodeCachePtr = std::shared_ptr<ZkNodeCache>;

    std::unordered_map<String, ZkNodeCachePtr> children;
    String value;
    String path;
    bool exists;
    bool changed;

    ZkNodeCache() : exists(true), changed(false) { }
    ZkNodeCache(String path_, bool exists_) : path(path_), exists(exists_), changed(false) { }

    void insert(const std::vector<String> & nodes, zkutil::ZooKeeperPtr zookeeper, const String & value_to_set, size_t index)
    {
        /// If this node has an empty name, just skip it.
        /// Possibly a "/a//b///c//d/" will cause empty node.
        while (index < nodes.size() && nodes[index].empty())
            ++index;

        if (index == nodes.size())
        {
            value = value_to_set;
            changed = true;
            return;
        }
        const String & child_name = nodes[index];
        ++index;
        if (!children.contains(child_name))
        {
            String sub_path = path + "/" + child_name;
            bool child_exist = false;
            if (exists)
            {
                /// If this node doesn't exists, neither will its child.
                child_exist = zookeeper->exists(sub_path);
            }
            children[child_name] = std::make_shared<ZkNodeCache>(sub_path, child_exist);
        }
        children[child_name]->insert(nodes, zookeeper, value_to_set, index);
    }

    void generateRequests(Coordination::Requests & requests)
    {
        /** If the node doesn't exists, we should generate create request.
          * If the node exists, we should generate set request.
          * This dfs will prove ancestor nodes are processed first.
          */
        if (!exists)
        {
            auto request = zkutil::makeCreateRequest(path, value, zkutil::CreateMode::Persistent);
            requests.push_back(request);
        }
        else if (changed)
        {
            auto request = zkutil::makeSetRequest(path, value, -1);
            requests.push_back(request);
        }
        for (const auto & [_, child] : children)
            child->generateRequests(requests);
    }
};

class ZooKeeperSink : public SinkToStorage
{
    zkutil::ZooKeeperPtr zookeeper;

    ZkNodeCache cache;

public:
    ZooKeeperSink(const Block & header, ContextPtr context) : SinkToStorage(header), zookeeper(context->getZooKeeper()) { }
    String getName() const override { return "ZooKeeperSink"; }

    void consume(Chunk & chunk) override
    {
        auto block = getHeader().cloneWithColumns(chunk.getColumns());
        size_t rows = block.rows();
        for (size_t i = 0; i < rows; i++)
        {
            String name = block.getByPosition(0).column->getDataAt(i).toString();
            String value = block.getByPosition(1).column->getDataAt(i).toString();
            String path = block.getByPosition(2).column->getDataAt(i).toString();

            /// We don't expect a "name" contains a path.
            if (name.find('/') != std::string::npos)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Column `name` should not contain '/'");
            }

            if (name.empty())
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Column `name` should not be empty");
            }

            if (path.empty())
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Column `path` should not be empty");
            }

            if (path.size() + name.size() > PATH_MAX)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Sum of `name` length and `path` length should not exceed PATH_MAX");
            }

            std::vector<String> path_vec;
            boost::split(path_vec, path, boost::is_any_of("/"));
            path_vec.push_back(name);
            cache.insert(path_vec, zookeeper, value, 0);
        }
    }

    void onFinish() override
    {
        Coordination::Requests requests;
        cache.generateRequests(requests);
        zookeeper->multi(requests);
    }
};

/// Type of path to be fetched
enum class ZkPathType : uint8_t
{
    Exact, /// Fetch all nodes under this path
    Prefix, /// Fetch all nodes starting with this prefix, recursively (multiple paths may match prefix)
    Recurse, /// Fetch all nodes under this path, recursively
};

/// List of paths to be fetched from zookeeper
using Paths = std::unordered_map<String, ZkPathType>;

class ReadFromSystemZooKeeper final : public SourceStepWithFilter
{
public:
    ReadFromSystemZooKeeper(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        const Block & header,
        UInt64 max_block_size_);

    String getName() const override { return "ReadFromSystemZooKeeper"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void applyFilters(ActionDAGNodes added_filter_nodes) override;

private:
    std::shared_ptr<const StorageLimitsList> storage_limits;
    const UInt64 max_block_size;
    Paths paths;
};


class SystemZooKeeperSource : public ISource
{
public:
    SystemZooKeeperSource(
        Paths && paths_,
        Block header_,
        UInt64 max_block_size_,
        ContextPtr context_)
        : ISource(header_)
        , max_block_size(max_block_size_)
        , paths(std::move(paths_))
        , context(std::move(context_))
    {
    }

    String getName() const override { return "SystemZooKeeper"; }

protected:
    Chunk generate() override;

private:
    const UInt64 max_block_size;
    Paths paths;
    ContextPtr context;
    ZooKeeperWithFaultInjection::Ptr zookeeper;
    bool started = false;
    std::unordered_set<String> visited;
};


StorageSystemZooKeeper::StorageSystemZooKeeper(const StorageID & table_id_)
        : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(getColumnsDescription());
    setInMemoryMetadata(storage_metadata);
}

void StorageSystemZooKeeper::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t /*num_streams*/)
{
    auto header = storage_snapshot->metadata->getSampleBlockWithVirtuals(getVirtualsList());
    auto read_step = std::make_unique<ReadFromSystemZooKeeper>(
        column_names,
        query_info,
        storage_snapshot,
        context,
        header,
        max_block_size);
    query_plan.addStep(std::move(read_step));
}

SinkToStoragePtr StorageSystemZooKeeper::write(const ASTPtr &, const StorageMetadataPtr &, ContextPtr context, bool /*async_insert*/)
{
    if (!context->getConfigRef().getBool("allow_zookeeper_write", false))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Prohibit writing to system.zookeeper, unless config `allow_zookeeper_write` as true");
    Block write_header;
    write_header.insert(ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "name"));
    write_header.insert(ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "value"));
    write_header.insert(ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "path"));
    return std::make_shared<ZooKeeperSink>(write_header, context);
}

ColumnsDescription StorageSystemZooKeeper::getColumnsDescription()
{
    auto description = ColumnsDescription
    {
        {"name",           std::make_shared<DataTypeString>(), "The name of the node."},
        {"value",          std::make_shared<DataTypeString>(), "Node value."},
        {"czxid",          std::make_shared<DataTypeInt64>(), "ID of the transaction that created the node."},
        {"mzxid",          std::make_shared<DataTypeInt64>(), "ID of the transaction that last changed the node."},
        {"ctime",          std::make_shared<DataTypeDateTime>(), "Time of node creation."},
        {"mtime",          std::make_shared<DataTypeDateTime>(), "Time of the last modification of the node."},
        {"version",        std::make_shared<DataTypeInt32>(), "Node version: the number of times the node was changed."},
        {"cversion",       std::make_shared<DataTypeInt32>(), "Number of added or removed descendants."},
        {"aversion",       std::make_shared<DataTypeInt32>(), "Number of changes to the ACL."},
        {"ephemeralOwner", std::make_shared<DataTypeInt64>(), "For ephemeral nodes, the ID of the session that owns this node."},
        {"dataLength",     std::make_shared<DataTypeInt32>(), "Size of the value."},
        {"numChildren",    std::make_shared<DataTypeInt32>(), "Number of descendants."},
        {"pzxid",          std::make_shared<DataTypeInt64>(), "ID of the transaction that last deleted or added descendants."},
        {"path",           std::make_shared<DataTypeString>(), "The path to the node."},
    };

    for (auto & name : description.getAllRegisteredNames())
    {
        description.modify(name, [&](ColumnDescription & column)
        {
            /// We only allow column `name`, `path`, `value` to insert.
            if (column.name != "name" && column.name != "path" && column.name != "value")
                column.default_desc.kind = ColumnDefaultKind::Materialized;
        });
    }

    return description;
}

static String pathCorrected(const String & path)
{
    String path_corrected;
    /// path should starts with '/', otherwise ZBADARGUMENTS will be thrown in
    /// ZooKeeper::sendThread and the session will fail.
    if (path.empty() || path[0] != '/')
        path_corrected = '/';
    path_corrected += path;
    /// In all cases except the root, path must not end with a slash.
    if (path_corrected != "/" && path_corrected.back() == '/')
        path_corrected.resize(path_corrected.size() - 1);
    return path_corrected;
}

static bool isPathNode(const ActionsDAG::Node * node)
{
    while (node->type == ActionsDAG::ActionType::ALIAS)
        node = node->children.at(0);

    return node->result_name == "path";
}

static void extractPathImpl(const ActionsDAG::Node & node, Paths & res, ContextPtr context, bool allow_unrestricted)
{
    if (node.type != ActionsDAG::ActionType::FUNCTION)
        return;

    auto function_name = node.function_base->getName();
    if (function_name == "and")
    {
        for (const auto * child : node.children)
            extractPathImpl(*child, res, context, allow_unrestricted);

        return;
    }

    if (node.children.size() != 2)
        return;

    if (function_name == "in")
    {
        if (!isPathNode(node.children.at(0)))
            return;

        auto value = node.children.at(1)->column;
        if (!value)
            return;

        const IColumn * column = value.get();
        if (const auto * column_const = typeid_cast<const ColumnConst *>(column))
            column = &column_const->getDataColumn();

        const ColumnSet * column_set = typeid_cast<const ColumnSet *>(column);
        if (!column_set)
            return;

        auto future_set = column_set->getData();
        if (!future_set)
            return;

        auto set = future_set->buildOrderedSetInplace(context);
        if (!set || !set->hasExplicitSetElements())
            return;

        set->checkColumnsNumber(1);
        auto type = set->getElementsTypes()[0];
        if (!isString(removeNullable(removeLowCardinality(type))))
            return;

        auto values = set->getSetElements()[0];
        size_t size = values->size();

        for (size_t row = 0; row < size; ++row)
            /// Only inserted if the key doesn't exists already
            res.insert({values->getDataAt(row).toString(), ZkPathType::Exact});
    }
    else if (function_name == "equals")
    {
        const ActionsDAG::Node * value = nullptr;

        if (isPathNode(node.children.at(0)))
            value = node.children.at(1);
        else if (isPathNode(node.children.at(1)))
            value = node.children.at(0);

        if (!value || !value->column)
            return;

        if (!isString(removeNullable(removeLowCardinality(value->result_type))))
            return;

        if (value->column->size() != 1)
            return;

        /// Only inserted if the key doesn't exists already
        res.insert({value->column->getDataAt(0).toString(), ZkPathType::Exact});
    }
    else if (allow_unrestricted && function_name == "like")
    {
        if (!isPathNode(node.children.at(0)))
            return;

        const auto * value = node.children.at(1);
        if (!value->column)
            return;

        if (!isString(removeNullable(removeLowCardinality(value->result_type))))
            return;

        if (value->column->size() != 1)
            return;

        String pattern = value->column->getDataAt(0).toString();
        bool has_metasymbol = false;
        String prefix{}; // pattern prefix before the first metasymbol occurrence
        for (size_t i = 0; i < pattern.size(); i++)
        {
            char c = pattern[i];
            // Handle escaping of metasymbols
            if (c == '\\' && i + 1 < pattern.size())
            {
                char c2 = pattern[i + 1];
                if (c2 == '_' || c2 == '%')
                {
                    prefix.append(1, c2);
                    i++; // to skip two bytes
                    continue;
                }
            }

            // Stop prefix on the first metasymbols occurrence
            if (c == '_' || c == '%')
            {
                has_metasymbol = true;
                break;
            }

            prefix.append(1, c);
        }

        res.insert_or_assign(prefix, has_metasymbol ? ZkPathType::Prefix : ZkPathType::Exact);
    }
}


/** Retrieve from the query a condition of the form `path = 'path'`, from conjunctions in the WHERE clause.
  */
static Paths extractPath(const ActionsDAG::NodeRawConstPtrs & filter_nodes, ContextPtr context, bool allow_unrestricted)
{
    Paths res;
    for (const auto * node : filter_nodes)
        extractPathImpl(*node, res, context, allow_unrestricted);

    auto node1 = res.find("/");
    auto node2 = res.find("");
    if ((node1 != res.end() && node1->second != ZkPathType::Exact) || (node2 != res.end() && node2->second != ZkPathType::Exact))
    {
        /// If we are already searching everything recursively, remove all other nodes
        res.clear();
        res.insert({"/", ZkPathType::Recurse});
    }

    if (res.empty() && allow_unrestricted)
        res.insert({"/", ZkPathType::Recurse});

    return res;
}


void ReadFromSystemZooKeeper::applyFilters(ActionDAGNodes added_filter_nodes)
{
    SourceStepWithFilter::applyFilters(added_filter_nodes);

    paths = extractPath(added_filter_nodes.nodes, context, context->getSettingsRef()[Setting::allow_unrestricted_reads_from_keeper]);
}


Chunk SystemZooKeeperSource::generate()
{
    if (paths.empty())
    {
        if (!started)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "SELECT from system.zookeeper table must contain condition like path = 'path' "
                        "or path IN ('path1','path2'...) or path IN (subquery) "
                        "in WHERE clause unless `set allow_unrestricted_reads_from_keeper = 'true'`.");

        /// No more work
        return {};
    }

    started = true;

    MutableColumns res_columns = getPort().getHeader().cloneEmptyColumns();
    size_t row_count = 0;

    QueryStatusPtr query_status = context->getProcessListElement();

    const auto & settings = context->getSettingsRef();
    /// Use insert settings for now in order not to introduce new settings.
    /// Hopefully insert settings will also be unified and replaced with some generic retry settings.
    ZooKeeperRetriesInfo retries_seetings(
        settings[Setting::insert_keeper_max_retries],
        settings[Setting::insert_keeper_retry_initial_backoff_ms],
        settings[Setting::insert_keeper_retry_max_backoff_ms]);

    /// Handles reconnects when needed
    auto get_zookeeper = [&] ()
    {
        if (!zookeeper || zookeeper->expired())
        {
            zookeeper = ZooKeeperWithFaultInjection::createInstance(
                settings[Setting::insert_keeper_fault_injection_probability],
                settings[Setting::insert_keeper_fault_injection_seed],
                context->getZooKeeper(),
                "",
                nullptr);
        }
        return zookeeper;
    };

    const Int64 max_inflight_requests = std::max<Int64>(1, context->getSettingsRef()[Setting::max_download_threads].value);

    struct ListTask
    {
        String path;
        ZkPathType path_type;
        String prefix;
        String path_corrected;
        String path_part;
    };
    std::vector<ListTask> list_tasks;
    while (!paths.empty())
    {
        if (query_status)
            query_status->checkTimeLimit();

        /// Check if the block is big enough already
        if (max_block_size > 0 && row_count > 0)
        {
            size_t total_size = 0;
            for (const auto & column : res_columns)
                total_size += column->byteSize();
            if (total_size > max_block_size)
                break;
        }

        list_tasks.clear();
        std::vector<String> paths_to_list;
        while (!paths.empty() && static_cast<Int64>(list_tasks.size()) < max_inflight_requests)
        {
            auto node = paths.extract(paths.begin());
            auto & path = node.key();
            auto & path_type = node.mapped();

            ListTask task;
            task.path = path;
            task.path_type = path_type;
            if (path_type == ZkPathType::Prefix)
            {
                task.prefix = path;
                size_t last_slash = task.prefix.rfind('/');
                path = task.prefix.substr(0, last_slash == String::npos ? 0 : last_slash);
            }

            task.path_corrected = pathCorrected(path);

            paths_to_list.emplace_back(task.path_corrected);
            list_tasks.emplace_back(std::move(task));
        }

        zkutil::ZooKeeper::MultiTryGetChildrenResponse list_responses;
        ZooKeeperRetriesControl("", nullptr, retries_seetings, query_status).retryLoop(
            [&]() { list_responses = get_zookeeper()->tryGetChildren(paths_to_list); });

        struct GetTask
        {
            size_t list_task_idx;   /// Index of 'parent' request in list_tasks
            String node;            /// Node name
        };
        std::vector<GetTask> get_tasks;
        std::vector<String> paths_to_get;
        for (size_t list_task_idx = 0; list_task_idx < list_tasks.size(); ++list_task_idx)
        {
            auto & list_result = list_responses[list_task_idx];
            /// Node can be deleted concurrently. It's Ok, we don't provide any
            /// consistency guarantees for system.zookeeper table.
            if (list_result.error == Coordination::Error::ZNONODE)
                continue;

            auto & task = list_tasks[list_task_idx];
            if (query_status)
                query_status->checkTimeLimit();

            Strings nodes = std::move(list_result.names);

            task.path_part = task.path_corrected;
            if (task.path_part == "/")
                task.path_part.clear();

            if (!task.prefix.empty())
            {
                // Remove nodes that do not match specified prefix
                std::erase_if(nodes, [&task] (const String & node)
                {
                    return (task.path_part + '/' + node).substr(0, task.prefix.size()) != task.prefix;
                });
            }

            get_tasks.reserve(get_tasks.size() + nodes.size());
            for (const String & node : nodes)
            {
                paths_to_get.emplace_back(task.path_part + '/' + node);
                get_tasks.emplace_back(GetTask{list_task_idx, node});
            }
        }

        zkutil::ZooKeeper::MultiTryGetResponse get_responses;
        ZooKeeperRetriesControl("", nullptr, retries_seetings, query_status).retryLoop(
            [&]() { get_responses = get_zookeeper()->tryGet(paths_to_get); });

        /// Add children count to query total rows. We can not get total rows in advance,
        /// because it is too heavy to get row count for non exact paths.
        /// Please be aware that there might be minor setbacks in the query progress,
        /// but overall it should reflect the advancement of the query.
        size_t children_count = 0;
        for (size_t i = 0, size = get_tasks.size(); i < size; ++i)
        {
            auto & res = get_responses[i];
            if (res.error == Coordination::Error::ZNONODE)
                continue; /// Node was deleted meanwhile.
            children_count += res.stat.numChildren;
        }
        addTotalRowsApprox(children_count);

        for (size_t i = 0, size = get_tasks.size(); i < size; ++i)
        {
            auto & res = get_responses[i];
            if (res.error == Coordination::Error::ZNONODE)
                continue; /// Node was deleted meanwhile.

            auto & get_task = get_tasks[i];
            auto & list_task = list_tasks[get_task.list_task_idx];
            if (query_status)
                query_status->checkTimeLimit();

            // Deduplication
            String key = list_task.path_part + '/' + get_task.node;
            if (auto [it, inserted] = visited.emplace(key); !inserted)
                continue;

            const Coordination::Stat & stat = res.stat;

            size_t col_num = 0;
            res_columns[col_num++]->insert(get_task.node);
            res_columns[col_num++]->insert(res.data);
            res_columns[col_num++]->insert(stat.czxid);
            res_columns[col_num++]->insert(stat.mzxid);
            res_columns[col_num++]->insert(UInt64(stat.ctime / 1000));
            res_columns[col_num++]->insert(UInt64(stat.mtime / 1000));
            res_columns[col_num++]->insert(stat.version);
            res_columns[col_num++]->insert(stat.cversion);
            res_columns[col_num++]->insert(stat.aversion);
            res_columns[col_num++]->insert(stat.ephemeralOwner);
            res_columns[col_num++]->insert(stat.dataLength);
            res_columns[col_num++]->insert(stat.numChildren);
            res_columns[col_num++]->insert(stat.pzxid);
            res_columns[col_num++]->insert(
                list_task.path); /// This is the original path. In order to process the request, condition in WHERE should be triggered.

            ++row_count;

            if (list_task.path_type != ZkPathType::Exact && res.stat.numChildren > 0)
            {
                paths.insert_or_assign(key, ZkPathType::Recurse);
            }
        }
    }

    return Chunk(std::move(res_columns), row_count);
}

ReadFromSystemZooKeeper::ReadFromSystemZooKeeper(
    const Names & column_names_,
    const SelectQueryInfo & query_info_,
    const StorageSnapshotPtr & storage_snapshot_,
    const ContextPtr & context_,
    const Block & header,
    UInt64 max_block_size_)
    : SourceStepWithFilter(
        {.header = header},
        column_names_,
        query_info_,
        storage_snapshot_,
        context_)
    , storage_limits(query_info.storage_limits)
    , max_block_size(max_block_size_)
{
}

void ReadFromSystemZooKeeper::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    const auto & header = getOutputStream().header;
    auto source = std::make_shared<SystemZooKeeperSource>(std::move(paths), header, max_block_size, context);
    source->setStorageLimits(storage_limits);
    processors.emplace_back(source);
    pipeline.init(Pipe(std::move(source)));
}

}
