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
#include <Common/typeid_cast.h>
#include <Columns/ColumnSet.h>
#include <Columns/ColumnConst.h>
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
#include <climits>


namespace DB
{

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
        for (auto [_, child] : children)
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

    void consume(Chunk chunk) override
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
enum class ZkPathType
{
    Exact,   /// Fetch all nodes under this path
    Prefix,  /// Fetch all nodes starting with this prefix, recursively (multiple paths may match prefix)
    Recurse, /// Fatch all nodes under this path, recursively
};

/// List of paths to be feched from zookeeper
using Paths = std::deque<std::pair<String, ZkPathType>>;

class ReadFromSystemZooKeeper final : public SourceStepWithFilter
{
public:
    ReadFromSystemZooKeeper(const Block & header, SelectQueryInfo & query_info_, ContextPtr context_);

    String getName() const override { return "ReadFromSystemZooKeeper"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void applyFilters() override;

private:
    void fillData(MutableColumns & res_columns);

    std::shared_ptr<const StorageLimitsList> storage_limits;
    ContextPtr context;
    Paths paths;
};

StorageSystemZooKeeper::StorageSystemZooKeeper(const StorageID & table_id_)
        : IStorage(table_id_)
{
        StorageInMemoryMetadata storage_metadata;
        ColumnsDescription desc;
        auto columns = getNamesAndTypes();
        for (const auto & col : columns)
        {
            ColumnDescription col_desc(col.name, col.type);
            /// We only allow column `name`, `path`, `value` to insert.
            if (col.name != "name" && col.name != "path" && col.name != "value")
                col_desc.default_desc.kind = ColumnDefaultKind::Materialized;
            desc.add(col_desc);
        }
        storage_metadata.setColumns(desc);
        setInMemoryMetadata(storage_metadata);
}

bool StorageSystemZooKeeper::mayBenefitFromIndexForIn(const ASTPtr & node, ContextPtr, const StorageMetadataPtr &) const
{
    return node->as<ASTIdentifier>() && node->getColumnName() == "path";
}

void StorageSystemZooKeeper::read(
    QueryPlan & query_plan,
    const Names & /*column_names*/,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    size_t /*num_streams*/)
{
    auto header = storage_snapshot->metadata->getSampleBlockWithVirtuals(getVirtuals());
    auto read_step = std::make_unique<ReadFromSystemZooKeeper>(header, query_info, context);
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

NamesAndTypesList StorageSystemZooKeeper::getNamesAndTypes()
{
    return {
        { "name",           std::make_shared<DataTypeString>() },
        { "value",          std::make_shared<DataTypeString>() },
        { "czxid",          std::make_shared<DataTypeInt64>() },
        { "mzxid",          std::make_shared<DataTypeInt64>() },
        { "ctime",          std::make_shared<DataTypeDateTime>() },
        { "mtime",          std::make_shared<DataTypeDateTime>() },
        { "version",        std::make_shared<DataTypeInt32>() },
        { "cversion",       std::make_shared<DataTypeInt32>() },
        { "aversion",       std::make_shared<DataTypeInt32>() },
        { "ephemeralOwner", std::make_shared<DataTypeInt64>() },
        { "dataLength",     std::make_shared<DataTypeInt32>() },
        { "numChildren",    std::make_shared<DataTypeInt32>() },
        { "pzxid",          std::make_shared<DataTypeInt64>() },
        { "path",           std::make_shared<DataTypeString>() },
    };
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
            res.emplace_back(values->getDataAt(row).toString(), ZkPathType::Exact);
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

        res.emplace_back(value->column->getDataAt(0).toString(), ZkPathType::Exact);
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
        String prefix; // pattern prefix before the first metasymbol occurrence
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

        res.emplace_back(prefix, has_metasymbol ? ZkPathType::Prefix : ZkPathType::Exact);
    }
}


/** Retrieve from the query a condition of the form `path = 'path'`, from conjunctions in the WHERE clause.
  */
static Paths extractPath(const ActionsDAG::NodeRawConstPtrs & filter_nodes, ContextPtr context, bool allow_unrestricted)
{
    Paths res;
    for (const auto * node : filter_nodes)
        extractPathImpl(*node, res, context, allow_unrestricted);

    if (filter_nodes.empty() && allow_unrestricted)
        res.emplace_back("/", ZkPathType::Recurse);

    return res;
}


void ReadFromSystemZooKeeper::applyFilters()
{
    paths = extractPath(getFilterNodes().nodes, context, context->getSettingsRef().allow_unrestricted_reads_from_keeper);
}

void ReadFromSystemZooKeeper::fillData(MutableColumns & res_columns)
{
    zkutil::ZooKeeperPtr zookeeper = context->getZooKeeper();

    if (paths.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "SELECT from system.zookeeper table must contain condition like path = 'path' "
                        "or path IN ('path1','path2'...) or path IN (subquery) "
                        "in WHERE clause unless `set allow_unrestricted_reads_from_keeper = 'true'`.");

    const Int64 max_inflight_requests = std::max<Int64>(1, context->getSettingsRef().max_download_threads.value);

    struct ListTask
    {
        String path;
        ZkPathType path_type;
        String prefix;
        String path_corrected;
        String path_part;
    };
    std::vector<ListTask> list_tasks;
    std::unordered_set<String> added;
    while (!paths.empty())
    {
        list_tasks.clear();
        std::vector<String> paths_to_list;
        while (!paths.empty() && static_cast<Int64>(list_tasks.size()) < max_inflight_requests)
        {
            auto [path, path_type] = std::move(paths.front());
            paths.pop_front();

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
        auto list_responses = zookeeper->tryGetChildren(paths_to_list);

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
            context->getProcessListElement()->checkTimeLimit();

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

        auto get_responses = zookeeper->tryGet(paths_to_get);

        for (size_t i = 0, size = get_tasks.size(); i < size; ++i)
        {
            auto & res = get_responses[i];
            if (res.error == Coordination::Error::ZNONODE)
                continue; /// Node was deleted meanwhile.

            auto & get_task = get_tasks[i];
            auto & list_task = list_tasks[get_task.list_task_idx];
            context->getProcessListElement()->checkTimeLimit();

            // Deduplication
            String key = list_task.path_part + '/' + get_task.node;
            if (auto [it, inserted] = added.emplace(key); !inserted)
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

            if (list_task.path_type != ZkPathType::Exact && res.stat.numChildren > 0)
            {
                paths.emplace_back(key, ZkPathType::Recurse);
            }
        }
    }
}

ReadFromSystemZooKeeper::ReadFromSystemZooKeeper(const Block & header, SelectQueryInfo & query_info, ContextPtr context_)
    : SourceStepWithFilter({.header = header})
    , storage_limits(query_info.storage_limits)
    , context(std::move(context_))
{
}

void ReadFromSystemZooKeeper::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    const auto & header = getOutputStream().header;
    MutableColumns res_columns = header.cloneEmptyColumns();
    fillData(res_columns);

    UInt64 num_rows = res_columns.at(0)->size();
    Chunk chunk(std::move(res_columns), num_rows);

    auto source = std::make_shared<SourceFromSingleChunk>(header, std::move(chunk));
    source->setStorageLimits(storage_limits);
    processors.emplace_back(source);
    pipeline.init(Pipe(std::move(source)));
}

}
