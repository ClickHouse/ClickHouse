#include <Storages/MergeTree/IvfTreeStore.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <QueryPipeline/BlockIO.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Common/CurrentThread.h>
#include <Common/QueryScope.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>

#include <filesystem>
#include <map>
#include <mutex>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

namespace IvfTreeStore
{

namespace
{

constexpr std::string_view trees_table = "system.vector_similarity_index_trees";

struct Store
{
    std::mutex mutex;
    bool table_created = false;
    /// Of (table, index, version).
    std::map<std::tuple<String, String, UInt64>, std::shared_ptr<const IvfTree>> trees;
    /// Of (table, index): the latest version, 0 for none known yet.
    std::map<std::pair<String, String>, UInt64> latest_versions;
};

Store & store()
{
    static Store instance;
    return instance;
}

ContextMutablePtr queryContext()
{
    auto context = Context::createCopy(Context::getGlobalContextInstance());
    context->makeQueryContext();
    context->setCurrentQueryId({});
    return context;
}

/// Runs an internal query whose pipeline takes and gives nothing, or gives the blocks to `on_block`.
void run(const String & query, const std::function<void(const Block &)> & on_block = {})
{
    auto context = queryContext();
    QueryScope query_scope;
    if (!CurrentThread::getGroup())
        query_scope = QueryScope::create(context);

    auto io = executeQuery(query, context, QueryFlags{.internal = true}).second;
    if (io.pipeline.pulling())
    {
        PullingPipelineExecutor executor(io.pipeline);
        Block block;
        while (executor.pull(block))
            if (on_block && !block.empty())
                on_block(block);
    }
    else if (io.pipeline.completed())
    {
        CompletedPipelineExecutor executor(io.pipeline);
        executor.execute();
    }
    io.onFinish();
}

void createTableUnderLock(Store & s)
{
    if (s.table_created)
        return;
    run(fmt::format(
        "CREATE TABLE IF NOT EXISTS {} (table_path String, index_name String, version UInt64, level UInt8, node UInt32, child_end UInt32, "
        "centroid Array(Float32)) ENGINE = MergeTree ORDER BY (table_path, index_name, version, level, node)",
        trees_table));
    s.table_created = true;
}

/// The nodes of a version of a tree, by levels; 0 for the latest version. Nothing if there is no such tree.
std::optional<VersionedTree> loadUnderLock(Store & s, const String & table, const String & index, size_t dimensions, UInt64 version)
{
    createTableUnderLock(s);
    const String key = fmt::format("table_path = {} AND index_name = {}", quoteString(table), quoteString(index));
    const String version_condition = version
        ? fmt::format("version = {}", version)
        : fmt::format("version = (SELECT max(version) FROM {} WHERE {})", trees_table, key);

    UInt64 found_version = 0;
    std::vector<std::vector<float>> centroids;
    std::vector<std::vector<UInt32>> child_offsets;
    run(fmt::format("SELECT version, level, node, child_end, centroid FROM {} WHERE {} AND {} ORDER BY level, node",
            trees_table, key, version_condition),
        [&](const Block & block)
        {
            const auto & versions = typeid_cast<const ColumnUInt64 &>(*block.getByPosition(0).column).getData();
            const auto & levels = typeid_cast<const ColumnUInt8 &>(*block.getByPosition(1).column).getData();
            const auto & nodes = typeid_cast<const ColumnUInt32 &>(*block.getByPosition(2).column).getData();
            const auto & child_ends = typeid_cast<const ColumnUInt32 &>(*block.getByPosition(3).column).getData();
            const auto & vectors = typeid_cast<const ColumnArray &>(*block.getByPosition(4).column);
            const auto & values = typeid_cast<const ColumnFloat32 &>(vectors.getData()).getData();
            const auto & offsets = vectors.getOffsets();
            for (size_t row = 0; row < block.rows(); ++row)
            {
                found_version = versions[row];
                const size_t level = levels[row];
                if (level >= centroids.size())
                {
                    if (level != centroids.size())
                        throw Exception(ErrorCodes::INCORRECT_DATA, "The IVF tree of {} {} lacks level {}", table, index, centroids.size());
                    centroids.emplace_back();
                    child_offsets.emplace_back(1, 0);
                }
                if (nodes[row] * dimensions != centroids[level].size() || offsets[row] - offsets[row - 1] != dimensions)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "The IVF tree of {} {} has an invalid node {} of level {}", table, index, nodes[row], level);
                centroids[level].insert(centroids[level].end(), values.begin() + offsets[row - 1], values.begin() + offsets[row]);
                child_offsets[level].push_back(child_ends[row]);
            }
        });
    if (centroids.empty())
        return std::nullopt;
    child_offsets.back().clear();

    auto tree = std::make_shared<const IvfTree>(IvfTree::fromLevels(dimensions, std::move(centroids), std::move(child_offsets)));
    s.trees[{table, index, found_version}] = tree;
    UInt64 & latest_version = s.latest_versions[{table, index}];
    latest_version = std::max(latest_version, found_version);
    return VersionedTree{found_version, std::move(tree)};
}

void storeUnderLock(Store & s, const String & table, const String & index, UInt64 version, const IvfTree & tree, size_t dimensions)
{
    createTableUnderLock(s);
    auto table_column = ColumnString::create();
    auto index_column = ColumnString::create();
    auto version_column = ColumnUInt64::create();
    auto level_column = ColumnUInt8::create();
    auto node_column = ColumnUInt32::create();
    auto child_end_column = ColumnUInt32::create();
    auto centroid_column = ColumnArray::create(ColumnFloat32::create());
    auto & centroid_values = typeid_cast<ColumnFloat32 &>(centroid_column->getData()).getData();
    auto & centroid_offsets = centroid_column->getOffsets();
    for (size_t l = 0; l < tree.depth(); ++l)
    {
        const auto & centroids = tree.centroidsOf(l);
        const auto & offsets = tree.childOffsetsOf(l);
        for (size_t node = 0; node < centroids.size() / dimensions; ++node)
        {
            table_column->insertData(table.data(), table.size());
            index_column->insertData(index.data(), index.size());
            version_column->insertValue(version);
            level_column->insertValue(static_cast<UInt8>(l));
            node_column->insertValue(static_cast<UInt32>(node));
            child_end_column->insertValue(offsets.empty() ? 0 : offsets[node + 1]);
            centroid_values.insert(centroids.begin() + node * dimensions, centroids.begin() + (node + 1) * dimensions);
            centroid_offsets.push_back(centroid_values.size());
        }
    }
    Block block{
        {std::move(table_column), std::make_shared<DataTypeString>(), "table_path"},
        {std::move(index_column), std::make_shared<DataTypeString>(), "index_name"},
        {std::move(version_column), std::make_shared<DataTypeUInt64>(), "version"},
        {std::move(level_column), std::make_shared<DataTypeUInt8>(), "level"},
        {std::move(node_column), std::make_shared<DataTypeUInt32>(), "node"},
        {std::move(child_end_column), std::make_shared<DataTypeUInt32>(), "child_end"},
        {std::move(centroid_column), std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>()), "centroid"},
    };

    auto context = queryContext();
    QueryScope query_scope;
    if (!CurrentThread::getGroup())
        query_scope = QueryScope::create(context);
    auto io = executeQuery(fmt::format("INSERT INTO {} (table_path, index_name, version, level, node, child_end, centroid) VALUES", trees_table),
        context, QueryFlags{.internal = true}).second;
    PushingPipelineExecutor executor(io.pipeline);
    executor.start();
    executor.push(block);
    executor.finish();
    io.onFinish();
}

}

String tableOf(const IDataPartStorage & part_storage)
{
    fs::path path = fs::path(part_storage.getRelativePath()).lexically_normal();
    if (!path.has_filename())
        path = path.parent_path();
    /// The part, then the directories of parts being moved or detached.
    path = path.parent_path();
    while (path.filename() == "moving" || path.filename() == "detached")
        path = path.parent_path();
    return path.string();
}

std::optional<VersionedTree> latest(const String & table, const String & index, size_t dimensions)
{
    auto & s = store();
    std::lock_guard lock(s.mutex);
    if (auto it = s.latest_versions.find({table, index}); it != s.latest_versions.end() && it->second)
        return VersionedTree{it->second, s.trees.at({table, index, it->second})};
    return loadUnderLock(s, table, index, dimensions, 0);
}

VersionedTree latestOrTrain(const String & table, const String & index, size_t dimensions, const std::function<IvfTree()> & train)
{
    auto & s = store();
    std::lock_guard lock(s.mutex);
    if (auto it = s.latest_versions.find({table, index}); it != s.latest_versions.end() && it->second)
        return VersionedTree{it->second, s.trees.at({table, index, it->second})};
    if (auto stored = loadUnderLock(s, table, index, dimensions, 0))
        return *stored;

    auto tree = std::make_shared<const IvfTree>(train());
    storeUnderLock(s, table, index, 1, *tree, dimensions);
    s.trees[{table, index, 1}] = tree;
    s.latest_versions[{table, index}] = 1;
    return VersionedTree{1, std::move(tree)};
}

std::shared_ptr<const IvfTree> get(const String & table, const String & index, size_t dimensions, UInt64 version)
{
    auto & s = store();
    std::lock_guard lock(s.mutex);
    if (auto it = s.trees.find({table, index, version}); it != s.trees.end())
        return it->second;
    auto stored = loadUnderLock(s, table, index, dimensions, version);
    if (!stored)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Version {} of the IVF tree of the index {} of {} is not in {}", version, index, table, trees_table);
    return stored->tree;
}

}

}
