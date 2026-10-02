#include <NodesSetup.h>

#include <fmt/ranges.h>
#include <deque>
#include <filesystem>
#include <iostream>
#include <ranges>

#include <Common/Exception.h>
#include <Common/ZooKeeper/KeeperException.h>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

constexpr size_t WRITE_BATCH_SIZE = 10000;
constexpr size_t READ_BATCH_SIZE = 10000;
/// How often RequestBatcher prints a progress line, in batches.
constexpr size_t REPORT_EVERY_BATCHES = 10;

Coordination::MultiResponse executeMulti(Coordination::ZooKeeper & zookeeper, const Coordination::Requests & batch)
{
    auto promise = std::make_shared<std::promise<Coordination::MultiResponse>>();
    auto future = promise->get_future();
    zookeeper.multi(batch, [promise](const Coordination::MultiResponse & response)
    {
        promise->set_value(response);
    });
    return future.get();
}

void flushMulti(Coordination::ZooKeeper & zookeeper, Coordination::Requests & batch)
{
    if (batch.empty())
        return;

    auto response = executeMulti(zookeeper, batch);
    if (response.error != Coordination::Error::ZOK)
        throw zkutil::KeeperException(response.error, "Multi request failed");

    batch.clear();
}

/// Accumulates requests and executes them in Multi requests of WRITE_BATCH_SIZE,
/// printing a progress line every REPORT_EVERY_BATCHES executed batches.
class RequestBatcher
{
public:
    /// `action` is a past-tense verb for progress messages: "<action> <count>[/<total>] nodes".
    /// Pass `total` of 0 if the total is not known in advance.
    RequestBatcher(Coordination::ZooKeeper & zookeeper_, std::string action_, size_t total_)
        : zookeeper(zookeeper_), action(std::move(action_)), total(total_)
    {
    }

    void add(Coordination::RequestPtr request)
    {
        batch.push_back(std::move(request));
        if (batch.size() >= WRITE_BATCH_SIZE)
            flush();
    }

    /// Execute the last incomplete batch and print the final count.
    void finish()
    {
        flush();
        if (executed != last_reported)
            report();
    }

private:
    void flush()
    {
        if (batch.empty())
            return;

        size_t batch_size = batch.size();
        flushMulti(zookeeper, batch);
        executed += batch_size;
        ++batches;
        if (batches % REPORT_EVERY_BATCHES == 0)
            report();
    }

    void report()
    {
        if (total != 0)
            std::cerr << action << " " << executed << "/" << total << " nodes" << std::endl;
        else
            std::cerr << action << " " << executed << " nodes" << std::endl;
        last_reported = executed;
    }

    Coordination::ZooKeeper & zookeeper;
    std::string action;
    size_t total = 0;

    Coordination::Requests batch;
    size_t executed = 0;
    size_t batches = 0;
    size_t last_reported = 0;
};

void removeRecursiveManual(Coordination::ZooKeeper & zookeeper, const std::string & path, RequestBatcher & remove_batcher)
{
    namespace fs = std::filesystem;

    auto promise = std::make_shared<std::promise<Coordination::Error>>();
    auto future = promise->get_future();

    std::vector<std::string> children;
    auto list_callback = [promise, &children](const Coordination::ListResponse & response)
    {
        children = response.names;
        promise->set_value(response.error);
    };
    zookeeper.list(path, Coordination::ListRequestType::ALL, list_callback, {}, false, false);
    auto error = future.get();
    if (error == Coordination::Error::ZNONODE)
        return;
    if (error != Coordination::Error::ZOK)
        throw zkutil::KeeperException(error, "Failed to list children of {}", path);

    for (const auto & child : children)
        removeRecursiveManual(zookeeper, fs::path(path) / child, remove_batcher);

    remove_batcher.add(zkutil::makeRemoveRequest(path, -1));
}

/// Like removeRecursiveManual, but suitable for huge subtrees (~100M nodes):
///  * List requests are batched into MultiRead requests instead of one round trip per node,
///  * nodes that their parent's listing showed to be childless are removed without listing.
/// Requires the MULTI_READ, FILTERED_LIST, and LIST_WITH_STAT_AND_DATA feature flags.
void removeRecursiveManualFast(Coordination::ZooKeeper & zookeeper, const std::string & root_path, RequestBatcher & remove_batcher)
{
    namespace fs = std::filesystem;

    /// Nodes that had children when their parent was listed (plus the root).
    /// The walk below appends parents before their children (BFS pre-order), so
    /// removing in reverse order removes children before parents. Nodes without
    /// children are removed right away instead.
    std::vector<std::string> listed;

    /// Nodes to list: path and the number of children reported by the parent's listing.
    std::deque<std::pair<std::string, size_t>> queue;
    queue.emplace_back(root_path, 1);

    while (!queue.empty())
    {
        /// Group List requests into one MultiRead, capping the expected total
        /// number of listed children to bound the response size.
        std::vector<std::string> batch_paths;
        size_t expected_children = 0;
        while (!queue.empty() && (batch_paths.empty() || expected_children + queue.front().second <= READ_BATCH_SIZE))
        {
            expected_children += queue.front().second;
            batch_paths.push_back(std::move(queue.front().first));
            queue.pop_front();
        }

        Coordination::Requests list_batch;
        list_batch.reserve(batch_paths.size());
        for (const auto & path : batch_paths)
            list_batch.push_back(zkutil::makeListRequest(path, Coordination::ListRequestType::ALL, /*with_stat=*/ true, /*with_data=*/ false));

        auto multi_response = executeMulti(zookeeper, list_batch);

        /// An error of a subrequest (e.g. ZNONODE, which we tolerate) is propagated to the
        /// error of the whole MultiRead. So check the subresponse errors first, and treat the
        /// multi-level error as fatal only if no subresponse explains it (e.g. a connection
        /// loss, where the subresponses are left blank and must not be trusted).
        chassert(multi_response.responses.size() == batch_paths.size());
        bool sub_error_found = false;
        for (size_t i = 0; i < batch_paths.size(); ++i)
        {
            auto sub_error = multi_response.responses.at(i)->error;
            if (sub_error == Coordination::Error::ZNONODE)
                sub_error_found = true;
            else if (sub_error != Coordination::Error::ZOK)
                throw zkutil::KeeperException(sub_error, "Failed to list children of {}", batch_paths[i]);
        }
        if (multi_response.error != Coordination::Error::ZOK && !sub_error_found)
            throw zkutil::KeeperException(multi_response.error, "MultiRead of {} List requests failed", list_batch.size());

        for (size_t i = 0; i < batch_paths.size(); ++i)
        {
            const auto & response = *multi_response.responses.at(i);
            /// The node was removed concurrently (e.g. an expired ephemeral); nothing to do.
            if (response.error == Coordination::Error::ZNONODE)
                continue;

            const auto & list_response = dynamic_cast<const Coordination::ListResponse &>(response);
            chassert(list_response.stats.size() == list_response.names.size());
            for (size_t j = 0; j < list_response.names.size(); ++j)
            {
                std::string child_path = fs::path(batch_paths[i]) / list_response.names[j];
                if (list_response.stats[j].numChildren == 0)
                    remove_batcher.add(zkutil::makeRemoveRequest(child_path, -1));
                else
                    queue.emplace_back(std::move(child_path), list_response.stats[j].numChildren);
            }

            listed.push_back(std::move(batch_paths[i]));
        }
    }

    for (auto it = listed.rbegin(); it != listed.rend(); ++it)
        remove_batcher.add(zkutil::makeRemoveRequest(*it, -1));
}

void removeRecursive(Coordination::ZooKeeper & zookeeper, const std::string & path, bool allow_native)
{
    if (allow_native && zookeeper.isFeatureEnabled(DB::KeeperFeatureFlag::REMOVE_RECURSIVE))
    {
        auto promise = std::make_shared<std::promise<Coordination::Error>>();
        auto future = promise->get_future();
        /// A native RemoveRecursive request is indivisible: the whole subtree is removed
        /// in one raft entry, blocking all other writes for its duration. Limit its size
        /// and fall back to manual removal (batched into many multis) for bigger subtrees.
        zookeeper.removeRecursive(path, /*remove_nodes_limit=*/ 1000000,
            [promise](const Coordination::RemoveRecursiveResponse & response)
            {
                promise->set_value(response.error);
            });
        auto error = future.get();
        if (error == Coordination::Error::ZNONODE || error == Coordination::Error::ZOK)
            return;
        /// ZNOTEMPTY means the subtree exceeded remove_nodes_limit (nothing was removed).
        if (error != Coordination::Error::ZNOTEMPTY)
            throw zkutil::KeeperException(error, "Failed to recursively remove {}", path);
        std::cerr << "Subtree at " << path << " is too large for one native RemoveRecursive request, removing manually" << std::endl;
    }

    RequestBatcher remove_batcher(zookeeper, "Removed", /*total=*/ 0);

    if (zookeeper.isFeatureEnabled(DB::KeeperFeatureFlag::MULTI_READ)
        && zookeeper.isFeatureEnabled(DB::KeeperFeatureFlag::FILTERED_LIST)
        && zookeeper.isFeatureEnabled(DB::KeeperFeatureFlag::LIST_WITH_STAT_AND_DATA))
        removeRecursiveManualFast(zookeeper, path, remove_batcher);
    else
        removeRecursiveManual(zookeeper, path, remove_batcher);

    remove_batcher.finish();
}

}

void NodesSetup::initializeFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    default_acls = getDefaultACLs();

    use_remove_recursive = config.getBool("use_remove_recursive", true);

    std::cerr << "---- Parsing setup ---- " << std::endl;
    static const std::string setup_key = "setup";
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(setup_key, keys);
    for (const auto & key : keys)
    {
        if (key.starts_with("node"))
        {
            const auto node = root_nodes.emplace_back(parseNode(setup_key + "." + key, config));

            std::cerr << "Tree to create:" << std::endl;
            node->dumpTree();
            std::cerr << std::endl;
        }
    }
    std::cerr << "---- Done parsing data setup ----\n" << std::endl;
}

std::shared_ptr<NodesSetup::Node> NodesSetup::parseNode(const std::string & key, const Poco::Util::AbstractConfiguration & config)
{
    auto node = std::make_shared<NodesSetup::Node>();
    node->name = StringGetter::fromConfig(key + ".name", config);

    if (config.has(key + ".data"))
        node->data = StringGetter::fromConfig(key + ".data", config);

    if (config.has(key + ".tag"))
    {
        node->tag = config.getString(key + ".tag");
        node->tag_set = getOrCreateTagSet(*node->tag);
        node->tag_set->is_setup_tag = true;
    }

    if (config.has(key + ".repeat"))
    {
        if (!node->name.isRandom())
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Repeating node creation for key {}, but name is not randomly generated", key);

        node->repeat_count = config.getUInt64(key + ".repeat");
        if (node->repeat_count == 0)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "repeat must be >= 1 for key {}", key);
    }

    Poco::Util::AbstractConfiguration::Keys node_keys;
    config.keys(key, node_keys);

    for (const auto & node_key : node_keys)
    {
        if (!node_key.starts_with("node"))
            continue;

        node->children.push_back(parseNode(key + "." + node_key, config));
    }

    return node;
}

size_t NodesSetup::Node::countNodes() const
{
    size_t count = 1;
    for (const auto & child : children)
        count += child->repeat_count * child->countNodes();
    return count;
}

void NodesSetup::Node::dumpTree(int level) const
{
    std::string data_string
        = data.has_value() ? fmt::format("{}", data->description()) : "no data";

    std::string repeat_count_string = repeat_count != 1 ? fmt::format(", repeated {} times", repeat_count) : "";

    std::string tag_string = tag.has_value() ? fmt::format(", tag: \"{}\"", *tag) : "";

    std::cerr << fmt::format("{}name: {}, data: {}{}{}", std::string(level, '\t'), name.description(), data_string, repeat_count_string, tag_string) << std::endl;

    for (const auto & child : children)
        child->dumpTree(level + 1);
}

void NodesSetup::Node::createAt(const std::string & path, const CreateRequestSink & sink, const Coordination::ACLs & acls, pcg64 & rng_) const
{
    auto request = std::make_shared<Coordination::ZooKeeperCreateRequest>();
    request->path = path;
    request->data = data ? data->getString(rng_) : "";
    request->acls = acls;
    sink(std::move(request));

    if (tag_set && tag_set->used_as_input)
        tag_set->populate(path);

    for (const auto & child : children)
        for (size_t i = 0; i < child->repeat_count; ++i)
            child->createAt(std::filesystem::path(path) / child->name.getString(rng_), sink, acls, rng_);
}

PathSetPtr NodesSetup::getOrCreateTagSet(const std::string & tag)
{
    auto & set = tag_sets[tag];
    if (!set)
    {
        set = std::make_shared<PathSet>();
        set->name = fmt::format("tag \"{}\"", tag);
    }
    return set;
}

PathSetPtr NodesSetup::getOrCreateChildrenOfSet(const std::string & parent_path)
{
    auto & set = children_of_sets[parent_path];
    if (!set)
    {
        set = std::make_shared<PathSet>();
        set->name = fmt::format("children of {}", parent_path);
        set->is_children_of = true;
        set->children_of_parent = parent_path;
    }
    return set;
}

PathSetPtr NodesSetup::createLiteralSet(std::vector<std::string> paths)
{
    auto set = std::make_shared<PathSet>();
    set->name = fmt::format("paths [{}]", fmt::join(paths, ", "));
    set->is_literal = true;
    for (auto & path : paths)
        set->populate(std::move(path));
    literal_sets.push_back(set);
    return set;
}

PathSetPtr NodesSetup::createAnonymousSet(std::string display_name)
{
    auto set = std::make_shared<PathSet>();
    set->name = std::move(display_name);
    anonymous_sets.push_back(set);
    return set;
}

void NodesSetup::registerTagChildrenOfConflict(std::string parent_path, std::string tag_name)
{
    tag_children_of_conflicts.emplace_back(std::move(parent_path), std::move(tag_name));
}

void NodesSetup::finalizePathSets(size_t num_threads)
{
    auto finalize = [&](const PathSetPtr & set)
    {
        /// A set some generator writes to changes while the benchmark runs.
        set->is_dynamic = set->used_as_output;
        set->finalize(num_threads);
    };

    for (const auto & [_, set] : tag_sets)
        finalize(set);
    for (const auto & [_, set] : children_of_sets)
        finalize(set);
    for (const auto & set : literal_sets)
        finalize(set);
    for (const auto & set : anonymous_sets)
        finalize(set);
}

void NodesSetup::resolveChildrenOf(const ListChildrenFn & list_children)
{
    for (const auto & [parent_path, set] : children_of_sets)
    {
        if (!set->used_as_input)
            continue;

        for (const auto & child : list_children(parent_path))
            set->populate(std::filesystem::path(parent_path) / child);
    }
}

void NodesSetup::validatePathSets()
{
    for (const auto & [parent_path, tag_name] : tag_children_of_conflicts)
    {
        auto it = children_of_sets.find(parent_path);
        if (it != children_of_sets.end() && it->second->used_as_input)
            throw DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS,
                "A create generator with parent {} outputs to tag \"{}\", but another generator reads `children_of` {}: "
                "the created nodes would be tracked in one set and read from another. Use the tag consistently "
                "(reference `tagged: {}` instead of `children_of`)",
                parent_path, tag_name, parent_path, tag_name);
    }

    bool printed_header = false;
    auto validate = [&](const PathSetPtr & set)
    {
        bool defined = set->is_setup_tag || set->is_children_of || set->is_literal || set->used_as_output;
        if (set->used_as_input && !defined)
            throw DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS,
                "{} is used by a request generator, but no setup node has this tag "
                "and no create generator outputs to it. Available tags: {}",
                set->name,
                tag_sets.empty() ? "(none)" : fmt::to_string(fmt::join(tag_sets | std::views::keys, ", ")));

        if (set->used_as_input && !set->is_dynamic && set->totalSize() == 0)
            throw DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS,
                "{} is empty: check that the `children_of` target has children or that setup nodes carry the tag",
                set->name);

        if (set->keep_count)
        {
            size_t num_shards = set->shards.size();
            size_t total_target = *set->keep_count == 0 ? set->totalSize() : *set->keep_count;
            set->target_count_per_shard = std::max<size_t>(1, total_target / num_shards);
        }

        if (set->used_as_input)
        {
            if (!printed_header)
            {
                std::cerr << "Path sets:" << std::endl;
                printed_header = true;
            }
            std::string target_string
                = set->target_count_per_shard != 0 ? fmt::format(", target {} per thread", set->target_count_per_shard) : "";
            std::cerr << fmt::format("  {}: {} paths{}{}", set->name, set->totalSize(), set->is_dynamic ? " (dynamic)" : "", target_string)
                      << std::endl;
        }
    };

    for (const auto & [_, set] : tag_sets)
        validate(set);
    for (const auto & [_, set] : children_of_sets)
        validate(set);
    for (const auto & set : literal_sets)
        validate(set);
    for (const auto & set : anonymous_sets)
        validate(set);
}

std::string NodesSetup::describeDynamicPathSets() const
{
    std::string result;
    auto append = [&](const PathSetPtr & set)
    {
        if (!set->is_dynamic || !set->used_as_input)
            return;
        if (!result.empty())
            result += ", ";
        result += fmt::format("{}: {}", set->name, set->totalSize());
        if (set->target_count_per_shard != 0)
            result += fmt::format(" (target {})", set->target_count_per_shard * set->shards.size());
    };

    for (const auto & [_, set] : tag_sets)
        append(set);
    for (const auto & [_, set] : children_of_sets)
        append(set);
    for (const auto & set : literal_sets)
        append(set);
    for (const auto & set : anonymous_sets)
        append(set);

    return result;
}

const std::vector<std::string> & NodesSetup::prepareRootPaths()
{
    if (created_root_paths.empty())
    {
        for (const auto & node : root_nodes)
            for (size_t i = 0; i < node->repeat_count; ++i)
                created_root_paths.push_back(std::filesystem::path("/") / node->name.getString(rng));
    }
    return created_root_paths;
}

void NodesSetup::createNodes(const CreateRequestSink & sink)
{
    const auto & root_paths = prepareRootPaths();

    size_t path_idx = 0;
    for (const auto & node : root_nodes)
        for (size_t i = 0; i < node->repeat_count; ++i)
            node->createAt(root_paths.at(path_idx++), sink, default_acls, rng);
}

size_t NodesSetup::countTotalNodes() const
{
    size_t total = 0;
    for (const auto & node : root_nodes)
        total += node->repeat_count * node->countNodes();
    return total;
}

void NodesSetup::startup(Coordination::ZooKeeper & zookeeper)
{
    if (root_nodes.empty())
        return;

    std::cerr << "---- Creating test data ----" << std::endl;

    /// Remove leftovers from previous runs.
    for (const auto & root_path : prepareRootPaths())
    {
        std::cerr << "Cleaning up " << root_path << std::endl;
        removeRecursive(zookeeper, root_path, use_remove_recursive);
    }

    size_t total_nodes = countTotalNodes();
    std::cerr << "Creating " << total_nodes << " nodes" << std::endl;

    RequestBatcher create_batcher(zookeeper, "Created", total_nodes);
    createNodes([&](std::shared_ptr<Coordination::ZooKeeperCreateRequest> request)
    {
        create_batcher.add(std::move(request));
    });
    create_batcher.finish();

    std::cerr << "---- Created test data ----\n" << std::endl;
}

void NodesSetup::cleanup(Coordination::ZooKeeper & zookeeper)
{
    if (created_root_paths.empty())
        return;

    std::cerr << "---- Cleaning up test data ----" << std::endl;
    for (const auto & root_path : created_root_paths)
    {
        std::cerr << "Cleaning up " << root_path << std::endl;
        removeRecursive(zookeeper, root_path, use_remove_recursive);
    }
}
