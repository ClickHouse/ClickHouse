#include <NodesSetup.h>

#include <fmt/ranges.h>
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

void flushMulti(Coordination::ZooKeeper & zookeeper, Coordination::Requests & batch)
{
    if (batch.empty())
        return;

    auto promise = std::make_shared<std::promise<Coordination::MultiResponse>>();
    auto future = promise->get_future();
    zookeeper.multi(batch, [promise](const Coordination::MultiResponse & response)
    {
        promise->set_value(response);
    });
    auto response = future.get();
    if (response.error != Coordination::Error::ZOK)
        throw zkutil::KeeperException(response.error, "Multi request failed");

    batch.clear();
}

void addToBatchAndMaybeFlush(Coordination::ZooKeeper & zookeeper, Coordination::Requests & batch, Coordination::RequestPtr request)
{
    batch.push_back(std::move(request));
    if (batch.size() >= 10000)
    {
        flushMulti(zookeeper, batch);
        batch.clear();
    }
}

void removeRecursiveManual(Coordination::ZooKeeper & zookeeper, const std::string & path, Coordination::Requests & batch)
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
        removeRecursiveManual(zookeeper, fs::path(path) / child, batch);

    addToBatchAndMaybeFlush(zookeeper, batch, zkutil::makeRemoveRequest(path, -1));
}

void removeRecursive(Coordination::ZooKeeper & zookeeper, const std::string & path, bool allow_native)
{
    if (allow_native && zookeeper.isFeatureEnabled(DB::KeeperFeatureFlag::REMOVE_RECURSIVE))
    {
        auto promise = std::make_shared<std::promise<Coordination::Error>>();
        auto future = promise->get_future();
        zookeeper.removeRecursive(path, /*remove_nodes_limit=*/ 100000000,
            [promise](const Coordination::RemoveRecursiveResponse & response)
            {
                promise->set_value(response.error);
            });
        auto error = future.get();
        if (error == Coordination::Error::ZNONODE)
            return;
        if (error != Coordination::Error::ZOK)
            throw zkutil::KeeperException(error, "Failed to recursively remove {}", path);
        return;
    }

    Coordination::Requests batch;
    removeRecursiveManual(zookeeper, path, batch);
    flushMulti(zookeeper, batch);
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

    Coordination::Requests batch;
    createNodes([&](std::shared_ptr<Coordination::ZooKeeperCreateRequest> request)
    {
        addToBatchAndMaybeFlush(zookeeper, batch, std::move(request));
    });
    flushMulti(zookeeper, batch);

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
