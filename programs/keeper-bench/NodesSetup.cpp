#include <NodesSetup.h>

#include <filesystem>
#include <iostream>

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
            auto node_key = setup_key + "." + key;
            auto parsed_root_node = parseNode(node_key, config);
            const auto node = root_nodes.emplace_back(parsed_root_node);

            if (config.has(node_key + ".repeat"))
            {
                if (!node->name.isRandom())
                    throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Repeating node creation for key {}, but name is not randomly generated", node_key);

                auto repeat_count = config.getUInt64(node_key + ".repeat");
                node->repeat_count = repeat_count;
                for (size_t i = 1; i < repeat_count; ++i)
                    root_nodes.emplace_back(node->clone());
            }

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
        node->tag = config.getString(key + ".tag");

    Poco::Util::AbstractConfiguration::Keys node_keys;
    config.keys(key, node_keys);

    for (const auto & node_key : node_keys)
    {
        if (!node_key.starts_with("node"))
            continue;

        const auto node_key_string = key + "." + node_key;
        auto child_node = parseNode(node_key_string, config);
        node->children.push_back(child_node);

        if (config.has(node_key_string + ".repeat"))
        {
            if (!child_node->name.isRandom())
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Repeating node creation for key {}, but name is not randomly generated", node_key_string);

            auto repeat_count = config.getUInt64(node_key_string + ".repeat");
            child_node->repeat_count = repeat_count;
            for (size_t i = 1; i < repeat_count; ++i)
                node->children.push_back(child_node);
        }
    }

    return node;
}

void NodesSetup::Node::dumpTree(int level) const
{
    std::string data_string
        = data.has_value() ? fmt::format("{}", data->description()) : "no data";

    std::string repeat_count_string = repeat_count != 0 ? fmt::format(", repeated {} times", repeat_count) : "";

    std::string tag_string = tag.has_value() ? fmt::format(", tag: \"{}\"", *tag) : "";

    std::cerr << fmt::format("{}name: {}, data: {}{}{}", std::string(level, '\t'), name.description(), data_string, repeat_count_string, tag_string) << std::endl;

    for (auto it = children.begin(); it != children.end();)
    {
        const auto & child = *it;
        child->dumpTree(level + 1);
        std::advance(it, child->repeat_count != 0 ? child->repeat_count : 1);
    }
}

std::shared_ptr<NodesSetup::Node> NodesSetup::Node::clone() const
{
    auto new_node = std::make_shared<Node>();
    new_node->name = name;
    new_node->data = data;
    new_node->tag = tag;
    new_node->repeat_count = repeat_count;

    // don't do deep copy of children because we will do clone only for root nodes
    new_node->children = children;

    return new_node;
}

void NodesSetup::Node::createNodes(
    const CreateRequestSink & sink,
    const std::string & parent_path,
    const Coordination::ACLs & acls,
    TaggedPaths & tagged_paths_out,
    pcg64 & rng_) const
{
    auto path = std::filesystem::path(parent_path) / name.getString(rng_);

    auto request = std::make_shared<Coordination::ZooKeeperCreateRequest>();
    request->path = path;
    request->data = data ? data->getString(rng_) : "";
    request->acls = acls;
    sink(std::move(request));

    if (tag.has_value())
        tagged_paths_out[*tag].push_back(path);

    for (const auto & child : children)
        child->createNodes(sink, path, acls, tagged_paths_out, rng_);
}

void NodesSetup::createNodes(const CreateRequestSink & sink)
{
    for (const auto & node : root_nodes)
    {
        /// Pin the root name (which may be randomly generated) so later getString
        /// calls (e.g. in cleanup) return the same path.
        auto node_name = node->name.getString(rng);
        node->name.setString(node_name);

        node->createNodes(sink, "/", default_acls, tagged_paths, rng);
    }

    if (!tagged_paths.empty())
    {
        std::cerr << "Tagged paths:" << std::endl;
        for (const auto & [tag_name, paths] : tagged_paths)
            std::cerr << fmt::format("  \"{}\": {} paths", tag_name, paths.size()) << std::endl;
    }
}

void NodesSetup::startup(Coordination::ZooKeeper & zookeeper)
{
    if (root_nodes.empty())
        return;

    std::cerr << "---- Creating test data ----" << std::endl;

    /// Pin root names and remove leftovers from previous runs.
    for (const auto & node : root_nodes)
    {
        auto node_name = node->name.getString(rng);
        node->name.setString(node_name);

        std::string root_path = std::filesystem::path("/") / node_name;
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
    if (root_nodes.empty())
        return;

    std::cerr << "---- Cleaning up test data ----" << std::endl;
    for (const auto & node : root_nodes)
    {
        auto node_name = node->name.getString(rng);
        std::string root_path = std::filesystem::path("/") / node_name;
        std::cerr << "Cleaning up " << root_path << std::endl;
        removeRecursive(zookeeper, root_path, use_remove_recursive);
    }
}
