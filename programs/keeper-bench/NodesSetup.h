#pragma once

#include <Common/ZooKeeper/ZooKeeperImpl.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <Generator.h>

/// Consumes `Create` requests produced by the setup tree walk. Implementations
/// execute them against a Keeper connection (batched into multis) or an
/// in-process `KeeperStorage`.
using CreateRequestSink = std::function<void(std::shared_ptr<Coordination::ZooKeeperCreateRequest>)>;

/// Parses the `setup` config section describing a tree of znodes to create
/// before the benchmark starts (and to remove afterwards in network modes).
class NodesSetup
{
public:
    struct Node
    {
        StringGetter name;
        std::optional<StringGetter> data;
        std::optional<std::string> tag;
        std::vector<std::shared_ptr<Node>> children;
        size_t repeat_count = 0;

        std::shared_ptr<Node> clone() const;

        void createNodes(
            const CreateRequestSink & sink,
            const std::string & parent_path,
            const Coordination::ACLs & acls,
            TaggedPaths & tagged_paths_out,
            pcg64 & rng_) const;
        void dumpTree(int level = 0) const;
    };

    void initializeFromConfig(const Poco::Util::AbstractConfiguration & config);

    /// Remove leftovers from previous runs, then create the setup tree over a Keeper connection.
    void startup(Coordination::ZooKeeper & zookeeper);
    void cleanup(Coordination::ZooKeeper & zookeeper);

    /// Walk the setup tree, passing a `Create` request for every node to `sink`
    /// and collecting tagged paths.
    void createNodes(const CreateRequestSink & sink);

    bool hasNodes() const { return !root_nodes.empty(); }
    const TaggedPaths & getTaggedPaths() const { return tagged_paths; }

private:
    static std::shared_ptr<Node> parseNode(const std::string & key, const Poco::Util::AbstractConfiguration & config);

    std::vector<std::shared_ptr<Node>> root_nodes;
    Coordination::ACLs default_acls;
    TaggedPaths tagged_paths;
    bool use_remove_recursive = true;
    /// For generating random node names/data during setup.
    pcg64 rng{randomSeed()};
};
