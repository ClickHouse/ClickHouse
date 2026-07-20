#pragma once

#include <Common/ZooKeeper/ZooKeeperImpl.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <Generator.h>
#include <PathSet.h>

/// Consumes `Create` requests produced by the setup tree walk. Implementations
/// execute them against a Keeper connection (batched into multis) or an
/// in-process `KeeperStorage`.
using CreateRequestSink = std::function<void(std::shared_ptr<Coordination::ZooKeeperCreateRequest>)>;

/// Returns child names of a znode by path. Used to populate `children_of` path
/// sets. The network runners query a running Keeper; the storage-only runner
/// supplies a callable that reads from an in-process `KeeperStorage`.
using ListChildrenFn = std::function<std::vector<std::string>(const std::string &)>;

/// Parses the `setup` config section describing a tree of znodes to create
/// before the benchmark starts (and to remove afterwards in network modes).
/// Also owns the registry of `PathSet`s that connects the setup tree to the
/// request generators.
class NodesSetup
{
public:
    struct Node
    {
        StringGetter name;
        std::optional<StringGetter> data;
        std::optional<std::string> tag;
        /// Set collecting the paths of this node's instances (if `tag` is set).
        PathSetPtr tag_set;
        std::vector<std::shared_ptr<Node>> children;
        /// How many instances of this node to create (they differ by the random name).
        size_t repeat_count = 1;

        /// Create one instance of this node at `path`, then its children (with
        /// `repeat_count`s expanded), recursively.
        void createAt(const std::string & path, const CreateRequestSink & sink, const Coordination::ACLs & acls, pcg64 & rng_) const;
        void dumpTree(int level = 0) const;
    };

    void initializeFromConfig(const Poco::Util::AbstractConfiguration & config);

    /// --- PathSet registry ---

    /// Look up or register a path set. The `getOrCreate` forms may be called both
    /// while parsing `setup` (tags) and while parsing generator configs.
    PathSetPtr getOrCreateTagSet(const std::string & tag);
    PathSetPtr getOrCreateChildrenOfSet(const std::string & parent_path);
    PathSetPtr createLiteralSet(std::vector<std::string> paths);
    /// A set not addressable by any config reference, e.g. tracking the nodes
    /// created by one create generator for its own `remove_factor`.
    PathSetPtr createAnonymousSet(std::string display_name);

    /// Records that a create generator with fixed parent `parent_path` outputs to
    /// an explicit tag. If some other generator reads `children_of` of the same
    /// parent, `validatePathSets` reports an error (the two would silently track
    /// the same nodes in different sets).
    void registerTagChildrenOfConflict(std::string parent_path, std::string tag_name);

    /// Allocate PathSet shards. Must be called after all generators are parsed
    /// (so `used_as_*` and `is_dynamic` flags are final) and before `startup`.
    void finalizePathSets(size_t num_threads);

    /// Populate `children_of` sets by listing each referenced parent. Called after
    /// the setup tree is created.
    void resolveChildrenOf(const ListChildrenFn & list_children);

    /// Check set usage consistency (an input set must be defined by something) and
    /// that static input sets are not empty; compute `keep_count` targets from the
    /// populated sizes; print a summary of the populated sets.
    void validatePathSets();

    /// One-line summary of the dynamic sets' current sizes (and targets), for
    /// periodic reports. Empty if there are no dynamic sets.
    std::string describeDynamicPathSets() const;

    /// --- Node creation ---

    /// Remove leftovers from previous runs, then create the setup tree over a Keeper connection.
    void startup(Coordination::ZooKeeper & zookeeper);
    void cleanup(Coordination::ZooKeeper & zookeeper);

    /// Walk the setup tree, passing a `Create` request for every node to `sink`
    /// and populating the tag path sets.
    void createNodes(const CreateRequestSink & sink);

    bool hasNodes() const { return !root_nodes.empty(); }

private:
    std::shared_ptr<Node> parseNode(const std::string & key, const Poco::Util::AbstractConfiguration & config);

    /// Generate the (possibly random) root names once; one path per root node instance.
    const std::vector<std::string> & prepareRootPaths();

    std::vector<std::shared_ptr<Node>> root_nodes;
    Coordination::ACLs default_acls;
    bool use_remove_recursive = true;
    /// For generating random node names/data during setup.
    pcg64 rng{randomSeed()};

    std::unordered_map<std::string, PathSetPtr> tag_sets;
    std::unordered_map<std::string, PathSetPtr> children_of_sets;
    std::vector<PathSetPtr> literal_sets;
    std::vector<PathSetPtr> anonymous_sets;

    /// (parent path, tag name) pairs to check in validatePathSets.
    std::vector<std::pair<std::string, std::string>> tag_children_of_conflicts;

    /// Root paths created by `createNodes`, removed again by `cleanup`.
    std::vector<std::string> created_root_paths;
};
