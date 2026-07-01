#pragma once

#include <Coordination/Storage/BlockPtr.h>
#include <Common/HashTable/Hash.h>
#include <Common/HashTable/HashMap.h>
#include <base/defines.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

namespace Coordination::Storage
{

struct BlockData;
struct FullNode;
struct ChildrenSet2;
class BlockCache;

/// SipHash of path string. We rely on not encountering collisions.
using NodePathHash = UInt128;

/// What happened to the node in a given file or memtable.
/// Inside one file/memtable, multiple actions are collapsed into one using combineActions.
///
/// There are two possible valid mental models:
///   Loose:  The last (by time of write) node+NodeAction determines the current state of the node.
///           All previous records for the same path are obsolete and ignored.
///   Strict: The sequence of all NodeAction-s that happened at a given path must be well-formed, e.g.
///           [Create, Update, Update, Remove, Create, Remove],
///           not [Remove, ...], not [Create, Create, ...] etc.
///           Consecutive nodes+actions for the same path can be merged into one using combineActions.
///           The merging is associative. The combination of all actions in the node's history
///           determines the current state of the node.
///
/// Strict model exists mostly just for consistency checks, e.g. that we didn't somehow end up
/// creating a node twice in a row. It would be ok to always use loose model and get rid of combineActions.
/// The committed state uses strict model. E.g. when applying deltas and when merging files we use
/// strict combineActions to assert that e.g. we don't try to Update a node that was Remove-d.
/// Inside uncommitted state we mostly use strict model (except that the first action may be Update
/// or Remove). When merging committed and uncommitted lookup results we have to use loose model
/// because there can be overlap between (the zxid ranges of) the two.
///
/// Our terminology is a bit ambiguous: the word "node" refers either to a znode or to an entry in
/// file/memtable, which has znode info + NodeAction (e.g. node may be a Remove tombstone).
enum class NodeAction : uint8_t
{
    Remove = 0,
    Update = 1,
    Create = 2,
    /// TODO: Amend (update a few fields like num_children and cversion, while leaving data and
    ///       other stats unchanged; useful for parent node in Create/Remove requests).
    ///       Possibly not worth the extra complexity and overhead.
};

/// What's the combined action of doing `first`, then `second`.
///
/// If `strict` is false, just returns `second` (loose model: the last record wins).
///
/// If `strict` is true:
///   Create + Update = Create
///   Create + Remove = nullopt
///   Update + Update = Update
///   Update + Remove = Remove
///   Remove + Create = Update
///   nullopt+ X      = X
///   anything else = throw LOGICAL_ERROR.
///
/// In other words, composition of partial functions on node existence: Create: absent -> present,
/// Update: present -> present, Remove: present -> absent. nullopt is Create + Remove cancelling
/// out, as if the node never existed. Composition of partial functions is associative.
/// (Note that the nullopt annihilation is only valid if `first` really is the first action in the
///  node's entire history, i.e. the node didn't exist before; that's part of what strict means.)
std::optional<NodeAction> combineActions(std::optional<NodeAction> first, NodeAction second, bool strict);

inline int64_t nodeCountDelta(NodeAction action)
{
    if (action == NodeAction::Create)
        return 1;
    if (action == NodeAction::Remove)
        return -1;
    return 0;
}

/// Map NodePathHash -> V.
/// Takes advantage of the key already being a hash, so it doesn't need to be hashed again
/// (and no hash is stored in the slot, so a slot is just sizeof(NodePathHash) + sizeof(V)).
template <typename V>
using NodeHashMap = HashMap<NodePathHash, V, UInt128TrivialHash>;

struct NodePathWithHash;

/// Znode path and precalculated number of components in that path.
/// Doesn't own memory, similar to std::string_view.
struct NodePath
{
    uint32_t depth = 0; // number of components in the path: "/" - 0, "/foo" - 1, etc
    uint32_t len = 0;
    const char * ptr = "";

    NodePath() = default; // lowest possible path

    /// `path` must be a normalized znode path: starts with '/', no trailing slash (except the
    /// root path "/").
    explicit NodePath(std::string_view path)
        : NodePath(path, path.size() <= 1 ? 0 : static_cast<uint32_t>(std::count(path.begin(), path.end(), '/')))
    {
    }

    NodePath(std::string_view path, uint32_t depth_)
        : depth(depth_), len(static_cast<uint32_t>(path.size())), ptr(path.data())
    {
    }

    std::string_view str() const { return std::string_view(ptr, len); }

    NodePathHash calculateHash() const;
    NodePathWithHash withCalculatedHash() const;

    /// Path of the parent znode: "/" for "/foo", "/a" for "/a/b".
    /// Must not be called on the root path (depth == 0).
    NodePath parentPath() const;

    /// Path to child znode: "/foo/bar" + "baz" = "/foo/bar/baz".
    /// Puts the path string into `path_buf`, returns a reference into it.
    NodePath childPath(std::string_view child_name, std::string & path_buf) const;

    /// The last component of the path: "b" for "/a/b". Must not be called on the root path.
    std::string_view baseName() const;

    /// Order in which znodes are sorted in files: by (depth, path string).
    /// All children of a node are consecutive in this order.
    int compare(const NodePath & rhs) const
    {
        if (depth != rhs.depth)
            return depth < rhs.depth ? -1 : 1;
        if (int c = memcmp(ptr, rhs.ptr, std::min(len, rhs.len)))
            return c;
        return len == rhs.len ? 0 : (len < rhs.len ? -1 : 1);
    }
};

/// NodePath with its hash always calculated (NodePath::calculateHash).
struct NodePathWithHash
{
    NodePath path;
    NodePathHash hash {};
};

/// Shared pointer referring to an immutable serialized node (or tombstone) in memory.
/// Can be fully or partially deserialized on demand, which is always thread safe.
struct NodeRef
{
    NodeAction action = NodeAction::Remove;
    uint32_t offset = 0; // from start of the block
    BlockPtr block; // may be nullptr if action == Remove

    /// True if the node exists.
    explicit operator bool() const { return action != NodeAction::Remove; }

    /// Deserialize the znode.
    /// Decompresses path into out_path_buf and points out_node.path into it.
    /// Points out_node.data_ptr directly into the block's memory.
    /// this->action is ignored, out_node.action is assigned; i.e. the caller doesn't need to know
    /// NodeAction in order to read the node.
    void read(FullNode & out_node, std::string & out_path_buf) const;

    /// Partial deserialization functions. Deserialize a subset of the fields, slightly faster than
    /// deserializing everything.

    /// Everything except `path`.
    void readWithoutPath(FullNode & out_node) const;

    void readWithKnownPath(FullNode & out_node, NodePath path) const;
    void readWithKnownPath(FullNode & out_node, NodePathWithHash path) const;

    /// Only path and a couple fields that are usually needed together with path.
    void readPath(NodePath & out_path, std::string & out_path_buf, uint32_t & out_serialized_size, NodeAction & out_action) const;

    /// Only serialized size (how much to advance `offset` to get to the next node in the block).
    uint32_t readSerializedSize() const;
};

}
