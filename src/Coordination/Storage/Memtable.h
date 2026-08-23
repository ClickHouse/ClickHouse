#pragma once

#include <Coordination/Storage/Common.h>
#include <Common/Arena.h>
#include <base/StringViewHash.h>
#include <base/defines.h>

#include <absl/container/flat_hash_set.h>

#include <string_view>
#include <vector>

namespace Coordination::Storage
{

/// Set of names and NodeAction-s of children of a node.
/// Stores only pointers to names. Arena storing the names should be managed separately, e.g. in Memtable.
/// Uses a little over 16 bytes per child.
struct ChildrenSet2
{
    struct Entry
    {
        const char * ptr = nullptr;
        uint32_t len = 0;

        /// Normally only Create and Remove actions are used in children sets, there's nothing to "Update".
        NodeAction action = NodeAction::Remove;

        std::string_view str() const { return std::string_view(ptr, static_cast<size_t>(len)); }

        /// Hash and comparison that look only at str() and ignore `action`.
        /// Also accept plain std::string_view, for heterogeneous lookup.
        struct StrHash
        {
            using is_transparent = void;
            size_t operator()(std::string_view s) const { return StringViewHash()(s); }
            size_t operator()(const Entry & e) const { return StringViewHash()(e.str()); }
        };
        struct StrEq
        {
            using is_transparent = void;
            bool operator()(const Entry & a, const Entry & b) const { return a.str() == b.str(); }
            bool operator()(const Entry & a, std::string_view b) const { return a.str() == b; }
            bool operator()(std::string_view a, const Entry & b) const { return a == b.str(); }
        };
    };

    /// Logically this is a map from string_view to NodeAction.
    /// We use set to pack the key+value into 16 bytes instead of 24.
    using Set = absl::flat_hash_set<Entry, Entry::StrHash, Entry::StrEq>;
    Set set;

    /// If name is not in the set, copies name into arena and adds to the set.
    /// If name is already in the set, leaves the entry unchanged and returns iterator to it.
    std::pair<Set::iterator, bool> insert(std::string_view name, NodeAction action, DB::Arena & arena);

    /// If name is not in the set, copies name into arena and adds to the set.
    /// If name is already in the set, updates the NodeAction in the set to
    /// combineActions(pre-existing action, `action`, strict=true).
    void insertCombine(std::string_view name, NodeAction action, DB::Arena & arena, bool strict);
};

/// Like ChildrenSet2, but using less memory if the set has 0 or 1 elements.
/// 16 bytes, same as ChildrenSet2::Entry: `mode` fits in what is tail padding in Entry.
struct MemtableChildrenSet
{
    enum class Mode : uint8_t
    {
        /// The set is empty.
        Empty = 0,
        /// The set has one element: {inline_name, inline_name_len, inline_action}.
        Inline,
        /// The set is stored as hash set at `*set`.
        Set,
    };

    MemtableChildrenSet() = default;

    MemtableChildrenSet(const MemtableChildrenSet &) = delete;
    MemtableChildrenSet & operator=(const MemtableChildrenSet &) = delete;

    MemtableChildrenSet(MemtableChildrenSet && other) noexcept
        : inline_name(other.inline_name)
        , inline_name_len(other.inline_name_len)
        , inline_action(other.inline_action)
        , mode(other.mode)
    {
        other.mode = Mode::Empty;
    }

    MemtableChildrenSet & operator=(MemtableChildrenSet && other) noexcept
    {
        if (this != &other)
        {
            destroy();
            inline_name = other.inline_name;
            inline_name_len = other.inline_name_len;
            inline_action = other.inline_action;
            mode = other.mode;
            other.mode = Mode::Empty;
        }
        return *this;
    }

    ~MemtableChildrenSet() { destroy(); }

    void insertCombine(std::string_view name, NodeAction action, DB::Arena & arena, bool strict);

    struct ConstIterator
    {
        struct Range
        {
            ChildrenSet2::Set::const_iterator it;
            ChildrenSet2::Set::const_iterator end;
        };

        Mode mode = Mode::Empty;
        union
        {
            ChildrenSet2::Entry entry;
            Range range;
        };

        /// (Not using the standard C++ iterator interface because it's not worth the 100 lines of
        ///  boilerplate, `while (it.next())` is perfectly fine..)
        bool next(ChildrenSet2::Entry & out);
    };

    ConstIterator iterate() const;

private:
    union
    {
        /// Inline mode: pointer to the name (in an arena managed separately, e.g. by Memtable).
        const char * inline_name = nullptr;
        /// Set mode: owned hash set. A plain untagged pointer, in particular so that LSan's
        /// reachability scan can trace it (it doesn't see through tagged pointers and would
        /// report the sets as leaked).
        ChildrenSet2 * set;
    };
    /// Inline mode: length of the name.
    uint32_t inline_name_len = 0;
    /// Inline mode: action of the single entry.
    NodeAction inline_action = NodeAction::Remove;
    Mode mode = Mode::Empty;

    ChildrenSet2 * getSet() const
    {
        chassert(mode == Mode::Set);
        return set;
    }

    ChildrenSet2::Entry getInlineEntry() const
    {
        chassert(mode == Mode::Inline);
        ChildrenSet2::Entry e;
        e.ptr = inline_name;
        e.len = inline_name_len;
        e.action = inline_action;
        return e;
    }

    void setInlineEntry(ChildrenSet2::Entry e)
    {
        inline_name = e.ptr;
        inline_name_len = e.len;
        inline_action = e.action;
        mode = Mode::Inline;
    }

    void setSet(ChildrenSet2 * s)
    {
        set = s;
        mode = Mode::Set;
    }

    void destroy()
    {
        if (mode == Mode::Set)
            delete set;
        mode = Mode::Empty;
    }
};

static_assert(sizeof(MemtableChildrenSet) == sizeof(ChildrenSet2::Entry));

struct Memtable;
using MemtablePtr = std::shared_ptr<Memtable>;

/// In-memory sequence of nodes. Can be appended to.
/// Nodes are in chronological order, i.e. later nodes override earlier ones (for equal paths).
/// When memtable gets big enough, it can be sorted and written ("flushed") to a new file (SortedFile).
struct Memtable
{
    size_t target_block_size = 0; // make blocks this big, in bytes

    /// Position in the chronological order of files and memtables (see
    /// StorageState::next_file_seqno). 0 for uncommitted memtables, which don't need it.
    uint32_t file_seqno = 0;

    /// Warning: We're not allowed to reallocate these blocks (e.g. BlockData::reserve) after at
    ///          least one node is appended to them. Because BlockWeakPtr in NodeRefCache::Entry
    ///          point to these blocks, and we rely on these weak ptrs not expiring.
    std::vector<BlockPtr> blocks;
    size_t total_bytes = 0; // sum of capacities of `blocks`
    size_t num_entries = 0; // number of nodes/tombstones in all blocks

    /// Number of Create-d nodes minus number of Remove-d nodes.
    int64_t node_count_delta = 0;

    NodeHashMap<MemtableChildrenSet> children;
    DB::Arena arena; // for children names

    /// Writes node to a block and adds/updates its parent's entry in `children` if needed.
    /// `strict` applies to the `children` update. Uncommitted memtables must pass false: not
    /// because their action sequences are invalid (they are valid), but because strict
    /// Create + Remove annihilation would erase the Remove tombstone; such tombstone is needed if
    /// the Create was committed but the Remove wasn't.
    NodeRef appendNode(FullNode & node, bool strict);

    void listChildrenNames(const NodePathWithHash & path, ChildrenSet2 & out, DB::Arena & arena) const;

    /// Make an immutable memtable with all of this Memtable's nodes, suitable only for iterating
    /// over all nodes. `children` is not populated, listChildrenNames won't work.
    /// The latest (mutable) block is copied, other blocks are just referenced by BlockPtr.
    MemtablePtr takeSnapshot() const;
};

}
