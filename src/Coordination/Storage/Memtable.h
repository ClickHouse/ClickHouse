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
        const char * ptr = nullptr; // must be the first field, MemtableChildrenSet's union relies on it
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
struct MemtableChildrenSet
{
    /// Stored in the upper 2 bits of the union (the other union members are pointers, so their
    /// upper bits are unused).
    enum class Mode : uint64_t
    {
        /// The set is empty.
        Empty = 0,
        /// The set has one element, stored in `entry`.
        Inline = 0x4000000000000000,
        /// The set is stored as hash set at `*set`.
        Set = 0x8000000000000000,
    };

    MemtableChildrenSet() = default;

    MemtableChildrenSet(const MemtableChildrenSet &) = delete;
    MemtableChildrenSet & operator=(const MemtableChildrenSet &) = delete;

    MemtableChildrenSet(MemtableChildrenSet && other) noexcept
    {
        entry = other.entry; // copies the whole union; `mode` aliases the first 8 bytes of `entry`
        other.mode = Mode::Empty;
    }

    MemtableChildrenSet & operator=(MemtableChildrenSet && other) noexcept
    {
        if (this != &other)
        {
            destroy();
            entry = other.entry;
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
    static constexpr uint64_t MODE_MASK = static_cast<uint64_t>(Mode::Inline) | static_cast<uint64_t>(Mode::Set);
    static constexpr uint64_t PTR_MASK = ~MODE_MASK;

    union
    {
        /// When reading these, mask off the upper 2 bits of the first 8-byte value. They're `mode`.
        ChildrenSet2::Entry entry;
        ChildrenSet2 * set;

        /// Lives in the upper 2 bits.
        /// In all other elements of this union these bits are upper bits of a pointer, so we can
        /// store things in them.
        Mode mode = Mode::Empty;
    };

    Mode getMode() const { return static_cast<Mode>(static_cast<uint64_t>(mode) & MODE_MASK); }

    ChildrenSet2 * getSet() const
    {
        chassert(getMode() == Mode::Set);
        return reinterpret_cast<ChildrenSet2 *>(static_cast<uint64_t>(mode) & PTR_MASK);
    }

    ChildrenSet2::Entry getInlineEntry() const
    {
        chassert(getMode() == Mode::Inline);
        static_assert(offsetof(ChildrenSet2::Entry, ptr) == 0);
        ChildrenSet2::Entry e = entry;
        e.ptr = reinterpret_cast<const char *>(reinterpret_cast<uint64_t>(e.ptr) & PTR_MASK);
        return e;
    }

    void setInlineEntry(ChildrenSet2::Entry e)
    {
        chassert((reinterpret_cast<uint64_t>(e.ptr) & MODE_MASK) == 0);
        entry = e;
        mode = static_cast<Mode>(static_cast<uint64_t>(mode) | static_cast<uint64_t>(Mode::Inline));
    }

    void setSet(ChildrenSet2 * s)
    {
        chassert((reinterpret_cast<uint64_t>(s) & MODE_MASK) == 0);
        mode = static_cast<Mode>(static_cast<uint64_t>(Mode::Set) | reinterpret_cast<uint64_t>(s));
    }

    void destroy()
    {
        if (getMode() == Mode::Set)
            delete getSet();
        mode = Mode::Empty;
    }
};

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

    /// Inserts into `seen`, calls callback only for newly inserted entries with action == Create.
    /// If load_node is provided, calls it to obtain the FullNode to pass to check_node; otherwise passes nullptr.
    /// Returns false if stopped early because `check_node` returned false.
    bool visitChildren(
        const NodePathWithHash & path,
        const std::function<NodeRef(const NodePathWithHash &)> & load_node,
        const std::function<bool(std::string_view /*name*/, const NodeRef &, const FullNode *)> & check_node,
        ChildrenSet2 & seen, DB::Arena & arena) const;

    /// Make an immutable memtable with all of this Memtable's nodes, suitable only for iterating
    /// over all nodes. `children` is not populated, visitChildren won't work.
    /// The latest (mutable) block is copied, other blocks are just referenced by BlockPtr.
    MemtablePtr takeSnapshot() const;
};

}
