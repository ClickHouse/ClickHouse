#pragma once

#include <bit>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

#include <Columns/IColumn_fwd.h>
#include <Core/Joins.h>
#include <Core/TypeId.h>
#include <Common/Arena.h>
#include <Common/PODArray.h>
#include <base/defines.h>


namespace DB
{

class Block;
class ColumnReplicated;

struct ColumnsInfo
{
    explicit ColumnsInfo(Columns && columns_);

    Columns columns;
    /// Sometimes we need to insert rows into a regular column from a Replicated column.
    /// And to avoid virtual calls and casts per each row insertion we store pointer
    /// to the replicated column for each column in the list above.
    /// If columns is not Replicated, pointer will be nullptr.
    PODArray<const ColumnReplicated *> replicated_columns;

    /// Must be called after `columns` are replaced in-place (e.g. by cloneResized).
    /// Raw pointers in `replicated_columns` point into the old column objects and become
    /// dangling as soon as those objects are released.
    void rebuildReplicatedColumns();
};

/// Reference to the row in block.
/// Used by ASOF join (sorted lookup vectors) and as a transient decoded form at emit time.
/// Hash map cells do NOT store this type any more, see BuildRef / BuildRefList below.
struct RowRef
{
    using SizeT = uint32_t; /// Do not use size_t cause of memory economy

    const ColumnsInfo * columns_info = nullptr;
    SizeT row_num = 0;

    RowRef() = default;
    RowRef(const ColumnsInfo * columns_, size_t row_num_)
        : columns_info(columns_)
        , row_num(static_cast<SizeT>(row_num_))
    {}
};

/// Compact 8-byte index-based reference to a row of the right table: (block_no, row_no).
/// `block_no` indexes the per-join `StoredColumnsIndex` (see below) that resolves it to the
/// stored block's `ColumnsInfo`. This is the mapped value of MapsOne join hash maps.
///
/// Layout: `row_no` occupies the LOW half and `block_no` the HIGH half of the 8-byte word
/// (little-endian), so the MSB of the `block_no` field is bit 63 of the whole word.
/// That bit is the INLINE/SINGLETON flag. It is always set for refs stored in hash map
/// cells and `LazyOutput` entries. It distinguishes an inline ref from:
///   - the zero word (the "default row" marker in `LazyOutput::row_refs`),
///   - a count-tagged `BuildRefList` list word (user-space pointers have bit 63 clear on
///     x86-64/aarch64, and its count occupies bits 62..48).
/// Thanks to the flag, unique keys of ALL joins cost no extra memory and no extra loads
/// at probe time (the win of RadixHashJoin's SINGLETON_FLAG design).
struct BuildRef
{
    static constexpr UInt32 SINGLETON_FLAG = 0x80000000u;
    static constexpr UInt32 BLOCK_NO_MASK = 0x7FFFFFFFu;
    static constexpr UInt64 SINGLETON_WORD_FLAG = 1ull << 63;

    UInt32 row_no = 0;
    UInt32 block_no = 0; /// includes SINGLETON_FLAG in the MSB

    BuildRef() = default;
    BuildRef(size_t block_no_, size_t row_no_)
        : row_no(static_cast<UInt32>(row_no_))
        , block_no(static_cast<UInt32>(block_no_) | SINGLETON_FLAG)
    {
        /// RowRef::SizeT is UInt32: blocks are limited to 4G rows (checked in addBlockToJoin)
        /// and the join is limited to 2^31 stored blocks (checked in StoredColumnsIndex::add).
        chassert(block_no_ <= BLOCK_NO_MASK);
        chassert(row_no_ <= std::numeric_limits<UInt32>::max());
    }

    UInt32 blockNo() const { return block_no & BLOCK_NO_MASK; }
    UInt32 rowNo() const { return row_no; }

    UInt64 word() const { return std::bit_cast<UInt64>(*this); }
    static BuildRef fromWord(UInt64 word_) { return std::bit_cast<BuildRef>(word_); }
};

static_assert(sizeof(BuildRef) == 8, "BuildRef must stay 8 bytes: it is the hash map cell payload");

/// Helpers for the encoded 64-bit ref words stored in LazyOutput / BuildRefList nodes.
inline bool refWordIsInline(UInt64 word) { return word & BuildRef::SINGLETON_WORD_FLAG; }
inline UInt32 refWordBlockNo(UInt64 word) { return static_cast<UInt32>(word >> 32) & BuildRef::BLOCK_NO_MASK; }
inline UInt32 refWordRowNo(UInt64 word) { return static_cast<UInt32>(word); }

/// Thrown when an arena pointer does not fit in the low 48 bits of a BuildRefList word, which would
/// make the count-tagged encoding ambiguous. Cannot happen on Linux x86-64/aarch64 today: even with
/// 5-level paging (`CONFIG_X86_5LEVEL`, 57-bit VA) or arm64 52-bit LVA, the kernel hands out
/// mappings above the 47-bit boundary only when the mmap address hint explicitly requests them,
/// which our allocators never do. If that ever changes, the contingency is to shrink the count
/// field from 15 to 7 bits (bits 62..56, saturation at 127 instead of 32767), making the pointer
/// field 56-bit-safe; that does NOT limit rows per key (the saturated count already falls back to
/// the node's 56-bit `total_rows`), it only lowers the load-free `rows()` fast path from keys with
/// up to 32766 rows to keys with up to 126 rows - still covering most practical duplication.
[[noreturn]] void throwBuildRefPointerTooLarge();

/// Mapped value of MapsAll join hash maps (ALL JOINs / non-unique keys): a tagged 8-byte word.
///   - bit 63 set: the key has exactly one row so far; the word IS the encoded BuildRef (singleton).
///   - bit 63 clear, non-zero: a pointer (bits 47..0) to an arena-allocated `Batch` node, with the
///     duplicate count packed into bits 62..48 (saturating; see COUNT_SAT). The count lets the probe
///     loop read `rows` straight from the cell word without dereferencing the node.
/// The node is allocated only when the first duplicate of a key arrives, so ALL-join cells are as
/// small as ANY-join cells for every key type, and unique keys never touch the arena.
struct BuildRefList
{
    /// Low 48 bits of a list word hold the node pointer; bits 62..48 hold the saturating count.
    /// See the comment of `throwBuildRefPointerTooLarge` for why 48 bits are enough and for the
    /// contingency if user-space mappings ever cross the 47-bit boundary.
    static constexpr UInt64 PTR_MASK = (1ull << 48) - 1;
    static constexpr UInt32 COUNT_SHIFT = 48;
    /// Sentinel stored in the count field meaning "count >= COUNT_SAT, load total_rows from the node".
    static constexpr UInt32 COUNT_SAT = 0x7FFFu;

    /// A single 64-byte node. The cell word always points at the FIRST ("cell") node of a key.
    ///
    /// Cell node, unchained (2..7 rows): `head` holds the first row, `slots[0 .. size-2]` the rest;
    ///   `size == total_rows`; no pointers.
    /// Cell node, chained (>= 8 rows): `head` + `slots[0..4]` hold the 6 oldest rows; `slots[5]` is a
    ///   raw pointer to the NEWEST overflow node; `size == 6 != total_rows`.
    /// Range node (rerange "sorted" path): `is_range == 1`, `head` is the range start ref, `total_rows`
    ///   is the run length, no slots/chain.
    /// Overflow node: `head` is repurposed as the raw pointer to the next-older overflow node (0 at the
    ///   end of the chain); `slots[0 .. size-1]` hold refs; `is_range`/`total_rows` are unused.
    ///
    /// Iteration order is head, then the cell node's local slots, then the overflow nodes newest-first.
    /// This equals the old RowRefList order for keys with up to 8 rows and deviates (deterministically)
    /// for larger keys; head identity (firstWord) is always the first-inserted row.
    struct Batch
    {
        /// Number of refs in `slots`: of an overflow node fully, of a cell node besides the head.
        static constexpr size_t SLOTS = 6;

        UInt64 is_range : 1 = 0;
        UInt64 size : 7 = 0;        /// cell node: local rows incl. head; overflow node: local refs
        UInt64 total_rows : 56 = 0; /// whole chain; authoritative in the cell node only
        UInt64 head = 0;            /// cell node: first ref word; overflow node: next-older Batch *
        UInt64 slots[SLOTS] {};     /// the occupied prefix is set by insert (Arena::alloc skips ctors)

        void assertIsRange() const
        {
            chassert(is_range, "BuildRefList node does not represent a range");
            chassert(total_rows >= 1, "BuildRefList range should have at least one row");
        }
    };

    /// head + slots: rows a cell node holds before it has to chain (= 7).
    static constexpr size_t MAX_LOCAL = 1 + Batch::SLOTS;

    UInt64 word = 0;

    BuildRefList() = default;
    BuildRefList(size_t block_no_, size_t row_no_) : word(BuildRef(block_no_, row_no_).word()) {}

    bool isSingleton() const { return refWordIsInline(word); }

    const Batch * asBatch() const
    {
        chassert(word != 0 && !isSingleton());
        return reinterpret_cast<const Batch *>(word & PTR_MASK); /// NOLINT(performance-no-int-to-ptr)
    }

    Batch * asBatch() /// NOLINT(readability-make-member-function-const)
    {
        chassert(word != 0 && !isSingleton());
        return reinterpret_cast<Batch *>(word & PTR_MASK); /// NOLINT(performance-no-int-to-ptr)
    }

    /// Total number of rows for this key. Load-free unless the count saturated.
    UInt32 rows() const
    {
        if (isSingleton())
            return 1;
        const UInt32 count = static_cast<UInt32>((word >> COUNT_SHIFT) & COUNT_SAT);
        if (count != COUNT_SAT)
            return count;
        return static_cast<UInt32>(asBatch()->total_rows);
    }

    /// Encoded ref word of the first row (any-row semantics, e.g. RightAny on MapsAll).
    UInt64 firstWord() const { return isSingleton() ? word : asBatch()->head; }

    void setRange(UInt64 start_word, size_t rows_, Arena & pool)
    {
        chassert(refWordIsInline(start_word));
        auto * b = pool.alloc<Batch>();
        b->is_range = 1;
        b->size = 0;
        b->total_rows = rows_;
        b->head = start_word;
        setListWord(b, rows_);
    }

    /// Insert one more row for this key. O(1). See the Batch comment for the representation.
    void insert(UInt64 ref_word, Arena & pool)
    {
        chassert(refWordIsInline(ref_word));

        /// First row: start as a singleton (no allocation).
        if (word == 0)
        {
            word = ref_word;
            return;
        }

        /// Second row: allocate the cell node and move the singleton into its head.
        if (isSingleton())
        {
            auto * b = pool.alloc<Batch>();
            b->is_range = 0;
            b->size = 2;
            b->total_rows = 2;
            b->head = word;
            b->slots[0] = ref_word;
            setListWord(b, 2);
            return;
        }

        Batch * b = asBatch();
        chassert(!b->is_range);
        const UInt64 new_total = b->total_rows + 1;

        if (b->size == b->total_rows) /// unchained cell node
        {
            if (b->size < MAX_LOCAL) /// room left in slots
            {
                b->slots[b->size - 1] = ref_word;
                b->size = b->size + 1;
            }
            else /// full: evict the last local ref into a new overflow node, chaining the key
            {
                auto * n = pool.alloc<Batch>();
                n->is_range = 0;
                n->size = 2;
                n->total_rows = 0;
                n->head = 0; /// no older node yet
                n->slots[0] = b->slots[Batch::SLOTS - 1]; /// the evicted last local ref
                n->slots[1] = ref_word;
                b->slots[Batch::SLOTS - 1] = reinterpret_cast<UInt64>(n);
                b->size = MAX_LOCAL - 1; /// head + (SLOTS-1) local refs remain
            }
        }
        else /// chained cell node: append into the newest overflow node
        {
            auto * newest = reinterpret_cast<Batch *>(b->slots[Batch::SLOTS - 1]); /// NOLINT(performance-no-int-to-ptr)
            if (newest->size < Batch::SLOTS)
            {
                newest->slots[newest->size] = ref_word;
                newest->size = newest->size + 1;
            }
            else
            {
                auto * n = pool.alloc<Batch>();
                n->is_range = 0;
                n->size = 1;
                n->total_rows = 0;
                n->head = reinterpret_cast<UInt64>(newest); /// next-older node
                n->slots[0] = ref_word;
                b->slots[Batch::SLOTS - 1] = reinterpret_cast<UInt64>(n);
            }
        }

        b->total_rows = new_total;
        setListWord(b, new_total);
    }

    /// Iterates encoded ref words: head first, then the cell node's local slots, then the overflow
    /// nodes newest-first (each in slot order). Handles singleton, list, and range representations.
    class ForwardIterator
    {
    public:
        explicit ForwardIterator(const BuildRefList & list)
        {
            if (list.word == 0)
                return; /// empty (default-constructed) list

            if (list.isSingleton())
            {
                head_word = list.word;
                remaining_range = 1;
                return;
            }

            const Batch * b = list.asBatch();
            head_word = b->head;
            if (b->is_range)
            {
                remaining_range = static_cast<UInt32>(b->total_rows);
                return;
            }

            remaining_range = 1; /// yield the head once
            cell = b;
            if (b->size != b->total_rows) /// chained
            {
                local_refs = Batch::SLOTS - 1;
                overflow = reinterpret_cast<const Batch *>(b->slots[Batch::SLOTS - 1]); /// NOLINT(performance-no-int-to-ptr)
            }
            else
            {
                local_refs = static_cast<UInt32>(b->size) - 1;
            }
        }

        UInt64 operator * () const
        {
            if (remaining_range)
                return head_word;
            if (local_pos < local_refs)
                return cell->slots[local_pos];
            return overflow->slots[overflow_pos];
        }

        void operator ++ ()
        {
            if (remaining_range)
            {
                --remaining_range;
                /// Consecutive rows of a range live in one block: only row_no advances.
                head_word += 1;
                return;
            }

            if (local_pos < local_refs)
            {
                ++local_pos;
                return;
            }

            if (overflow)
            {
                ++overflow_pos;
                if (overflow_pos >= overflow->size)
                {
                    overflow = reinterpret_cast<const Batch *>(overflow->head); /// NOLINT(performance-no-int-to-ptr)
                    overflow_pos = 0;
                }
            }
        }

        bool ok() const { return remaining_range || local_pos < local_refs || overflow != nullptr; }

    private:
        UInt64 head_word = 0;
        UInt32 remaining_range = 0;
        const Batch * cell = nullptr;
        const Batch * overflow = nullptr;
        UInt32 local_refs = 0;
        UInt32 local_pos = 0;
        UInt32 overflow_pos = 0;
    };

    ForwardIterator begin() const { return ForwardIterator(*this); }

private:
    /// Repoint `word` at `b` with the saturating row count in bits 62..48. The cell-node pointer is
    /// stable across inserts, so this only rewrites the count bits of an already-resident cache line.
    void setListWord(Batch * b, UInt64 total_rows_)
    {
        const UInt64 ptr = reinterpret_cast<UInt64>(b);
        if (ptr & ~PTR_MASK) [[unlikely]]
            throwBuildRefPointerTooLarge();
        const UInt64 count = total_rows_ < COUNT_SAT ? total_rows_ : COUNT_SAT;
        word = ptr | (count << COUNT_SHIFT);
    }
};

static_assert(sizeof(BuildRefList) == 8, "BuildRefList must stay 8 bytes: it is the hash map cell payload");
static_assert(sizeof(BuildRefList::Batch) == 64, "BuildRefList::Batch must stay one cache line");

/// Maps `block_no` (the high half of BuildRef) to the stored block's ColumnsInfo.
/// Appended under mutex during the build phase (possibly from several ConcurrentHashJoin
/// slots sharing one index, so that block numbers are globally unique across slots and
/// remain valid after the slot block lists are spliced together in onBuildPhaseFinish).
/// Read lock-free at probe/emit time, which is safe because probing starts only after
/// the build phase is finished.
class StoredColumnsIndex
{
public:
    /// Registers a stored block, returns its block_no. Throws when the 2^31 limit
    /// (BuildRef::BLOCK_NO_MASK, the MSB is the singleton flag) is exceeded.
    UInt32 add(const ColumnsInfo * columns_info);

    /// Protection against dangling pointers: a popped/replaced block keeps its slot,
    /// but the slot is nulled so that a stale ref fails loudly instead of reading freed memory.
    void clearEntry(UInt32 block_no);

    /// Raw pointer for hot decode loops. Must not be called before the build phase is finished.
    const ColumnsInfo * const * blocksData() const { return blocks.data(); }

    const ColumnsInfo * at(UInt32 block_no) const
    {
        chassert(block_no < blocks.size());
        /// A cleared entry (see `clearEntry`) must never be reached: no refs to such a block exist.
        /// In debug builds a stale ref trips this assertion; in release builds it dereferences
        /// nullptr at a deterministic, near-zero address instead of reading freed memory.
        chassert(blocks[block_no] != nullptr);
        return blocks[block_no];
    }

private:
    mutable std::mutex mutex;
    std::vector<const ColumnsInfo *> blocks;
};

using StoredColumnsIndexPtr = std::shared_ptr<StoredColumnsIndex>;

/**
 * This class is intended to push sortable data into.
 * When looking up values the container ensures that it is sorted for log(N) lookup
 * After calling any of the lookup methods, it is no longer allowed to insert more data as this would invalidate the
 * references that can be returned by the lookup methods
 */
struct SortedLookupVectorBase
{
    SortedLookupVectorBase() = default;
    virtual ~SortedLookupVectorBase() = default;

    static std::optional<TypeIndex> getTypeSize(const IColumn & asof_column, size_t & type_size);

    // This will be synchronized by the rwlock mutex in Join.h
    virtual void insert(const IColumn &, const ColumnsInfo *, size_t) = 0;

    // This needs to be synchronized internally
    virtual RowRef * findAsof(const IColumn &, size_t) = 0;
};


// It only contains a std::unique_ptr which is memmovable.
// Source: https://github.com/ClickHouse/ClickHouse/issues/4906
using AsofRowRefs = std::unique_ptr<SortedLookupVectorBase>;
AsofRowRefs createAsofRowRef(TypeIndex type, ASOFJoinInequality inequality);
}
