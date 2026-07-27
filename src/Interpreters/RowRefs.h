#pragma once

#include <iterator>
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
#include <Common/VectorWithMemoryTracking.h>
#include <base/defines.h>


namespace DB
{

class Block;
class ColumnReplicated;
struct StoredBlock;

/// Thrown by the RowRef constructor when a block or row number does not fit in its 32-bit field.
[[noreturn]] void throwRowRefOutOfRange(size_t block_no, size_t row_no);

/// 8-byte index-based reference to a row of the right table. `row_no` and `block_no` consume
/// 4 bytes each; `block_no` indexes the per-join `StoredColumnsIndex` (see below) that resolves
/// it to the stored block. This is the mapped value of MapsOne join hash maps and the leaf of
/// the ASOF sorted lookup vectors (resolved to a stored block the same way, at emit time).
///
/// `encode` packs both fields into a single UInt64 with `block_no` in the high half, so bit 63
/// of the encoded value is the MSB of `block_no`. That bit is the INLINE flag: when it is 1, the
/// value stored in a hash map cell or `LazyOutput` entry is the row reference itself (an inline
/// ref) - always the case for ANY joins, and for keys of ALL joins that have no duplicate rows.
/// When it is 0, the value is either zero (the "default row" marker in `LazyOutput::row_refs`)
/// or a `RowRefList` value (see below).
struct RowRef
{
    static constexpr UInt32 INLINE_FLAG = 0x80000000u;
    static constexpr UInt32 BLOCK_NO_MASK = 0x7FFFFFFFu;
    static constexpr UInt64 ENCODED_INLINE_FLAG = 1ull << 63;

    UInt32 row_no = 0;
    UInt32 block_no = 0; /// includes INLINE_FLAG in the MSB

    RowRef() = default;
    RowRef(size_t block_no_, size_t row_no_)
        : row_no(static_cast<UInt32>(row_no_))
        , block_no(static_cast<UInt32>(block_no_) | INLINE_FLAG)
    {
        /// Both fields are stored in 32 bits: blocks are limited to 4G rows (checked in
        /// addBlockToJoin) and the join is limited to 2^31 stored blocks (checked in
        /// StoredColumnsIndex::add), so this is only a defensive re-check of those limits.
        if (block_no_ > BLOCK_NO_MASK || row_no_ > std::numeric_limits<UInt32>::max()) [[unlikely]]
            throwRowRefOutOfRange(block_no_, row_no_);
    }

    UInt32 blockNo() const { return block_no & BLOCK_NO_MASK; }
    UInt32 rowNo() const { return row_no; }

    /// Encode this ref into the single UInt64 stored in hash map cells and `LazyOutput` entries.
    /// Built with explicit shifts (not std::bit_cast of the struct) so that the bit layout is the
    /// same on little- and big-endian systems: block_no (with INLINE_FLAG in its MSB) lands in the
    /// high half and row_no in the low half, matching the refWord* decoders below either way.
    UInt64 encode() const { return (static_cast<UInt64>(block_no) << 32) | row_no; }
};

static_assert(sizeof(RowRef) == 8, "RowRef must stay 8 bytes: it is the hash map cell payload");

/// Helpers for the encoded 64-bit ref words stored in LazyOutput / RowRefList nodes.
inline bool refWordIsInline(UInt64 word) { return word & RowRef::ENCODED_INLINE_FLAG; }
inline UInt32 refWordBlockNo(UInt64 word) { return static_cast<UInt32>(word >> 32) & RowRef::BLOCK_NO_MASK; }
inline UInt32 refWordRowNo(UInt64 word) { return static_cast<UInt32>(word); }

/// Thrown when an arena pointer does not fit in the low 48 bits of a RowRefList word, which would
/// make the pointer+count packing ambiguous. Cannot happen on Linux x86-64/aarch64 today: even with
/// 5-level paging (`CONFIG_X86_5LEVEL`, 57-bit VA) or arm64 52-bit LVA, the kernel hands out
/// mappings above the 47-bit boundary only when the mmap address hint explicitly requests them,
/// which our allocators never do. If that ever changes, the contingency is to shrink the count
/// field from 15 to 7 bits (bits 62..56, saturation at 127 instead of 32767), making the pointer
/// field 56-bit-safe; that does NOT limit rows per key (the saturated count already falls back to
/// the node's 56-bit `total_rows`), it only lowers the load-free `rows()` fast path from keys with
/// up to 32766 rows to keys with up to 126 rows - still covering most practical duplication.
[[noreturn]] void throwRowRefPointerTooLarge();

/// Mapped value of MapsAll join hash maps (ALL JOINs / non-unique keys): a tagged 8-byte word.
///   - bit 63 is 1: the key has exactly one row so far; the word IS the encoded RowRef (inline).
///   - bit 63 is 0 and the word is not 0: a pointer (bits 47..0) to an arena-allocated `Batch` node,
///     with the duplicate count packed into bits 62..48 (saturating; see COUNT_SAT). The count lets
///     the probe loop read `rows` straight from the cell word without dereferencing the node.
/// The node is allocated only when the first duplicate of a key arrives, so ALL-join cells are as
/// small as ANY-join cells for every key type, and unique keys never touch the arena.
struct RowRefList
{
    /// Low 48 bits of a list word hold the node pointer; bits 62..48 hold the saturating count.
    /// See the comment of `throwRowRefPointerTooLarge` for why 48 bits are enough and for the
    /// contingency if user-space mappings ever cross the 47-bit boundary.
    static constexpr UInt64 PTR_MASK = (1ull << 48) - 1;
    static constexpr UInt32 COUNT_SHIFT = 48;
    /// Sentinel stored in the count field meaning "count >= COUNT_SAT, load total_rows from the node".
    static constexpr UInt32 COUNT_SAT = 0x7FFFu;

    /// A single 64-byte node. The cell word always points at the FIRST ("cell") node of a key.
    /// `head` and the local slots are one contiguous `refs` array (refs[0] is the head) so the
    /// iterator can walk them as a single run without out-of-array pointer arithmetic.
    ///
    /// Cell node, unchained (2..7 rows): `refs[0]` holds the first row, `refs[1 .. size-1]` the rest;
    ///   `size == total_rows`; no pointers.
    /// Cell node, chained (>= 8 rows): `refs[0 .. 5]` hold the 6 oldest rows; `refs[SLOTS]` (= refs[6])
    ///   is a raw pointer to the NEWEST overflow node; `size == 6 != total_rows`.
    /// Range node (rerange "sorted" path): `is_range == 1`, `refs[0]` is the range start ref,
    ///   `total_rows` is the run length, no slots/chain.
    /// Overflow node: `refs[0]` is repurposed as the raw pointer to the next-older overflow node (0 at
    ///   the end of the chain); `refs[1 .. size]` hold refs; `is_range`/`total_rows` are unused.
    ///
    /// Iteration order is refs[0], then the cell node's local refs, then the overflow nodes newest-first.
    /// This equals the old RowRefList order for keys with up to 8 rows and deviates (deterministically)
    /// for larger keys; head identity (firstWord) is always the first-inserted row.
    struct Batch
    {
        /// Number of local ref slots besides `refs[0]` (the head): an overflow node uses them all,
        /// a cell node uses them for the rows after the head (the last doubles as the overflow ptr).
        static constexpr size_t SLOTS = 6;

        UInt64 is_range : 1 = 0;
        UInt64 size : 7 = 0;        /// cell node: local rows incl. head; overflow node: local refs
        UInt64 total_rows : 56 = 0; /// whole chain; authoritative in the cell node only
        /// One contiguous run: `refs[0]` is the head (cell node: first ref word; overflow node:
        /// next-older Batch *); `refs[1 .. SLOTS]` are the local slots. The occupied prefix is set
        /// by insert (Arena::alloc skips ctors). Keeping head and slots in one array lets the
        /// iterator form `&refs[0] + n` as in-array pointer arithmetic instead of undefined behavior.
        UInt64 refs[SLOTS + 1] {};
    };

    /// refs[0] + the SLOTS local slots: rows a cell node holds before it has to chain (= 7).
    static constexpr size_t MAX_LOCAL = 1 + Batch::SLOTS;

    UInt64 word = 0;

    RowRefList() = default;
    RowRefList(size_t block_no_, size_t row_no_) : word(RowRef(block_no_, row_no_).encode()) {}

    /// View an encoded cell / LazyOutput word as a RowRefList (the dominant runtime case: map
    /// cells and LazyOutput entries hold words, not (block_no, row_no) pairs).
    static RowRefList fromWord(UInt64 word_)
    {
        RowRefList list;
        list.word = word_;
        return list;
    }

    bool isInline() const { return refWordIsInline(word); }

    const Batch * asBatch() const
    {
        chassert(word != 0 && !isInline());
        return reinterpret_cast<const Batch *>(word & PTR_MASK); /// NOLINT(performance-no-int-to-ptr)
    }

    /// Not const on purpose: a const-qualified version returning a mutable `Batch *` would leak
    /// mutable access from a const list and could not coexist with the const overload above
    /// (overloads cannot differ only in the return type).
    Batch * asBatch() /// NOLINT(readability-make-member-function-const)
    {
        chassert(word != 0 && !isInline());
        return reinterpret_cast<Batch *>(word & PTR_MASK); /// NOLINT(performance-no-int-to-ptr)
    }

    /// Total number of rows for this key. Load-free unless the count saturated.
    UInt32 rows() const
    {
        if (isInline())
            return 1;
        const UInt32 count = static_cast<UInt32>((word >> COUNT_SHIFT) & COUNT_SAT);
        if (count != COUNT_SAT)
            return count;
        return static_cast<UInt32>(asBatch()->total_rows);
    }

    /// Encoded ref word of the first row (any-row semantics, e.g. RightAny on MapsAll).
    UInt64 firstWord() const { return isInline() ? word : asBatch()->refs[0]; }

    void setRange(UInt64 start_word, size_t rows_, Arena & pool)
    {
        chassert(refWordIsInline(start_word));

        /// A single-row range is just the inline ref itself: no node needed, and the emit paths
        /// already treat an inline word as a 1-length range.
        if (rows_ == 1)
        {
            word = start_word;
            return;
        }

        auto * b = pool.alloc<Batch>();
        b->is_range = 1;
        b->size = 0;
        b->total_rows = rows_;
        b->refs[0] = start_word;
        setListWord(b, rows_);
    }

    /// Insert one more row for this key. O(1). See the Batch comment for the representation.
    void insert(UInt64 ref_word, Arena & pool)
    {
        chassert(refWordIsInline(ref_word));

        /// First row: store it inline (no allocation).
        if (word == 0)
        {
            word = ref_word;
            return;
        }

        /// Second row: allocate the cell node and move the inline ref into its head.
        if (isInline())
        {
            auto * b = pool.alloc<Batch>();
            b->is_range = 0;
            b->size = 2;
            b->total_rows = 2;
            b->refs[0] = word;
            b->refs[1] = ref_word;
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
                b->refs[b->size] = ref_word;
                b->size = b->size + 1;
            }
            else /// full: evict the last local ref into a new overflow node, chaining the key
            {
                auto * n = pool.alloc<Batch>();
                n->is_range = 0;
                n->size = 2;
                n->total_rows = 0;
                n->refs[0] = 0; /// no older node yet
                n->refs[1] = b->refs[Batch::SLOTS]; /// the evicted last local ref
                n->refs[2] = ref_word;
                b->refs[Batch::SLOTS] = reinterpret_cast<UInt64>(n);
                b->size = MAX_LOCAL - 1; /// refs[0] + (SLOTS-1) local refs remain
            }
        }
        else /// chained cell node: append into the newest overflow node
        {
            auto * newest = reinterpret_cast<Batch *>(b->refs[Batch::SLOTS]); /// NOLINT(performance-no-int-to-ptr)
            if (newest->size < Batch::SLOTS)
            {
                newest->refs[newest->size + 1] = ref_word;
                newest->size = newest->size + 1;
            }
            else
            {
                auto * n = pool.alloc<Batch>();
                n->is_range = 0;
                n->size = 1;
                n->total_rows = 0;
                n->refs[0] = reinterpret_cast<UInt64>(newest); /// next-older node
                n->refs[1] = ref_word;
                b->refs[Batch::SLOTS] = reinterpret_cast<UInt64>(n);
            }
        }

        b->total_rows = new_total;
        setListWord(b, new_total);
    }

    /// Iterates encoded ref words: refs[0] first, then the cell node's local refs, then the overflow
    /// nodes newest-first (each in ref order). Handles inline, list, and range representations.
    ///
    /// The list of duplicates is a sequence of contiguous runs: in `Batch` the head and the local
    /// slots are one `refs` array, so the cell node's [refs[0] .. last local ref] is a single run
    /// starting at `&refs[0]`, and each overflow node contributes the run `refs[1 .. 1 + size)`. The
    /// hot path (the overflow walk of a heavily-duplicated key, where emit time concentrates) is then
    /// just three live pointers - `cur`/`run_end`/`next_node` - which keeps the emit loop off the
    /// stack. The inline and range representations take the separate `range_*` path (`cur == nullptr`).
    class ForwardIterator
    {
    public:
        explicit ForwardIterator(const RowRefList & list)
        {
            if (list.word == 0)
                return; /// empty (default-constructed) list -> ok() is false

            if (list.isInline())
            {
                range_word = list.word;
                range_remaining = 1;
                return;
            }

            const Batch * b = list.asBatch();
            if (b->is_range)
            {
                range_word = b->refs[0];
                range_remaining = static_cast<UInt32>(b->total_rows);
                return;
            }

            /// Run mode. The cell run covers refs[0] + the local slots, contiguous from `&refs[0]`:
            /// `size` words when unchained, refs[0] + `SLOTS - 1` slots (= `SLOTS` words, the last
            /// slot holds the overflow pointer) when chained. The overflow chain follows, newest-first.
            const bool chained = b->size != b->total_rows;
            cur = &b->refs[0];
            run_end = &b->refs[0] + (chained ? Batch::SLOTS : static_cast<size_t>(b->size));
            next_node = chained ? reinterpret_cast<const Batch *>(b->refs[Batch::SLOTS]) : nullptr; /// NOLINT(performance-no-int-to-ptr)
        }

        UInt64 operator * () const { return cur ? *cur : range_word; }

        void operator ++ ()
        {
            if (cur) /// run mode - the hot path: only cur/run_end/next_node are live here
            {
                ++cur;
                if (cur == run_end)
                {
                    if (next_node)
                    {
                        cur = &next_node->refs[1];
                        run_end = &next_node->refs[1] + next_node->size;
                        next_node = reinterpret_cast<const Batch *>(next_node->refs[0]); /// NOLINT(performance-no-int-to-ptr)
                    }
                    else
                        cur = nullptr; /// exhausted
                }
                return;
            }

            /// Range mode (inline ref / rerange run): consecutive rows live in one block, only row_no advances.
            if (--range_remaining)
                ++range_word;
        }

        bool ok() const { return cur != nullptr || range_remaining != 0; }

        /// Lets `refsOf(word)` drive a range-based for loop against a default sentinel end.
        bool operator != (std::default_sentinel_t) const { return ok(); }

    private:
        /// Run mode (eviction list): `cur` walks the current contiguous run bounded by `run_end`,
        /// `next_node` is the next overflow node (newest-first). `cur == nullptr` => range mode or done.
        const UInt64 * cur = nullptr;
        const UInt64 * run_end = nullptr;
        const Batch * next_node = nullptr;
        /// Range mode (inline ref / rerange): `range_word` is the current ref, `range_remaining` the count.
        UInt64 range_word = 0;
        UInt32 range_remaining = 0;
    };

    ForwardIterator begin() const { return ForwardIterator(*this); }
    std::default_sentinel_t end() const { return {}; } /// NOLINT(readability-convert-member-functions-to-static)

private:
    /// Repoint `word` at `b` with the saturating row count in bits 62..48. The cell-node pointer is
    /// stable across inserts, so this only rewrites the count bits of an already-resident cache line.
    void setListWord(Batch * b, UInt64 total_rows_)
    {
        const UInt64 ptr = reinterpret_cast<UInt64>(b);
        if (ptr & ~PTR_MASK) [[unlikely]]
            throwRowRefPointerTooLarge();
        const UInt64 count = total_rows_ < COUNT_SAT ? total_rows_ : COUNT_SAT;
        word = ptr | (count << COUNT_SHIFT);
    }
};

static_assert(sizeof(RowRefList) == 8, "RowRefList must stay 8 bytes: it is the hash map cell payload");
static_assert(sizeof(RowRefList::Batch) == 64, "RowRefList::Batch must stay one cache line");

/// Number of rows an encoded cell / LazyOutput word represents (inline ref = 1, list = its count,
/// range = its length), without spelling out a RowRefList at the call site. A zero word yields 0.
inline UInt32 refWordRows(UInt64 word)
{
    return RowRefList::fromWord(word).rows();
}

/// Iterable view over the encoded refs of a cell / LazyOutput word, so a call site can write
/// `for (UInt64 ref_word : refsOf(word))` instead of materializing a RowRefList and driving its
/// ForwardIterator by hand. Covers inline, list, and range words alike.
inline RowRefList refsOf(UInt64 word)
{
    return RowRefList::fromWord(word);
}

/// Encoded ref word of a key's first row: the "any row of the key" semantics used by ANY/RightAny/Semi
/// matches and by `StorageJoin` fills, on both MapsOne (RowRef) and MapsAll (RowRefList) cells.
template <typename Mapped>
ALWAYS_INLINE UInt64 firstRefWord(const Mapped & mapped)
{
    if constexpr (std::is_same_v<std::decay_t<Mapped>, RowRefList>)
        return mapped.firstWord();
    else
        return mapped.encode();
}

/// Maps `block_no` (the high half of RowRef) to the stored block.
/// Appended under mutex during the build phase (possibly from several ConcurrentHashJoin
/// slots sharing one index, so that block numbers are globally unique across slots and
/// remain valid after the slot block lists are spliced together in onBuildPhaseFinish).
/// Read lock-free at probe/emit time, which is safe because probing starts only after
/// the build phase is finished.
///
/// On top of the block map it builds a direct-pointer emit table: for each requested output column the
/// resolved `const IColumn *` per block (see `EmitColumn`). The hot emit loop then resolves a row ref to a
/// column with one indexed load instead of going through the stored block and its column vector.
/// `resolveEmitColumns` builds the requested positions lazily under `mutex` and hands back the per-column
/// base pointers; positions already built for the current generation are reused. The table is keyed by
/// `blocks_generation`, bumped whenever the stored blocks change (add/clearEntry, and in-place column
/// replacement via `invalidateEmitTable`), so a stale table is dropped and rebuilt. This matters for
/// `StorageJoin`, which (a) inserts more blocks between queries and (b) lets different queries select
/// different right-column subsets while sharing one index. StorageJoin serializes mutations (write lock)
/// against read-locked queries, so a rebuild never races a reader. The hot per-row emit loop stays
/// lock-free; only the per-probe-batch resolution takes the (briefly held) `mutex`.
///
/// Invalidation is generation-based (a counter compare) rather than rebuilding the table on every
/// probe: a rebuild would re-resolve every block for every probe batch under the mutex, whereas the
/// generation almost never changes after the build phase, so the table is built once and reused.
class StoredColumnsIndex
{
public:
    /// Resolved column pointers for one output (saved-block) column position, indexed by block_no.
    struct EmitColumn
    {
        /// `by_block[b]` is the source column for this position in block `b` (nullptr for a cleared block).
        PODArray<const IColumn *> by_block;
        /// `repl_by_block[b]` is that column as `ColumnReplicated *` if it is one, otherwise nullptr.
        PODArray<const ColumnReplicated *> repl_by_block;
    };

    /// Registers a stored block, returns its block_no. Throws when the 2^31 limit
    /// (RowRef::BLOCK_NO_MASK, the MSB is the inline flag) is exceeded.
    UInt32 add(const StoredBlock * block);

    /// Protection against dangling pointers: a popped/replaced block keeps its slot,
    /// but the slot is nulled so that a stale ref fails loudly instead of reading freed memory.
    void clearEntry(UInt32 block_no);

    /// Raw pointer for hot decode loops. Must not be called before the build phase is finished.
    const StoredBlock * const * blocksData() const { return blocks.data(); }

    const StoredBlock * at(UInt32 block_no) const
    {
        chassert(block_no < blocks.size());
        /// A cleared entry (see `clearEntry`) must never be reached: no refs to such a block exist.
        /// In debug builds a stale ref trips this assertion; in release builds it dereferences
        /// nullptr at a deterministic, near-zero address instead of reading freed memory.
        chassert(blocks[block_no] != nullptr);
        return blocks[block_no];
    }

    /// Resolve the emit table for the given saved-block column `positions` (the output columns of one
    /// probe), building any not-yet-built positions for the current generation (and dropping the whole
    /// table first if the blocks changed). Fills `out_columns[k]`/`out_replicated[k]` with the per-block
    /// base pointers for `positions[k]` (stable for as long as the generation does not change, which a
    /// StorageJoin read lock or a normal join's build-then-probe guarantees for the caller's lifetime).
    /// Holds `mutex` for the duration; called once per probe batch, never in the per-row loop.
    void resolveEmitColumns(
        size_t saved_columns_count,
        const std::vector<size_t> & positions,
        std::vector<const IColumn * const *> & out_columns,
        std::vector<const ColumnReplicated * const *> & out_replicated);

    /// Invalidate the emit table after the stored columns are replaced in place (e.g. shrinkStoredBlocksToFit
    /// `cloneResized`), which would otherwise leave the cached `const IColumn *` dangling. Bumps the generation.
    void invalidateEmitTable();

private:
    mutable std::mutex mutex;
    /// One entry per stored block (data-proportional): use the throwing memory tracker so a huge build
    /// fails the query at the limit instead of letting the process get OOM-killed.
    VectorWithMemoryTracking<const StoredBlock *> blocks;

    /// Built by `resolveEmitColumns`; indexed by saved-block position, nullptr for not-yet-requested ones.
    std::vector<std::unique_ptr<EmitColumn>> emit_columns;
    /// `blocks_generation` is bumped under `mutex` whenever the blocks change (add/clearEntry/invalidateEmitTable).
    /// `emit_columns` is valid for `emit_generation`; when it differs from `blocks_generation` the table is
    /// dropped and rebuilt. Both are guarded by `mutex`. `emit_generation` starts at SIZE_MAX (never built).
    size_t blocks_generation = 0;
    size_t emit_generation = std::numeric_limits<size_t>::max();
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
    virtual void insert(const IColumn &, UInt32 block_no, size_t) = 0;

    // This needs to be synchronized internally. Returns nullptr when there is no match.
    virtual const RowRef * findAsof(const IColumn &, size_t) = 0;
};


// It only contains a std::unique_ptr which is memmovable.
// Source: https://github.com/ClickHouse/ClickHouse/issues/4906
using AsofRowRefs = std::unique_ptr<SortedLookupVectorBase>;
AsofRowRefs createAsofRowRef(TypeIndex type, ASOFJoinInequality inequality);
}
