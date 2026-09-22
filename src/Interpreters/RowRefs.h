#pragma once

#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <typeinfo>
#include <vector>

#include <Columns/IColumn.h>
#include <Core/Joins.h>
#include <Core/TypeId.h>
#include <DataTypes/IDataType_fwd.h>
#include <base/defines.h>
#include <Common/Arena.h>
#include <Common/PODArray.h>
#include <Common/VectorWithMemoryTracking.h>


namespace DB
{

class Block;
class ColumnReplicated;
class RowDataStore;
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

    /// The exact inverse of `encode`, `INLINE_FLAG` included, so re-encoding reproduces the word.
    static RowRef fromWord(UInt64 word)
    {
        RowRef ref;
        ref.row_no = static_cast<UInt32>(word);
        ref.block_no = static_cast<UInt32>(word >> 32);
        return ref;
    }
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

/// Iterating a range advances only the word's 32-bit row field, so a run past the last addressable
/// row would carry into `block_no` and start reading another block.
[[noreturn]] void throwRowRefRangeOutOfRange(size_t row_no, size_t rows);

/// Mapped value of MapsAll join hash maps (ALL JOINs / non-unique keys): a tagged 8-byte word.
///   - bit 63 is 1: the key has exactly one row so far; the word IS the encoded RowRef (inline).
///   - bit 63 is 0 and the word is not 0: an 8-aligned arena pointer in bits 47..3, a count in
///     bits 62..48 (saturating; see COUNT_SAT), and a tag in bits 2..0 naming the layout:
///       `TAG_BATCH` a `Batch` node (the standard `HashJoin` insert path, see below);
///       `TAG_RUN`   a headerless block of `count` contiguous refs, `count` exact in [2, MAX_RANGE_REFS];
///       `TAG_CHAIN` a newest-first chain of ranges; the pointer is the newest range's 16-byte header.
///       `TAG_COUNT`, `TAG_FILL`, `TAG_FILL_H` are build-time words `PartitionedHashJoin` never publishes.
/// Every layout shares the reader contract (`rows`, `firstWord`, `ForwardIterator`).
/// `LazyOutput`, the used flags and the non-joined fillers never care which build produced the word.
/// A `Batch` is allocated only on the first duplicate. ALL-join cells stay as small as ANY-join cells,
/// and unique keys never touch the arena. `PartitionedHashJoin` writes run and chain (`SpanWriter`):
/// 8 bytes per row, and a 16-byte header on every range after the key's first.
struct RowRefList
{
    /// Low 48 bits of a list word hold the node pointer; bits 62..48 hold the saturating count.
    /// See the comment of `throwRowRefPointerTooLarge` for why 48 bits are enough and for the
    /// contingency if user-space mappings ever cross the 47-bit boundary.
    static constexpr UInt64 PTR_MASK = (1ull << 48) - 1;
    static constexpr UInt32 COUNT_SHIFT = 48;
    /// Sentinel stored in the count field meaning "count >= COUNT_SAT, load the total from the node".
    static constexpr UInt32 COUNT_SAT = 0x7FFFu;
    /// A range holds at most this many refs, so a published count equal to `COUNT_SAT` is never a run.
    static constexpr UInt32 MAX_RANGE_REFS = COUNT_SAT - 1;

    /// Bits 2..0 of a non-inline, non-zero word. Every pointed-at object is 8-aligned, so they are free.
    static constexpr UInt64 TAG_MASK = 0x7;
    static constexpr UInt64 TAG_BATCH = 0x0;
    static constexpr UInt64 TAG_FILL = 0x2; /// build-time: cursor into the key's open span
    static constexpr UInt64 TAG_COUNT = 0x3; /// build-time: n_items of the current pass
    static constexpr UInt64 TAG_FILL_H = 0x4; /// build-time: the open chunk has a header
    static constexpr UInt64 TAG_CHAIN = 0x5;
    static constexpr UInt64 TAG_RUN = 0x6;
    static constexpr UInt64 NODE_PTR_MASK = PTR_MASK & ~TAG_MASK;

    /// 16-byte header in front of every range of a key except the key's first. The range's refs follow
    /// the header directly (`refs`). The previous pointer names the previous range's header when
    /// `prevHasHeader`, its refs otherwise.
    struct RangeHeader
    {
        UInt64 len_total = 0; /// bits 63..48 own_len, bits 47..0 total (this range plus all older)
        UInt64 prev = 0; /// bits 63..48 prev_len, bits 47..3 prev ptr, bit 2 prev_has_header

        UInt32 ownLen() const { return static_cast<UInt32>(len_total >> COUNT_SHIFT); }
        UInt64 total() const { return len_total & PTR_MASK; }
        const UInt64 * refs() const { return reinterpret_cast<const UInt64 *>(this) + 2; }
        static UInt32 prevLen(UInt64 prev_word) { return static_cast<UInt32>(prev_word >> COUNT_SHIFT); }
        static const UInt64 * prevPtr(UInt64 prev_word)
        {
            return reinterpret_cast<const UInt64 *>(prev_word & NODE_PTR_MASK); /// NOLINT(performance-no-int-to-ptr)
        }
        static bool prevHasHeader(UInt64 prev_word) { return (prev_word >> 2) & 1; }
    };

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

    /// A headerless block of `count` refs, `count` in [2, MAX_RANGE_REFS]. A larger contribution is a
    /// chain of chunks, each at most `MAX_RANGE_REFS`.
    static RowRefList makeRun(const UInt64 * refs, size_t count)
    {
        chassert(count >= 2 && count <= MAX_RANGE_REFS);
        return fromWord(checkedNodePointer(refs) | (static_cast<UInt64>(count) << COUNT_SHIFT) | TAG_RUN);
    }

    static RowRefList makeChain(const RangeHeader * header, UInt64 total)
    {
        const UInt64 count = total < COUNT_SAT ? total : COUNT_SAT;
        return fromWord(checkedNodePointer(header) | (count << COUNT_SHIFT) | TAG_CHAIN);
    }

    /// Build-time: `n_items` of this pass (the previous published word counts as one item when
    /// `has_prev`), owner-private until the pass finishes.
    static RowRefList makeCount(UInt64 n_items, bool has_prev)
    {
        chassert(n_items < (1ull << 59));
        return fromWord((n_items << 4) | (static_cast<UInt64>(has_prev) << 3) | TAG_COUNT);
    }

    /// Build-time: cursor at the next free slot of the key's span, `placed` refs in the open chunk.
    static RowRefList makeFill(const UInt64 * cursor, UInt32 placed, bool has_header)
    {
        chassert(placed <= MAX_RANGE_REFS);
        return fromWord(checkedNodePointer(cursor) | (static_cast<UInt64>(placed) << COUNT_SHIFT) | (has_header ? TAG_FILL_H : TAG_FILL));
    }

    /// `prev` is the previous range's header when `prev_has_header`, its refs otherwise.
    static UInt64 makePrevWord(const void * prev, UInt32 prev_len, bool prev_has_header)
    {
        chassert(prev_len >= 1 && prev_len <= MAX_RANGE_REFS);
        return checkedNodePointer(prev) | (static_cast<UInt64>(prev_len) << COUNT_SHIFT) | (static_cast<UInt64>(prev_has_header) << 2);
    }

    /// `w` is the key's previous published word: `TAG_RUN` or `TAG_CHAIN`.
    static UInt64 makePrevWordFrom(UInt64 w)
    {
        const RowRefList list = fromWord(w);
        if (list.isRun())
            return makePrevWord(list.runRefs(), list.countField(), false);
        const RangeHeader * header = list.chainHeader();
        return makePrevWord(header, header->ownLen(), true);
    }

    bool isInline() const { return refWordIsInline(word); }

    bool hasTag(UInt64 t) const { return word != 0 && !isInline() && (word & TAG_MASK) == t; }
    bool isBatch() const { return hasTag(TAG_BATCH); }
    bool isRun() const { return hasTag(TAG_RUN); }
    bool isChain() const { return hasTag(TAG_CHAIN); }
    bool isCount() const { return hasTag(TAG_COUNT); }
    bool isFill() const { return hasTag(TAG_FILL) || hasTag(TAG_FILL_H); }

    /// The count field as stored: exact for runs, saturating for batches and chains.
    UInt32 countField() const { return static_cast<UInt32>((word >> COUNT_SHIFT) & COUNT_SAT); }

    UInt64 countItems() const
    {
        chassert(isCount());
        return word >> 4;
    }
    bool hasPrev() const
    {
        chassert(isCount());
        return (word >> 3) & 1;
    }
    void addItem()
    {
        chassert(isCount());
        word += 1ull << 4;
    }

    UInt64 * fillCursor() /// NOLINT(readability-make-member-function-const)
    {
        chassert(isFill());
        return reinterpret_cast<UInt64 *>(word & NODE_PTR_MASK); /// NOLINT(performance-no-int-to-ptr)
    }
    UInt32 fillPlaced() const
    {
        chassert(isFill());
        return countField();
    }
    bool fillHasHeader() const
    {
        chassert(isFill());
        return (word & TAG_MASK) == TAG_FILL_H;
    }

    const Batch * asBatch() const
    {
        chassert(isBatch());
        return reinterpret_cast<const Batch *>(word & PTR_MASK); /// NOLINT(performance-no-int-to-ptr)
    }

    /// Not const on purpose: a const-qualified version returning a mutable `Batch *` would leak
    /// mutable access from a const list and could not coexist with the const overload above
    /// (overloads cannot differ only in the return type).
    Batch * asBatch() /// NOLINT(readability-make-member-function-const)
    {
        chassert(isBatch());
        return reinterpret_cast<Batch *>(word & PTR_MASK); /// NOLINT(performance-no-int-to-ptr)
    }

    /// The refs of a run; their number is `countField()`.
    const UInt64 * runRefs() const
    {
        chassert(isRun());
        return reinterpret_cast<const UInt64 *>(word & NODE_PTR_MASK); /// NOLINT(performance-no-int-to-ptr)
    }

    const RangeHeader * chainHeader() const
    {
        chassert(isChain());
        return reinterpret_cast<const RangeHeader *>(word & NODE_PTR_MASK); /// NOLINT(performance-no-int-to-ptr)
    }

    /// Total rows for this key, load-free unless the count saturated. Wider than the in-word
    /// counter, because past saturation it comes from `Batch::total_rows` (56 bits) or a chain header's total.
    size_t rows() const
    {
        if (isInline())
            return 1;
        const UInt32 count = countField();
        if (count != COUNT_SAT)
            return count;
        switch (word & TAG_MASK)
        {
            case TAG_CHAIN: return chainHeader()->total();
            case TAG_BATCH: return asBatch()->total_rows;
            default: chassert(false && "a build-time word reached a reader"); return 0;
        }
    }

    /// Encoded ref word of the first-inserted row of the key (any-row semantics, e.g. RightAny on MapsAll).
    UInt64 firstWord() const
    {
        if (isInline())
            return word;
        switch (word & TAG_MASK)
        {
            case TAG_RUN:
                return runRefs()[0];
            case TAG_CHAIN: {
                UInt64 pending = chainHeader()->prev;
                while (RangeHeader::prevHasHeader(pending))
                    pending = reinterpret_cast<const RangeHeader *>(RangeHeader::prevPtr(pending))->prev;
                return RangeHeader::prevPtr(pending)[0];
            }
            default:
                return asBatch()->refs[0];
        }
    }

    void setRange(UInt64 start_word, size_t rows_, Arena & pool)
    {
        chassert(refWordIsInline(start_word));

        /// The run has to end inside the block it starts in - see `throwRowRefRangeOutOfRange`.
        const size_t start_row = refWordRowNo(start_word);
        if (start_row + rows_ > static_cast<size_t>(std::numeric_limits<UInt32>::max()) + 1) [[unlikely]]
            throwRowRefRangeOutOfRange(start_row, rows_);

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

            if ((list.word & TAG_MASK) != TAG_BATCH) [[unlikely]]
            {
                moveTo(startRanges(list.word));
                return;
            }

            const Batch * b = list.asBatch();
            if (b->is_range)
            {
                range_word = b->refs[0];
                range_remaining = b->total_rows;
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
            if (cur) /// run mode - the hot path: only cur/run_end and one next pointer are live here
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
                    else if (pending != 0) [[unlikely]]
                        moveTo(nextRange(pending));
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
        /// A run of a `TAG_RUN` / `TAG_CHAIN` word and the previous-range word that follows it.
        struct RangeCursor
        {
            const UInt64 * cur = nullptr;
            const UInt64 * run_end = nullptr;
            UInt64 pending = 0;
        };

        /// Run and chain layouts are decoded out of line and returned by value so the `Batch` path
        /// stays as small as it was without the tags. Callers that iterate one key per map cell (the
        /// non-joined fillers) keep inlining it. An escaping call would force the iterator onto the stack.
        static NO_INLINE RangeCursor startRanges(UInt64 word)
        {
            const RowRefList list = fromWord(word);
            switch (word & TAG_MASK)
            {
                case TAG_RUN:
                {
                    const UInt64 * refs = list.runRefs();
                    return {refs, refs + list.countField(), 0};
                }
                case TAG_CHAIN:
                {
                    const RangeHeader * header = list.chainHeader();
                    return {header->refs(), header->refs() + header->ownLen(), header->prev};
                }
                default:
                    chassert(false && "a build-time word reached a reader");
                    return {};
            }
        }

        /// The range before the one just exhausted; `pending` is the exhausted range's header's `prev`.
        static NO_INLINE RangeCursor nextRange(UInt64 pending)
        {
            if (RangeHeader::prevHasHeader(pending))
            {
                const auto * header = reinterpret_cast<const RangeHeader *>(RangeHeader::prevPtr(pending));
                return {header->refs(), header->refs() + header->ownLen(), header->prev};
            }
            const UInt64 * refs = RangeHeader::prevPtr(pending);
            return {refs, refs + RangeHeader::prevLen(pending), 0};
        }

        void moveTo(RangeCursor range)
        {
            cur = range.cur;
            run_end = range.run_end;
            pending = range.pending;
        }

        /// Run mode: `cur` walks the current contiguous run bounded by `run_end`. For a `Batch` chain
        /// `next_node` is the next overflow node (newest-first); for a range chain `pending` is the
        /// header's previous-range word (0 when none). `cur == nullptr` means range mode or done.
        const UInt64 * cur = nullptr;
        const UInt64 * run_end = nullptr;
        const Batch * next_node = nullptr;
        UInt64 pending = 0;
        /// Range mode: `range_word` is the current ref, `range_remaining` the count, as wide as
        /// `rows` because `Batch::total_rows` is 56 bits and a narrower counter would stop early.
        UInt64 range_word = 0;
        size_t range_remaining = 0;
    };

    ForwardIterator begin() const { return ForwardIterator(*this); }
    std::default_sentinel_t end() const { return {}; } /// NOLINT(readability-convert-member-functions-to-static)

private:
    /// An arena pointer that fits bits 47..3: below the 48-bit boundary and 8-aligned, so the tag
    /// bits are free. Both violations are fatal, not recoverable.
    static UInt64 checkedNodePointer(const void * p)
    {
        const UInt64 ptr = reinterpret_cast<UInt64>(p);
        if (ptr & ~NODE_PTR_MASK) [[unlikely]]
            throwRowRefPointerTooLarge();
        return ptr;
    }

    /// Repoint `word` at `b` with the saturating row count in bits 62..48. The cell-node pointer is
    /// stable across inserts, so this only rewrites the count bits of an already-resident cache line.
    /// Only the 48-bit bound is checked here, not the tag bits: a `Batch` is 8-aligned (see the
    /// `static_assert` below), so its pointer's tag bits are zero by construction. Checking them
    /// would keep the old word live across `insert` for nothing (`insert` is the `HashJoin` build loop).
    void setListWord(Batch * b, UInt64 total_rows_)
    {
        const UInt64 ptr = reinterpret_cast<UInt64>(b);
        if (ptr & ~PTR_MASK) [[unlikely]]
            throwRowRefPointerTooLarge();
        const UInt64 count = total_rows_ < COUNT_SAT ? total_rows_ : COUNT_SAT;
        word = ptr | (count << COUNT_SHIFT) | TAG_BATCH;
    }
};

static_assert(sizeof(RowRefList) == 8, "RowRefList must stay 8 bytes: it is the hash map cell payload");
static_assert(sizeof(RowRefList::Batch) == 64, "RowRefList::Batch must stay one cache line");
static_assert(alignof(RowRefList::Batch) == 8, "Batch pointers must leave the three tag bits free");
static_assert(sizeof(RowRefList::RangeHeader) == 16 && alignof(RowRefList::RangeHeader) == 8);

/// Number of rows an encoded cell / LazyOutput word represents (inline ref = 1, list = its count,
/// range = its length), without spelling out a RowRefList at the call site. A zero word yields 0.
inline size_t refWordRows(UInt64 word)
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

/// The run and chain layouts (`TAG_RUN`, `TAG_CHAIN`) of `forEachRef`, out of line: the standard
/// `HashJoin` never publishes them. Keeping their decode out of the emit loop's inlined body lets
/// that loop close on one conditional back-edge.
template <typename F>
NO_INLINE void forEachRefOfRangeChain(UInt64 word, F & f)
{
    for (const UInt64 ref_word : refsOf(word))
        f(ref_word);
}

/// Applies `f` to every encoded ref of a non-zero cell / LazyOutput word, in `ForwardIterator` order.
/// The inline word, the `Batch` layouts, and the range node are walked with plain pointer ranges:
/// cell run `refs[0 .. size)`, or `refs[0 .. SLOTS)` plus the overflow chain newest-first when chained.
/// An emit loop that inlines this has one exit per run instead of the iterator's run-end switch.
/// Only a run or chain word takes the iterator, out of line.
template <typename F>
ALWAYS_INLINE void forEachRef(UInt64 word, F && f)
{
    const RowRefList list = RowRefList::fromWord(word);
    if (list.isInline())
    {
        f(word);
        return;
    }
    if (!list.isBatch()) [[unlikely]]
    {
        forEachRefOfRangeChain(word, f);
        return;
    }
    const RowRefList::Batch * b = list.asBatch();
    if (b->is_range)
    {
        UInt64 ref_word = b->refs[0];
        for (UInt64 n = b->total_rows; n != 0; --n, ++ref_word)
            f(ref_word);
        return;
    }
    const bool chained = b->size != b->total_rows;
    const UInt64 * cur = &b->refs[0];
    for (const UInt64 * end = cur + (chained ? RowRefList::Batch::SLOTS : static_cast<size_t>(b->size)); cur != end; ++cur)
        f(*cur);
    if (!chained)
        return;
    for (const auto * node = reinterpret_cast<const RowRefList::Batch *>(b->refs[RowRefList::Batch::SLOTS]); node != nullptr; /// NOLINT(performance-no-int-to-ptr)
         node = reinterpret_cast<const RowRefList::Batch *>(node->refs[0])) /// NOLINT(performance-no-int-to-ptr)
    {
        cur = &node->refs[1];
        for (const UInt64 * end = cur + node->size; cur != end; ++cur)
            f(*cur);
    }
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

/// Shape of an encoded ref-word sequence as handed from an emit producer to an emit consumer:
///   Flat   - exactly one word per output row: 0 is a default row, anything else an inline
///            (block_no, row_no) ref.
///   Lists  - a word may be a `RowRefList` list word standing for every row of one key.
///   Ranges - a word may be a range node (the reranged "sorted" build): a consumer emits one range
///            operation per word and never flattens it, so sorted output stays O(ranges).
enum class RefWordShape : UInt8
{
    Flat,
    Lists,
    Ranges,
};

/// One selection of right-table rows to emit: the ref words, their shape, and the number of output
/// rows they expand to (a zero word counting as one default row). This is what the emit producers -
/// the lazy-output builders and the not-joined scans - hand to the emit kernels.
struct RefWordSelection
{
    const UInt64 * begin = nullptr;
    const UInt64 * end = nullptr;
    size_t rows = 0;
    RefWordShape shape = RefWordShape::Flat;
};

struct GatherNode;

/// One level of a gather source descriptor: the `ColumnPlanes` of a stored column, per block,
/// mirroring the column's own nesting. Every `*_by_block` vector below is indexed by `block_no`; a
/// cleared block's entry stays null and is never dereferenced, as no live ref points at it. The
/// pointers hold only for the current emit-table generation.
struct GatherNode
{
    using Kind = ColumnPlanes::Shape;

    Kind kind = Kind::Fixed;
    size_t stride = 0;
    /// Block `b`'s `ColumnPlanes::data` and `aux`; for `Rows`, `data_by_block[b]` is the source column
    /// itself. Sized by the data, so they use the throwing memory tracker like
    /// `StoredColumnsIndex::blocks` does, and a huge build fails the query rather than the process.
    VectorWithMemoryTracking<const void *> data_by_block;
    VectorWithMemoryTracking<const void *> aux_by_block;
    std::vector<GatherNode> children;
    /// `Variant` only: block `b`'s local discriminator `d` is global `[b * children.size() + d]`.
    VectorWithMemoryTracking<UInt8> local_to_global_by_block;
    /// `Rows` only: the output type whose `insertDefaultInto` writes an unmatched row. Null below a
    /// `Nullable`, where `insertDefault` fills the nested column.
    DataTypePtr type;
    /// `Fixed` only: the `stride` bytes an unmatched row writes. Held as data rather than derived,
    /// because a fixed-width default is not always bitwise zero - an `Enum`'s is its first value.
    std::vector<char> default_pattern;
    /// The concrete column class of the first resolved block, which decided the shape. Every later
    /// block has to match, so a mismatch is a broken plan rather than a case to handle.
    const std::type_info * column_type = nullptr;
};

/// Per-block row indirection of a `ColumnReplicated` stored column: `row' = indexes[row]`, read at
/// `index_width` bytes. A null `indexes_data` means the block stores the column plainly.
struct GatherRowRemap
{
    const void * indexes_data = nullptr;
    UInt8 index_width = 0; /// bytes per index: 1, 2, 4 or 8
};

/// The source of one output column, as handed to the emit path.
struct GatherColumn
{
    const GatherNode * node = nullptr;
    /// Indexed by block_no; null when no block stores this column as `ColumnReplicated`.
    const GatherRowRemap * remap_by_block = nullptr;
};

/// One column an emit table is asked for. The destination type is part of the request: it decides
/// what an unmatched row writes, and its column class has to be the stored one.
struct EmitColumnRequest
{
    size_t position = 0;
    DataTypePtr type;
};

/// Maps `block_no` (the high half of RowRef) to the stored block.
/// Appended under mutex from every build thread, so block numbers are unique across the join.
/// Read lock-free at probe/emit time, which is safe because probing starts only after
/// the build phase is finished.
///
/// On top of the block map it builds the emit table: a resolved gather source per requested output
/// column, whose raw plane pointers let the kernels skip the stored block and its column vector.
/// `resolveEmitColumns` builds the requested positions lazily under `mutex` and hands back the per-column
/// descriptors. Positions already built for the current generation are reused. The table is keyed by
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
    /// The resolved gather source for one output (saved-block) column position.
    struct EmitColumn
    {
        /// The type this entry was resolved for. It picks the defaults, so reusing the entry under
        /// another type would write the wrong ones.
        DataTypePtr request_type;
        /// The raw bases hold only until the generation changes, so mutating a stored column's
        /// buffer in place requires bumping it.
        GatherNode gather_root;
        /// Indexed by block_no; filled only when at least one block stores the column as `ColumnReplicated`
        /// (identity entries for the blocks that do not).
        VectorWithMemoryTracking<GatherRowRemap> gather_remap_by_block;
        /// Owns the shape when no live block resolved one - see `resolveEmitColumns`.
        MutableColumnPtr shape_prototype;
    };

    /// Registers a stored block, returns its block_no. Throws when the 2^31 limit
    /// (RowRef::BLOCK_NO_MASK, the MSB is the inline flag) is exceeded.
    UInt32 add(const StoredBlock * block);

    /// Protection against dangling pointers: a popped/replaced block keeps its slot,
    /// but the slot is nulled so that a stale ref fails loudly instead of reading freed memory.
    void clearEntry(UInt32 block_no);

    /// Raw pointer for hot decode loops. Must not be called before the build phase is finished.
    const StoredBlock * const * blocksData() const { return blocks.data(); }

    /// Per-block row store base pointers (block_no -> RowDataStore*). A block without a row store stores nullptr.
    const RowDataStore * const * rowStoresData() const { return row_stores.data(); }

    /// Number of registered blocks. Must not be called before the build phase is finished.
    size_t size() const { return blocks.size(); }

    const StoredBlock * at(UInt32 block_no) const
    {
        chassert(block_no < blocks.size());
        /// A cleared entry (see `clearEntry`) must never be reached: no refs to such a block exist.
        /// In debug builds a stale ref trips this assertion; in release builds it dereferences
        /// nullptr at a deterministic, near-zero address instead of reading freed memory.
        chassert(blocks[block_no] != nullptr);
        return blocks[block_no];
    }

    /// Resolve the emit table for one probe's output columns, building the positions not yet built
    /// for this generation and dropping the table first if the blocks changed. `out_gather` is
    /// indexed by stored-block column position, and a position not requested stays empty. Asking for
    /// one under a second type throws, because rebuilding its entry would invalidate the pointers an
    /// earlier caller still holds. Called once per probe batch, never in the per-row loop.
    void resolveEmitColumns(
        size_t saved_columns_count, const std::vector<EmitColumnRequest> & requests, std::vector<GatherColumn> & out_gather);

    /// Invalidate the emit table after the stored columns are replaced in place (e.g. shrinkStoredBlocksToFit
    /// `cloneResized`), which would otherwise leave the cached `const IColumn *` dangling. Bumps the generation.
    void invalidateEmitTable();

private:
    mutable std::mutex mutex;
    /// One entry per stored block (data-proportional): use the throwing memory tracker so a huge build
    /// fails the query at the limit instead of letting the process get OOM-killed.
    VectorWithMemoryTracking<const StoredBlock *> blocks;
    /// The per-block row store (or nullptr).
    VectorWithMemoryTracking<const RowDataStore *> row_stores;

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
