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
///   - a `BuildRefList::Blob *` (user-space pointers have bit 63 clear on x86-64/aarch64).
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

/// Helpers for the encoded 64-bit ref words stored in LazyOutput / BuildRefList batches.
inline bool refWordIsInline(UInt64 word) { return word & BuildRef::SINGLETON_WORD_FLAG; }
inline UInt32 refWordBlockNo(UInt64 word) { return static_cast<UInt32>(word >> 32) & BuildRef::BLOCK_NO_MASK; }
inline UInt32 refWordRowNo(UInt64 word) { return static_cast<UInt32>(word); }

/// Mapped value of MapsAll join hash maps (ALL JOINs / non-unique keys): a tagged 8-byte union.
///   - bit 63 set: the key has exactly one row so far; the word IS the encoded BuildRef.
///   - bit 63 clear, non-zero: pointer to an arena-allocated Blob with the row count and refs.
/// The Blob is allocated only when the first duplicate of a key arrives, so ALL-join cells are
/// as small as ANY-join cells for every key type, and unique keys never touch the arena.
struct BuildRefList
{
    /// Portion of encoded ref words, 8 * (MAX_SIZE + 1) bytes sized.
    struct Batch
    {
        static constexpr size_t MAX_SIZE = 7; /// Adequate values are 3, 7, 15, 31.

        UInt32 size = 0;
        Batch * next;
        UInt64 refs[MAX_SIZE]; /// the occupied prefix is initialized by insert

        explicit Batch(Batch * parent) /// NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init)
            : next(parent)
        {}

        bool full() const { return size == MAX_SIZE; }

        Batch * insert(UInt64 ref_word, Arena & pool)
        {
            if (full())
            {
                auto * batch = pool.alloc<Batch>();
                *batch = Batch(this);
                batch->insert(ref_word, pool);
                return batch;
            }

            refs[size] = ref_word;
            ++size;
            return this;
        }
    };

    /// Out-of-line list head for keys with more than one row. The total row count is kept
    /// at offset 0. If `is_range` is set, the value represents `rows` CONSECUTIVE rows
    /// starting at `head` (produced by the rerange/"sorted" optimization) and `next` is null.
    struct Blob
    {
        UInt32 rows = 0;
        UInt32 is_range = 0;
        UInt64 head = 0; /// encoded BuildRef word of the first row (or the range start)
        Batch * next = nullptr;

        void assertIsRange() const
        {
            chassert(rows >= 1, "BuildRefList range should have at least one row");
            chassert(is_range, "BuildRefList blob does not represent a range");
            chassert(next == nullptr, "When BuildRefList represents a range, it should not have batches");
        }
    };

    UInt64 word = 0;

    BuildRefList() = default;
    BuildRefList(size_t block_no_, size_t row_no_) : word(BuildRef(block_no_, row_no_).word()) {}

    bool isSingleton() const { return refWordIsInline(word); }

    const Blob * asBlob() const
    {
        chassert(word != 0 && !isSingleton());
        return reinterpret_cast<const Blob *>(word); /// NOLINT(performance-no-int-to-ptr)
    }

    Blob * asBlob() /// NOLINT(readability-make-member-function-const)
    {
        chassert(word != 0 && !isSingleton());
        return reinterpret_cast<Blob *>(word); /// NOLINT(performance-no-int-to-ptr)
    }

    /// Total number of rows for this key.
    UInt32 rows() const { return isSingleton() ? 1 : asBlob()->rows; }

    /// Encoded ref word of the first row (any-row semantics, e.g. RightAny on MapsAll).
    UInt64 firstWord() const { return isSingleton() ? word : asBlob()->head; }

    void setRange(UInt64 start_word, size_t rows_, Arena & pool)
    {
        chassert(refWordIsInline(start_word));
        auto * blob = pool.alloc<Blob>();
        *blob = Blob{};
        blob->rows = static_cast<UInt32>(rows_);
        blob->is_range = 1;
        blob->head = start_word;
        setBlob(blob);
    }

    /// Insert one more row for this key. Preserves the iteration order of the old
    /// RowRefList: head first, then the newest batch, then progressively older batches.
    void insert(UInt64 ref_word, Arena & pool)
    {
        chassert(refWordIsInline(ref_word));

        /// Init the first element when starting from a default-constructed value.
        if (word == 0)
        {
            word = ref_word;
            return;
        }

        if (isSingleton())
        {
            auto * blob = pool.alloc<Blob>();
            *blob = Blob{};
            blob->rows = 1;
            blob->head = word;
            setBlob(blob);
        }

        Blob * blob = asBlob();
        if (!blob->next)
        {
            blob->next = pool.alloc<Batch>();
            *blob->next = Batch(nullptr);
        }
        blob->next = blob->next->insert(ref_word, pool);
        ++blob->rows;
    }

    /// Iterates encoded ref words in the same order as the old RowRefList::ForwardIterator.
    /// Handles all three representations: singleton, list blob, range blob.
    class ForwardIterator
    {
    public:
        explicit ForwardIterator(const BuildRefList & list)
        {
            if (list.word == 0)
                return; /// empty (default-constructed) list

            if (list.isSingleton())
            {
                head = list.word;
                remaining_range = 1;
            }
            else
            {
                const Blob * blob = list.asBlob();
                head = blob->head;
                remaining_range = blob->is_range ? blob->rows : 1;
                batch = blob->next;
            }
        }

        UInt64 operator * () const
        {
            if (remaining_range)
                return head;
            return batch->refs[position];
        }

        void operator ++ ()
        {
            if (remaining_range)
            {
                --remaining_range;
                /// Consecutive rows of a range live in one block: only row_no advances.
                head += 1;
                return;
            }

            if (batch)
            {
                ++position;
                if (position >= batch->size)
                {
                    batch = batch->next;
                    position = 0;
                }
            }
        }

        bool ok() const { return remaining_range || batch; }

    private:
        UInt64 head = 0;
        UInt32 remaining_range = 0;
        const Batch * batch = nullptr;
        size_t position = 0;
    };

    ForwardIterator begin() const { return ForwardIterator(*this); }

private:
    void setBlob(Blob * blob)
    {
        const UInt64 ptr = reinterpret_cast<UInt64>(blob);
        chassert((ptr & BuildRef::SINGLETON_WORD_FLAG) == 0, "Arena pointer with MSB set cannot be tagged");
        word = ptr;
    }
};

static_assert(sizeof(BuildRefList) == 8, "BuildRefList must stay 8 bytes: it is the hash map cell payload");

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
