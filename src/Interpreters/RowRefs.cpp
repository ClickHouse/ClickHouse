#include <Interpreters/RowRefs.h>

#if defined(__FILC__)
#include <stdfil.h>
#endif

#include <Interpreters/HashJoin/ScatteredBlock.h>
#include <Columns/ColumnDecimal.h>
#include <Common/Exception.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnReplicated.h>
#include <Columns/IColumn.h>
#include <Common/FailPoint.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <Core/Joins.h>
#include <DataTypes/IDataType.h>
#include <base/types.h>
#include <Common/RadixSort.h>
#include <Interpreters/RowDataStore.h>

#include <mutex>


namespace DB
{

#if defined(__FILC__)
namespace
{
zptrtable * rowRefBatchPointerTable()
{
    static zptrtable * const table = zptrtable_new();
    return table;
}
}

UInt64 encodeRowRefBatchPointer(void * pointer)
{
    return zptrtable_encode(rowRefBatchPointerTable(), pointer);
}

void * decodeRowRefBatchPointer(UInt64 encoded_pointer)
{
    return zptrtable_decode(rowRefBatchPointerTable(), encoded_pointer);
}
#endif

namespace ErrorCodes
{
    extern const int BAD_TYPE_OF_FIELD;
    extern const int FAULT_INJECTED;
    extern const int LOGICAL_ERROR;
}

namespace FailPoints
{
extern const char stored_columns_index_throw_on_add[];
}

void RowRefList::setRange(UInt64 start_word, size_t rows_, Arena & pool)
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

void RowRefList::insert(UInt64 ref_word, Arena & pool)
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
            b->refs[Batch::SLOTS] = batchPointerToWord(n);
            b->size = MAX_LOCAL - 1; /// refs[0] + (SLOTS-1) local refs remain
        }
    }
    else /// chained cell node: append into the newest overflow node
    {
        const UInt64 newest_word = b->refs[Batch::SLOTS];
        auto * newest = batchPointerFromWord(newest_word);
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
            n->refs[0] = newest_word; /// next-older node
            n->refs[1] = ref_word;
            b->refs[Batch::SLOTS] = batchPointerToWord(n);
        }
    }

    b->total_rows = new_total;
    setListCount(new_total);
}

namespace
{

/// maps enum values to types
template <typename F>
void callWithType(TypeIndex type, F && f)
{
    WhichDataType which(type);

#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return f(TYPE());

    FOR_NUMERIC_TYPES(DISPATCH)
    DISPATCH(Decimal32)
    DISPATCH(Decimal64)
    DISPATCH(Decimal128)
    DISPATCH(Decimal256)
    DISPATCH(DateTime64)
#undef DISPATCH

    UNREACHABLE();
}

template <typename TKey, ASOFJoinInequality inequality>
class SortedLookupVector : public SortedLookupVectorBase
{
    struct Entry
    {
        TKey value;
        uint32_t row_ref_index;

        Entry() = delete;
        Entry(TKey value_, uint32_t row_ref_index_)
            : value(value_)
            , row_ref_index(row_ref_index_)
        { }

    };

    struct LessEntryOperator
    {
        ALWAYS_INLINE bool operator()(const Entry & lhs, const Entry & rhs) const
        {
            return lhs.value < rhs.value;
        }
    };

    struct GreaterEntryOperator
    {
        ALWAYS_INLINE bool operator()(const Entry & lhs, const Entry & rhs) const
        {
            return lhs.value > rhs.value;
        }
    };


public:
    using Entries = PODArrayWithStackMemory<Entry, sizeof(Entry)>;
    using RowRefs = PODArrayWithStackMemory<RowRef, sizeof(RowRef)>;

    static constexpr bool is_descending = (inequality == ASOFJoinInequality::Greater || inequality == ASOFJoinInequality::GreaterOrEquals);
    static constexpr bool is_strict = (inequality == ASOFJoinInequality::Less) || (inequality == ASOFJoinInequality::Greater);

    void insert(const IColumn & asof_column, UInt32 block_no, size_t row_num) override
    {
        using ColumnType = ColumnVectorOrDecimal<TKey>;
        const auto & column = assert_cast<const ColumnType &>(asof_column);
        TKey key = column.getElement(row_num);

        chassert(!sorted.load(std::memory_order_acquire));

        entries.emplace_back(key, static_cast<UInt32>(row_refs.size()));
        row_refs.emplace_back(RowRef(block_no, row_num));
    }

    /// Unrolled version of upper_bound and lower_bound
    /// Loosely based on https://academy.realm.io/posts/how-we-beat-cpp-stl-binary-search/
    /// In the future it'd interesting to replace it with a B+Tree Layout as described
    /// at https://en.algorithmica.org/hpc/data-structures/s-tree/
    size_t boundSearch(TKey value)
    {
        size_t size = entries.size();
        size_t low = 0;

        /// This is a single binary search iteration as a macro to unroll. Takes into account the inequality:
        /// is_strict -> Equal values are not requested
        /// is_descending -> The vector is sorted in reverse (for greater or greaterOrEquals)
#define BOUND_ITERATION \
    { \
        size_t half = size / 2; \
        size_t other_half = size - half; \
        size_t probe = low + half; \
        size_t other_low = low + other_half; \
        TKey & v = entries[probe].value; \
        size = half; \
        if constexpr (is_descending) \
        { \
            if constexpr (is_strict) \
                low = value <= v ? other_low : low; \
            else \
                low = value < v ? other_low : low; \
        } \
        else \
        { \
            if constexpr (is_strict) \
                low = value >= v ? other_low : low; \
            else \
                low = value > v ? other_low : low; \
        } \
    }

        while (size >= 8)
        {
            BOUND_ITERATION
            BOUND_ITERATION
            BOUND_ITERATION
        }

        while (size > 0)
        {
            BOUND_ITERATION
        }

#undef BOUND_ITERATION
        return low;
    }

    const RowRef * findAsof(const IColumn & asof_column, size_t row_num) override
    {
        sort();

        using ColumnType = ColumnVectorOrDecimal<TKey>;
        const auto & column = assert_cast<const ColumnType &>(asof_column);
        TKey k = column.getElement(row_num);

        size_t pos = boundSearch(k);
        if (pos != entries.size())
        {
            size_t row_ref_index = entries[pos].row_ref_index;
            return &row_refs[row_ref_index];
        }

        return nullptr;
    }

private:
    std::atomic<bool> sorted = false;
    mutable std::mutex lock;
    Entries entries;
    RowRefs row_refs;

    // Double checked locking with SC atomics works in C++
    // https://preshing.com/20130930/double-checked-locking-is-fixed-in-cpp11/
    // The first thread that calls one of the lookup methods sorts the data
    // After calling the first lookup method it is no longer allowed to insert any data
    // the array becomes immutable
    void sort()
    {
        if (sorted.load(std::memory_order_acquire))
            return;

        std::lock_guard<std::mutex> l(lock);

        if (sorted.load(std::memory_order_relaxed))
            return;

        if constexpr (std::is_arithmetic_v<TKey> && !is_floating_point<TKey>)
        {
            if (likely(entries.size() > 256))
            {
                struct RadixSortTraits : RadixSortNumTraits<TKey>
                {
                    using Element = Entry;
                    using Result = Element;

                    static TKey & extractKey(Element & elem) { return elem.value; }
                    static Result extractResult(Element & elem) { return elem; }
                };

                RadixSort<RadixSortTraits>::executeLSDWithTrySort(entries.data(), entries.size(), is_descending /*reverse*/);
                sorted.store(true, std::memory_order_release);
                return;
            }
        }

        if constexpr (is_descending)
            ::sort(entries.begin(), entries.end(), GreaterEntryOperator());
        else
            ::sort(entries.begin(), entries.end(), LessEntryOperator());

        sorted.store(true, std::memory_order_release);
    }
};

}

StoredBlock::StoredBlock(Columns columns_, RowDataStorePtr row_store_)
    : columns(std::move(columns_)), row_store(std::move(row_store_))
{
    rebuildReplicatedColumns();
}

StoredBlock::StoredBlock(Columns columns_, detail::Selector selector_, RowDataStorePtr row_store_)
    : columns(std::move(columns_)), selector(std::move(selector_)), row_store(std::move(row_store_))
{
    rebuildReplicatedColumns();
}

void StoredBlock::rebuildReplicatedColumns()
{
    replicated_columns.resize(columns.size());
    for (size_t i = 0; i != columns.size(); ++i)
        replicated_columns[i] = typeid_cast<const ColumnReplicated *>(columns[i].get());
}

bool StoredBlock::hasRowStore() const { return row_store != nullptr; }

size_t StoredBlock::blockRows() const
{
    if (!columns.empty())
        return columns.at(0)->size();
    return hasRowStore() ? row_store->size() : 0;
}

size_t StoredBlock::allocatedBytes() const
{
    size_t row_nums = blockRows();
    if (row_nums == 0)
        return 0;

    size_t allocated_bytes = 0;
    if (hasRowStore())
        allocated_bytes = row_store->allocatedBytes();

    for (const auto & column : columns)
        allocated_bytes += column->allocatedBytes();

    return allocated_bytes * selector.size() / row_nums;
}

void throwRowRefPointerTooLarge()
{
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "Arena pointer does not fit in 48 bits; RowRefList pointer+count packing is invalid on this platform");
}

void throwRowRefOutOfRange(size_t block_no, size_t row_no)
{
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "RowRef out of range: block_no {} must fit in 31 bits and row_no {} in 32 bits",
        block_no, row_no);
}

UInt32 StoredColumnsIndex::add(const StoredBlock * block)
{
    std::lock_guard guard(mutex);
    if (blocks.size() > RowRef::BLOCK_NO_MASK)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Too many stored blocks in HashJoin: {}", blocks.size());
    fiu_do_on(FailPoints::stored_columns_index_throw_on_add,
    {
        throw Exception(ErrorCodes::FAULT_INJECTED, "Injected failure while registering a stored block");
    });
    /// `blocks` and `row_stores` are indexed by the same block number, so grow both before either is
    /// appended to: both appends are then non-allocating and cannot leave the two different lengths.
    /// The target doubles the shared size, so growth is amortised constant per block, and a reserve
    /// that threw after growing only one vector is retried at the same target.
    chassert(blocks.size() == row_stores.size());
    if (blocks.size() == blocks.capacity() || row_stores.size() == row_stores.capacity())
    {
        const size_t new_capacity = 2 * blocks.size() + 1;
        blocks.reserve(new_capacity);
        row_stores.reserve(new_capacity);
    }
    blocks.push_back(block);
    row_stores.push_back(block->row_store.get());
    ++blocks_generation; /// Invalidate any previously built emit table (StorageJoin can insert between joins).
    return static_cast<UInt32>(blocks.size() - 1);
}

void StoredColumnsIndex::clearEntry(UInt32 block_no)
{
    std::lock_guard guard(mutex);
    chassert(block_no < blocks.size());
    blocks[block_no] = nullptr;
    row_stores[block_no] = nullptr;
    ++blocks_generation;
}

void StoredColumnsIndex::invalidateEmitTable()
{
    std::lock_guard guard(mutex);
    ++blocks_generation;
}

void StoredColumnsIndex::resolveEmitColumns(
    size_t saved_columns_count,
    const std::vector<size_t> & positions,
    std::vector<const IColumn * const *> & out_columns,
    std::vector<const ColumnReplicated * const *> & out_replicated)
{
    std::lock_guard guard(mutex);

    if (emit_generation != blocks_generation)
    {
        /// Blocks changed since the table was last built: every cached `const IColumn *` is stale. Drop
        /// the whole table; positions other queries still need will be rebuilt when they ask for them.
        emit_columns.clear();
        emit_columns.resize(saved_columns_count); /// value-initializes to null unique_ptrs (no copy)
        emit_generation = blocks_generation;
    }
    else if (emit_columns.size() < saved_columns_count)
    {
        emit_columns.resize(saved_columns_count); /// defensive; saved_columns_count is fixed per join
    }

    const size_t num_blocks = blocks.size();
    out_columns.assign(saved_columns_count, nullptr);
    out_replicated.assign(saved_columns_count, nullptr);
    for (size_t pos : positions)
    {
        chassert(pos < saved_columns_count);
        if (!emit_columns[pos]) /// not built yet for this generation: build this requested position
        {
            auto emit_column = std::make_unique<EmitColumn>();
            emit_column->by_block.resize(num_blocks);
            emit_column->repl_by_block.resize(num_blocks);
            for (size_t b = 0; b < num_blocks; ++b)
            {
                const StoredBlock * block = blocks[b];
                /// A cleared/popped slot keeps a null entry: no live ref points to it (mirrors `at()`).
                emit_column->by_block[b] = block ? block->columns[pos].get() : nullptr;
                emit_column->repl_by_block[b] = block ? block->replicated_columns[pos] : nullptr;
            }
            emit_columns[pos] = std::move(emit_column);
        }
        const EmitColumn & emit_column = *emit_columns[pos];
        out_columns[pos] = emit_column.by_block.data();
        out_replicated[pos] = emit_column.repl_by_block.data();
    }
}

AsofRowRefs createAsofRowRef(TypeIndex type, ASOFJoinInequality inequality)
{
    AsofRowRefs result;
    auto call = [&](const auto & t)
    {
        using T = std::decay_t<decltype(t)>;
        switch (inequality)
        {
            case ASOFJoinInequality::LessOrEquals:
                result = std::make_unique<SortedLookupVector<T, ASOFJoinInequality::LessOrEquals>>();
                break;
            case ASOFJoinInequality::Less:
                result = std::make_unique<SortedLookupVector<T, ASOFJoinInequality::Less>>();
                break;
            case ASOFJoinInequality::GreaterOrEquals:
                result = std::make_unique<SortedLookupVector<T, ASOFJoinInequality::GreaterOrEquals>>();
                break;
            case ASOFJoinInequality::Greater:
                result = std::make_unique<SortedLookupVector<T, ASOFJoinInequality::Greater>>();
                break;
            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid ASOF Join order");
        }
    };

    callWithType(type, call);
    return result;
}

std::optional<TypeIndex> SortedLookupVectorBase::getTypeSize(const IColumn & asof_column, size_t & size)
{
    WhichDataType which(asof_column.getDataType());
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
    { \
        size = sizeof(TYPE); \
        return asof_column.getDataType(); \
    }


    FOR_NUMERIC_TYPES(DISPATCH)
    DISPATCH(Decimal32)
    DISPATCH(Decimal64)
    DISPATCH(Decimal128)
    DISPATCH(Decimal256)
    DISPATCH(DateTime64)
#undef DISPATCH

    throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "ASOF join not supported for type: {}", std::string(asof_column.getFamilyName()));
}

}
