#include <Interpreters/RowRefs.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnVector.h>
#include <Columns/IColumn.h>
#include <Core/Joins.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/HashJoin/ScatteredBlock.h>
#include <Interpreters/HashJoin/gatherJoinOutputColumns.h>
#include <Interpreters/RowDataStore.h>
#include <base/types.h>
#include <Common/Exception.h>
#include <Common/RadixSort.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <limits>
#include <mutex>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_TYPE_OF_FIELD;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
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

void throwRowRefRangeOutOfRange(size_t row_no, size_t rows)
{
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "RowRefList range out of range: a run of {} rows from row_no {} does not end in the same block",
        rows, row_no);
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
    size_t saved_columns_count, const std::vector<EmitColumnRequest> & requests, std::vector<GatherColumn> & out_gather)
{
    std::lock_guard guard(mutex);

    if (emit_generation != blocks_generation)
    {
        /// Blocks changed since the table was last built: every cached plane pointer is stale. Drop
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
    out_gather.assign(saved_columns_count, {});
    for (const EmitColumnRequest & request : requests)
    {
        const size_t pos = request.position;
        chassert(pos < saved_columns_count);
        /// `resolveGatherNode` catches a differing shape. Two types of the same shape can still have
        /// different defaults - `Enum8` and `Int8` - and would otherwise share one `default_pattern`.
        if (emit_columns[pos] && !emit_columns[pos]->request_type->equals(*request.type))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Join emit column {} was resolved for type {} and is now asked for as {}",
                pos, emit_columns[pos]->request_type->getName(), request.type->getName());

        if (!emit_columns[pos]) /// not built yet for this generation: build this requested position
        {
            auto emit_column = std::make_unique<EmitColumn>();
            emit_column->request_type = request.type;
            bool any_replicated = false;
            std::vector<GatherRowRemap> remap(num_blocks);
            for (size_t b = 0; b < num_blocks; ++b)
            {
                /// A cleared/popped slot is skipped: no live ref points to it (mirrors `at()`).
                const StoredBlock * block = blocks[b];
                if (!block)
                    continue;

                const IColumn * column = block->columns[pos].get();
                /// `row' = indexes[row]` addresses the nested column, so the planes come from there
                /// and the remap entry carries the indexes plane.
                if (const ColumnReplicated * replicated = block->replicated_columns[pos])
                {
                    const IColumn & indexes = *replicated->getIndexes().getIndexes();
                    const size_t index_width = indexes.isFixedAndContiguous() ? indexes.sizeOfValueIfFixed() : 0;
                    if (index_width != 1 && index_width != 2 && index_width != 4 && index_width != 8)
                        throw Exception(
                            ErrorCodes::LOGICAL_ERROR, "Replicated join column has indexes of {} bytes", index_width);
                    const std::string_view indexes_raw = indexes.getRawData();
                    /// A column that delegates `getRawData` hands back a buffer that is not its own rows.
                    if (indexes_raw.size() != indexes.size() * index_width)
                        throw Exception(
                            ErrorCodes::LOGICAL_ERROR, "Replicated join column indexes do not hold exactly their own rows");
                    remap[b] = {.indexes_data = indexes_raw.data(), .index_width = static_cast<UInt8>(index_width)};
                    any_replicated = true;
                    column = replicated->getNestedColumn().get();
                    /// `remapFlatWord` puts `indexes[row]` back in a 32-bit row field, so the nested
                    /// column is under a block's limit. A row expanding to none is what exceeds it.
                    if (column->size() > std::numeric_limits<UInt32>::max())
                        throw Exception(
                            ErrorCodes::NOT_IMPLEMENTED,
                            "Too many deduplicated rows in right table block for HashJoin: {}",
                            column->size());
                }

                resolveGatherNode(emit_column->gather_root, request.type, *column, b, num_blocks);
            }
            /// With no live block to decide the shape - an empty right side, or one whose blocks
            /// were all cleared - the join still emits, every row a default, so a column of the
            /// output type stands in. It is kept alive to keep the plane pointers valid; no ref word
            /// can name a block, so none is read.
            if (!emit_column->gather_root.column_type)
            {
                emit_column->shape_prototype = request.type->createColumn();
                resolveGatherNode(emit_column->gather_root, request.type, *emit_column->shape_prototype, 0, 1);
            }
            if (any_replicated)
                emit_column->gather_remap_by_block = std::move(remap);
            emit_columns[pos] = std::move(emit_column);
        }
        const EmitColumn & emit_column = *emit_columns[pos];
        out_gather[pos]
            = {.node = &emit_column.gather_root,
               .remap_by_block = emit_column.gather_remap_by_block.empty() ? nullptr : emit_column.gather_remap_by_block.data()};
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
