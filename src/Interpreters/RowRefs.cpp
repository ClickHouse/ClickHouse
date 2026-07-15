#include <Interpreters/RowRefs.h>

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnVector.h>
#include <Columns/IColumn.h>
#include <Common/assert_cast.h>
#include <Core/Joins.h>
#include <DataTypes/IDataType.h>
#include <base/types.h>
#include <Common/RadixSort.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_TYPE_OF_FIELD;
    extern const int LOGICAL_ERROR;
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

    void insert(const IColumn & asof_column, const Columns * columns, size_t row_num) override
    {
        using ColumnType = ColumnVectorOrDecimal<TKey>;
        const auto & column = assert_cast<const ColumnType &>(asof_column);
        TKey key = column.getElement(row_num);

        assert(!sorted.load(std::memory_order_acquire));

        entries.emplace_back(key, static_cast<UInt32>(row_refs.size()));
        row_refs.emplace_back(RowRef(columns, row_num));
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

    RowRef * findAsof(const IColumn & asof_column, size_t row_num) override
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
