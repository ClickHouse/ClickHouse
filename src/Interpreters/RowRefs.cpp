#include <Interpreters/RowRefs.h>

#include <AggregateFunctions/Helpers.h>
#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>
#include <base/types.h>


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

    __builtin_unreachable();
}

template <typename TKey, ASOF::Inequality inequality>
class SortedLookupVector : public SortedLookupVectorBase
{
public:
    struct Entry
    {
        /// We don't store a RowRef and instead keep it's members separately (and return a tuple) to reduce the memory usage.
        /// For example, for sizeof(T) == 4 => sizeof(Entry) == 16 (while before it would be 20). Then when you put it into a vector, the effect is even greater
        decltype(RowRef::block) block;
        decltype(RowRef::row_num) row_num;
        TKey asof_value;

        Entry() = delete;
        Entry(TKey v, const Block * b, size_t r) : block(b), row_num(r), asof_value(v) { }

        bool operator<(const Entry & other) const { return asof_value < other.asof_value; }
    };

    struct greaterEntryOperator
    {
        bool operator()(Entry const & a, Entry const & b) const { return a.asof_value > b.asof_value; }
    };


public:
    using Base = std::vector<Entry>;
    using Keys = std::vector<TKey>;
    static constexpr bool isDescending = (inequality == ASOF::Inequality::Greater || inequality == ASOF::Inequality::GreaterOrEquals);
    static constexpr bool isStrict = (inequality == ASOF::Inequality::Less) || (inequality == ASOF::Inequality::Greater);

    void insert(const IColumn & asof_column, const Block * block, size_t row_num) override
    {
        using ColumnType = ColumnVectorOrDecimal<TKey>;
        const auto & column = assert_cast<const ColumnType &>(asof_column);
        TKey k = column.getElement(row_num);

        assert(!sorted.load(std::memory_order_acquire));
        array.emplace_back(k, block, row_num);
    }

    /// Unrolled version of upper_bound and lower_bound
    /// Loosely based on https://academy.realm.io/posts/how-we-beat-cpp-stl-binary-search/
    /// In the future it'd interesting to replace it with a B+Tree Layout as described
    /// at https://en.algorithmica.org/hpc/data-structures/s-tree/
    size_t boundSearch(TKey value)
    {
        size_t size = array.size();
        size_t low = 0;

        /// This is a single binary search iteration as a macro to unroll. Takes into account the inequality:
        /// isStrict -> Equal values are not requested
        /// isDescending -> The vector is sorted in reverse (for greater or greaterOrEquals)
#define BOUND_ITERATION \
    { \
        size_t half = size / 2; \
        size_t other_half = size - half; \
        size_t probe = low + half; \
        size_t other_low = low + other_half; \
        TKey v = array[probe].asof_value; \
        size = half; \
        if constexpr (isDescending) \
        { \
            if constexpr (isStrict) \
                low = value <= v ? other_low : low; \
            else \
                low = value < v ? other_low : low; \
        } \
        else \
        { \
            if constexpr (isStrict) \
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

    std::tuple<decltype(RowRef::block), decltype(RowRef::row_num)> findAsof(const IColumn & asof_column, size_t row_num) override
    {
        sort();

        using ColumnType = ColumnVectorOrDecimal<TKey>;
        const auto & column = assert_cast<const ColumnType &>(asof_column);
        TKey k = column.getElement(row_num);

        size_t pos = boundSearch(k);
        if (pos != array.size())
            return std::make_tuple(array[pos].block, array[pos].row_num);

        return {nullptr, 0};
    }

private:
    std::atomic<bool> sorted = false;
    mutable std::mutex lock;
    Base array;

    // Double checked locking with SC atomics works in C++
    // https://preshing.com/20130930/double-checked-locking-is-fixed-in-cpp11/
    // The first thread that calls one of the lookup methods sorts the data
    // After calling the first lookup method it is no longer allowed to insert any data
    // the array becomes immutable
    void sort()
    {
        if (!sorted.load(std::memory_order_acquire))
        {
            std::lock_guard<std::mutex> l(lock);
            if (!sorted.load(std::memory_order_relaxed))
            {
                if constexpr (isDescending)
                    ::sort(array.begin(), array.end(), greaterEntryOperator());
                else
                    ::sort(array.begin(), array.end());
                sorted.store(true, std::memory_order_release);
            }
        }
    }
};
}

AsofRowRefs createAsofRowRef(TypeIndex type, ASOF::Inequality inequality)
{
    AsofRowRefs result;
    auto call = [&](const auto & t)
    {
        using T = std::decay_t<decltype(t)>;
        switch (inequality)
        {
            case ASOF::Inequality::LessOrEquals:
                result = std::make_unique<SortedLookupVector<T, ASOF::Inequality::LessOrEquals>>();
                break;
            case ASOF::Inequality::Less:
                result = std::make_unique<SortedLookupVector<T, ASOF::Inequality::Less>>();
                break;
            case ASOF::Inequality::GreaterOrEquals:
                result = std::make_unique<SortedLookupVector<T, ASOF::Inequality::GreaterOrEquals>>();
                break;
            case ASOF::Inequality::Greater:
                result = std::make_unique<SortedLookupVector<T, ASOF::Inequality::Greater>>();
                break;
            default:
                throw Exception("Invalid ASOF Join order", ErrorCodes::LOGICAL_ERROR);
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

    throw Exception("ASOF join not supported for type: " + std::string(asof_column.getFamilyName()), ErrorCodes::BAD_TYPE_OF_FIELD);
}

}
