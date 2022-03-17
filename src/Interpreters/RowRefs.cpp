#include <Interpreters/RowRefs.h>

#include <base/types.h>
#include <Columns/IColumn.h>


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
void callWithType(TypeIndex which, F && f)
{
    switch (which)
    {
        case TypeIndex::UInt8:  return f(UInt8());
        case TypeIndex::UInt16: return f(UInt16());
        case TypeIndex::UInt32: return f(UInt32());
        case TypeIndex::UInt64: return f(UInt64());
        case TypeIndex::Int8:   return f(Int8());
        case TypeIndex::Int16:  return f(Int16());
        case TypeIndex::Int32:  return f(Int32());
        case TypeIndex::Int64:  return f(Int64());
        case TypeIndex::Float32: return f(Float32());
        case TypeIndex::Float64: return f(Float64());
        case TypeIndex::Decimal32: return f(Decimal32());
        case TypeIndex::Decimal64: return f(Decimal64());
        case TypeIndex::Decimal128: return f(Decimal128());
        case TypeIndex::DateTime64: return f(DateTime64());
        default:
            break;
    }

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
    TypeIndex idx = asof_column.getDataType();

    switch (idx)
    {
        case TypeIndex::UInt8:
            size = sizeof(UInt8);
            return idx;
        case TypeIndex::UInt16:
            size = sizeof(UInt16);
            return idx;
        case TypeIndex::UInt32:
            size = sizeof(UInt32);
            return idx;
        case TypeIndex::UInt64:
            size = sizeof(UInt64);
            return idx;
        case TypeIndex::Int8:
            size = sizeof(Int8);
            return idx;
        case TypeIndex::Int16:
            size = sizeof(Int16);
            return idx;
        case TypeIndex::Int32:
            size = sizeof(Int32);
            return idx;
        case TypeIndex::Int64:
            size = sizeof(Int64);
            return idx;
        case TypeIndex::Float32:
            size = sizeof(Float32);
            return idx;
        case TypeIndex::Float64:
            size = sizeof(Float64);
            return idx;
        case TypeIndex::Decimal32:
            size = sizeof(Decimal32);
            return idx;
        case TypeIndex::Decimal64:
            size = sizeof(Decimal64);
            return idx;
        case TypeIndex::Decimal128:
            size = sizeof(Decimal128);
            return idx;
        case TypeIndex::DateTime64:
            size = sizeof(DateTime64);
            return idx;
        default:
            break;
    }

    throw Exception("ASOF join not supported for type: " + std::string(asof_column.getFamilyName()), ErrorCodes::BAD_TYPE_OF_FIELD);
}

}
