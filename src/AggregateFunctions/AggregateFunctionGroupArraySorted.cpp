#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>

#include <base/sort.h>
#include <algorithm>
#include <type_traits>
#include <utility>

#include <Common/RadixSort.h>
#include <Common/Exception.h>
#include <Common/ArenaAllocator.h>
#include <Common/assert_cast.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>

#include <Columns/IColumn.h>
#include <Columns/ColumnConst.h>

namespace DB
{

struct Settings;

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int TOO_LARGE_ARRAY_SIZE;
}

namespace
{

enum class GroupArraySortedStrategy : uint8_t
{
    heap,
    sort
};


constexpr size_t group_array_sorted_sort_strategy_max_elements_threshold = 1000000;

template <typename T, GroupArraySortedStrategy strategy>
struct GroupArraySortedData
{
    using Allocator = MixedAlignedArenaAllocator<alignof(T), 4096>;
    using Array = PODArray<T, 32, Allocator>;

    static constexpr size_t partial_sort_max_elements_factor = 2;

    static constexpr bool is_value_generic_field = std::is_same_v<T, Field>;

    Array values;

    static bool compare(const T & lhs, const T & rhs)
    {
        if constexpr (is_value_generic_field)
        {
            return lhs < rhs;
        }
        else
        {
            return CompareHelper<T>::less(lhs, rhs, -1);
        }
    }

    struct Comparator
    {
        bool operator()(const T & lhs, const T & rhs)
        {
            return compare(lhs, rhs);
        }
    };

    ALWAYS_INLINE void heapReplaceTop()
    {
        size_t size = values.size();
        if (size < 2)
            return;

        size_t child_index = 1;

        if (values.size() > 2 && compare(values[1], values[2]))
            ++child_index;

        /// Check if we are in order
        if (compare(values[child_index], values[0]))
            return;

        size_t current_index = 0;
        auto current = values[current_index];

        do
        {
            /// We are not in heap-order, swap the parent with it's largest child.
            values[current_index] = values[child_index];
            current_index = child_index;

            // Recompute the child based off of the updated parent
            child_index = 2 * child_index + 1;

            if (child_index >= size)
                break;

            if ((child_index + 1) < size && compare(values[child_index], values[child_index + 1]))
            {
                /// Right child exists and is greater than left child.
                ++child_index;
            }

            /// Check if we are in order.
        } while (!compare(values[child_index], current));

        values[current_index] = current;
    }

    ALWAYS_INLINE void sortAndLimit(size_t max_elements, Arena * arena)
    {
        if constexpr (is_value_generic_field)
        {
            ::sort(values.begin(), values.end(), Comparator());
        }
        else
        {
            bool try_sort = trySort(values.begin(), values.end(), Comparator());
            if (!try_sort)
                RadixSort<RadixSortNumTraits<T>>::executeLSD(values.data(), values.size());
        }

        if (values.size() > max_elements)
            values.resize(max_elements, arena);
    }

    ALWAYS_INLINE void partialSortAndLimitIfNeeded(size_t max_elements, Arena * arena)
    {
        if (values.size() < max_elements * partial_sort_max_elements_factor)
            return;

        ::nth_element(values.begin(), values.begin() + max_elements, values.end(), Comparator());
        values.resize(max_elements, arena);
    }

    ALWAYS_INLINE void addElement(T && element, size_t max_elements, Arena * arena)
    {
        if constexpr (strategy == GroupArraySortedStrategy::heap)
        {
            if (values.size() >= max_elements)
            {
                /// Element is greater or equal than current max element, it cannot be in k min elements
                if (!compare(element, values[0]))
                    return;

                values[0] = std::move(element);
                heapReplaceTop();
                return;
            }

            values.push_back(std::move(element), arena);
            std::push_heap(values.begin(), values.end(), Comparator());
        }
        else
        {
            values.push_back(std::move(element), arena);
            partialSortAndLimitIfNeeded(max_elements, arena);
        }
    }

    ALWAYS_INLINE void insertResultInto(IColumn & to, size_t max_elements, Arena * arena)
    {
        auto & result_array = assert_cast<ColumnArray &>(to);
        auto & result_array_offsets = result_array.getOffsets();

        sortAndLimit(max_elements, arena);

        result_array_offsets.push_back(result_array_offsets.back() + values.size());

        if (values.empty())
            return;

        if constexpr (is_value_generic_field)
        {
            auto & result_array_data = result_array.getData();
            for (auto & value : values)
                result_array_data.insert(value);
        }
        else
        {
            auto & result_array_data = assert_cast<ColumnVector<T> &>(result_array.getData()).getData();

            size_t result_array_data_insert_begin = result_array_data.size();
            result_array_data.resize(result_array_data_insert_begin + values.size());

            for (size_t i = 0; i < values.size(); ++i)
                result_array_data[result_array_data_insert_begin + i] = values[i];
        }
    }

    ~GroupArraySortedData()
    {
        for (auto & value : values)
        {
            value.~T();
        }
    }
};

template <typename T>
using GroupArraySortedDataHeap = GroupArraySortedData<T, GroupArraySortedStrategy::heap>;

template <typename T>
using GroupArraySortedDataSort = GroupArraySortedData<T, GroupArraySortedStrategy::sort>;

constexpr UInt64 aggregate_function_group_array_sorted_max_element_size = 0xFFFFFF;

template <typename Data, typename T>
class GroupArraySorted final
    : public IAggregateFunctionDataHelper<Data, GroupArraySorted<Data, T>>
{
public:
    explicit GroupArraySorted(
        const DataTypePtr & data_type_, const Array & parameters_, UInt64 max_elements_)
        : IAggregateFunctionDataHelper<Data, GroupArraySorted<Data, T>>(
            {data_type_}, parameters_, std::make_shared<DataTypeArray>(data_type_))
        , max_elements(max_elements_)
        , serialization(data_type_->getDefaultSerialization())
    {
        if (max_elements > aggregate_function_group_array_sorted_max_element_size)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Too large limit parameter for groupArraySorted aggregate function, it should not exceed {}",
                aggregate_function_group_array_sorted_max_element_size);
    }

    String getName() const override { return "groupArraySorted"; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        if constexpr (std::is_same_v<T, Field>)
        {
            auto row_value = (*columns[0])[row_num];
            this->data(place).addElement(std::move(row_value), max_elements, arena);
        }
        else
        {
            auto row_value = assert_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num];
            this->data(place).addElement(std::move(row_value), max_elements, arena);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & rhs_values = this->data(rhs).values;
        for (auto rhs_element : rhs_values)
            this->data(place).addElement(std::move(rhs_element), max_elements, arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        auto & values = this->data(place).values;
        size_t size = values.size();
        writeVarUInt(size, buf);

        if constexpr (std::is_same_v<T, Field>)
        {
            for (const Field & element : values)
            {
                if (element.isNull())
                {
                    writeBinary(false, buf);
                }
                else
                {
                    writeBinary(true, buf);
                    serialization->serializeBinary(element, buf, {});
                }
            }
        }
        else
        {
            if constexpr (std::endian::native == std::endian::little)
            {
                buf.write(reinterpret_cast<const char *>(values.data()), size * sizeof(values[0]));
            }
            else
            {
                for (const auto & element : values)
                    writeBinaryLittleEndian(element, buf);
            }
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        size_t size = 0;
        readVarUInt(size, buf);

        if (unlikely(size > max_elements))
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large array size, it should not exceed {}", max_elements);

        auto & values = this->data(place).values;
        values.resize_exact(size, arena);

        if constexpr (std::is_same_v<T, Field>)
        {
            for (Field & element : values)
            {
                /// We must initialize the Field type since some internal functions (like operator=) use them
                new (&element) Field;
                bool has_value = false;
                readBinary(has_value, buf);
                if (has_value)
                    serialization->deserializeBinary(element, buf, {});
            }
        }
        else
        {
            if constexpr (std::endian::native == std::endian::little)
            {
                buf.readStrict(reinterpret_cast<char *>(values.data()), size * sizeof(values[0]));
            }
            else
            {
                for (auto & element : values)
                    readBinaryLittleEndian(element, buf);
            }
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        this->data(place).insertResultInto(to, max_elements, arena);
    }

    bool allocatesMemoryInArena() const override { return true; }

private:
    UInt64 max_elements;
    SerializationPtr serialization;
};

template <typename T>
using GroupArraySortedHeap = GroupArraySorted<GroupArraySortedDataHeap<T>, T>;

template <typename T>
using GroupArraySortedSort = GroupArraySorted<GroupArraySortedDataSort<T>, T>;

template <template <typename> class AggregateFunctionTemplate, typename ... TArgs>
AggregateFunctionPtr createWithNumericOrTimeType(const IDataType & argument_type, TArgs && ... args)
{
    WhichDataType which(argument_type);

    if (which.idx == TypeIndex::Date) return std::make_shared<AggregateFunctionTemplate<UInt16>>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::DateTime) return std::make_shared<AggregateFunctionTemplate<UInt32>>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::IPv4) return std::make_shared<AggregateFunctionTemplate<IPv4>>(std::forward<TArgs>(args)...);

    return AggregateFunctionPtr(createWithNumericType<AggregateFunctionTemplate, TArgs...>(argument_type, std::forward<TArgs>(args)...));
}

template <template <typename> class AggregateFunctionTemplate, typename ... TArgs>
inline AggregateFunctionPtr createAggregateFunctionGroupArraySortedImpl(const DataTypePtr & argument_type, const Array & parameters, TArgs ... args)
{
    if (auto res = createWithNumericOrTimeType<AggregateFunctionTemplate>(*argument_type, argument_type, parameters, args...))
        return AggregateFunctionPtr(res);

    return std::make_shared<AggregateFunctionTemplate<Field>>(argument_type, parameters, args...);
}

AggregateFunctionPtr createAggregateFunctionGroupArray(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertUnary(name, argument_types);

    UInt64 max_elems = std::numeric_limits<UInt64>::max();

    if (parameters.empty())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter for aggregate function {} should have limit argument", name);
    }
    if (parameters.size() == 1)
    {
        auto type = parameters[0].getType();
        if (type != Field::Types::Int64 && type != Field::Types::UInt64)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter for aggregate function {} should be positive number", name);

        if ((type == Field::Types::Int64 && parameters[0].safeGet<Int64>() < 0)
            || (type == Field::Types::UInt64 && parameters[0].safeGet<UInt64>() == 0))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter for aggregate function {} should be positive number", name);

        max_elems = parameters[0].safeGet<UInt64>();
    }
    else
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} does not support this number of arguments", name);

    if (max_elems > group_array_sorted_sort_strategy_max_elements_threshold)
        return createAggregateFunctionGroupArraySortedImpl<GroupArraySortedSort>(argument_types[0], parameters, max_elems);

    return createAggregateFunctionGroupArraySortedImpl<GroupArraySortedHeap>(argument_types[0], parameters, max_elems);
}

}

void registerAggregateFunctionGroupArraySorted(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = false, .is_order_dependent = false };

    factory.registerFunction("groupArraySorted", { createAggregateFunctionGroupArray, properties });
}

}
