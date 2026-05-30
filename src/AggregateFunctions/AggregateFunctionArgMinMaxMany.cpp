#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Common/VectorWithMemoryTracking.h>
#include <DataTypes/DataTypeArray.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

struct Settings;

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TOO_LARGE_ARRAY_SIZE;
}

namespace
{

constexpr UInt64 aggregate_function_arg_min_max_many_max_element_size = 0xFFFFFF;

struct Entry
{
    Field arg;
    Field val;
};

/// Comparator for min-heap on val: used by argMaxMany to keep the N largest val values.
/// A min-heap puts the smallest val at the root, so we can easily evict it when a larger
/// val arrives.
struct MinHeapComparator
{
    bool operator()(const Entry & a, const Entry & b) const { return a.val > b.val; }
};

/// Comparator for max-heap on val: used by argMinMany to keep the N smallest val values.
/// A max-heap puts the largest val at the root, so we can easily evict it when a smaller
/// val arrives.
struct MaxHeapComparator
{
    bool operator()(const Entry & a, const Entry & b) const { return a.val < b.val; }
};

template <bool isMin>
struct AggregateFunctionArgMinManyData
{
    VectorWithMemoryTracking<Entry> entries;
    bool is_heap = false;
};

template <bool isMin>
class AggregateFunctionArgMinMaxMany final
    : public IAggregateFunctionDataHelper<AggregateFunctionArgMinManyData<isMin>, AggregateFunctionArgMinMaxMany<isMin>>
{
    UInt64 max_elems;
    DataTypePtr data_type_arg;
    DataTypePtr data_type_val;
    SerializationPtr serialization_arg;
    SerializationPtr serialization_val;

    using Data = AggregateFunctionArgMinManyData<isMin>;
    using Base = IAggregateFunctionDataHelper<Data, AggregateFunctionArgMinMaxMany<isMin>>;

    void addEntry(AggregateDataPtr __restrict place, Entry new_entry) const
    {
        auto & data = this->data(place);

        if (data.entries.size() < max_elems)
        {
            data.entries.push_back(std::move(new_entry));
            if (data.entries.size() == max_elems)
            {
                if constexpr (isMin)
                    std::make_heap(data.entries.begin(), data.entries.end(), MaxHeapComparator{});
                else
                    std::make_heap(data.entries.begin(), data.entries.end(), MinHeapComparator{});
                data.is_heap = true;
            }
            return;
        }

        if constexpr (isMin)
        {
            /// Max-heap: root is the largest val among the N smallest we keep.
            /// Replace root if the new val is smaller.
            if (new_entry.val < data.entries[0].val)
            {
                std::pop_heap(data.entries.begin(), data.entries.end(), MaxHeapComparator{});
                data.entries.back() = std::move(new_entry);
                std::push_heap(data.entries.begin(), data.entries.end(), MaxHeapComparator{});
            }
        }
        else
        {
            /// Min-heap: root is the smallest val among the N largest we keep.
            /// Replace root if the new val is larger.
            if (new_entry.val > data.entries[0].val)
            {
                std::pop_heap(data.entries.begin(), data.entries.end(), MinHeapComparator{});
                data.entries.back() = std::move(new_entry);
                std::push_heap(data.entries.begin(), data.entries.end(), MinHeapComparator{});
            }
        }
    }

public:
    explicit AggregateFunctionArgMinMaxMany(const DataTypes & argument_types_, UInt64 max_elems_)
        : Base(argument_types_, {}, std::make_shared<DataTypeArray>(argument_types_[0]))
        , max_elems(max_elems_)
        , data_type_arg(argument_types_[0])
        , data_type_val(argument_types_[1])
        , serialization_arg(argument_types_[0]->getDefaultSerialization())
        , serialization_val(argument_types_[1]->getDefaultSerialization())
    {
        if (!data_type_val->isComparable())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument of aggregate function {} because the values of that data type are not comparable",
                data_type_val->getName(),
                getName());

        if (isDynamic(data_type_val) || isVariant(data_type_val))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of aggregate function {} because the column of that type can contain values with different data types. Consider using typed subcolumns or cast column to a specific data type",
                data_type_val->getName(),
                getName());
    }

    String getName() const override
    {
        if constexpr (isMin)
            return "argMinMany";
        else
            return "argMaxMany";
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        if (columns[1]->isNullAt(row_num))
            return;

        auto & data = this->data(place);

        /// When the heap is full, check val first to avoid materializing arg on the hot rejection path.
        if (data.is_heap)
        {
            Field new_val = (*columns[1])[row_num];
            using Cmp = std::conditional_t<isMin, MaxHeapComparator, MinHeapComparator>;
            Cmp cmp;
            if (!cmp(Entry{Field{}, new_val}, data.entries[0]))
                return;
            std::pop_heap(data.entries.begin(), data.entries.end(), cmp);
            data.entries.back() = Entry{(*columns[0])[row_num], std::move(new_val)};
            std::push_heap(data.entries.begin(), data.entries.end(), cmp);
            return;
        }

        /// Fill-up phase: heap not yet built, materialize both fields.
        addEntry(place, Entry{(*columns[0])[row_num], (*columns[1])[row_num]});
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        const auto & rhs_data = this->data(rhs);
        for (const auto & entry : rhs_data.entries)
            addEntry(place, entry);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        const auto & entries = this->data(place).entries;
        writeVarUInt(entries.size(), buf);
        for (const auto & entry : entries)
        {
            if (entry.arg.isNull())
            {
                writeBinary(false, buf);
            }
            else
            {
                writeBinary(true, buf);
                serialization_arg->serializeBinary(entry.arg, buf, {});
            }

            serialization_val->serializeBinary(entry.val, buf, {});
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        size_t size = 0;
        readVarUInt(size, buf);

        if (unlikely(size > max_elems))
            throw Exception(
                ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                "Too large array size {} for aggregate function {}, maximum is {}",
                size,
                getName(),
                max_elems);

        auto & data = this->data(place);
        data.entries.resize(size);
        data.is_heap = false;

        for (auto & entry : data.entries)
        {
            bool has_arg = false;
            readBinary(has_arg, buf);
            if (has_arg)
                serialization_arg->deserializeBinary(entry.arg, buf, {});
            else
                entry.arg = Field{};

            serialization_val->deserializeBinary(entry.val, buf, {});
        }

        if (data.entries.size() == max_elems)
        {
            if constexpr (isMin)
                std::make_heap(data.entries.begin(), data.entries.end(), MaxHeapComparator{});
            else
                std::make_heap(data.entries.begin(), data.entries.end(), MinHeapComparator{});
            data.is_heap = true;
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & col_array = assert_cast<ColumnArray &>(to);
        auto & offsets = col_array.getOffsets();
        auto & col_data = col_array.getData();

        auto & entries = this->data(place).entries;

        if constexpr (isMin)
            std::sort(entries.begin(), entries.end(), [](const Entry & a, const Entry & b) { return a.val < b.val; });
        else
            std::sort(entries.begin(), entries.end(), [](const Entry & a, const Entry & b) { return a.val > b.val; });

        for (const auto & entry : entries)
            col_data.insert(entry.arg);

        offsets.push_back(offsets.back() + entries.size());
    }

    bool allocatesMemoryInArena() const override { return false; }
};

AggregateFunctionPtr createAggregateFunctionArgMinMaxMany(
    bool isMin, const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertBinary(name, argument_types);

    if (parameters.size() != 1)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Aggregate function {} requires exactly one parameter: the number of elements N",
            name);

    const auto & param = parameters[0];
    auto param_type = param.getType();
    if (param_type != Field::Types::Int64 && param_type != Field::Types::UInt64)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter for aggregate function {} should be a positive integer", name);

    UInt64 max_elems;
    if (param_type == Field::Types::UInt64)
    {
        max_elems = param.safeGet<UInt64>();
        if (max_elems == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter for aggregate function {} should be a positive integer", name);
    }
    else
    {
        Int64 v = param.safeGet<Int64>();
        if (v <= 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter for aggregate function {} should be a positive integer", name);
        max_elems = static_cast<UInt64>(v);
    }
    if (max_elems > aggregate_function_arg_min_max_many_max_element_size)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Too large limit parameter for aggregate function {}, it should not exceed {}",
            name,
            aggregate_function_arg_min_max_many_max_element_size);

    if (isMin)
        return std::make_shared<AggregateFunctionArgMinMaxMany<true>>(argument_types, max_elems);
    else
        return std::make_shared<AggregateFunctionArgMinMaxMany<false>>(argument_types, max_elems);
}

}

void registerAggregateFunctionsArgMinMaxMany(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = {.returns_default_when_only_null = false, .is_order_dependent = true};

    FunctionDocumentation::Description description_argMaxMany = R"(
Returns an array of the `arg` values corresponding to the `N` largest `val` values, sorted in descending order of `val`.
If there are fewer than `N` rows, all `arg` values are returned.
Null `val` values are ignored.

**See also**

- [argMax](/sql-reference/aggregate-functions/reference/argMax.md)
- [argMinMany](/sql-reference/aggregate-functions/reference/argMinMany.md)
    )";
    FunctionDocumentation::Syntax syntax_argMaxMany = "argMaxMany(N)(arg, val)";
    FunctionDocumentation::Parameters parameters_argMaxMany = {
        {"N", "The maximum number of elements to return.", {"UInt64"}}
    };
    FunctionDocumentation::Arguments arguments_argMaxMany = {
        {"arg", "Argument values to collect.", {"Any"}},
        {"val", "Values used to determine the top N rows.", {"(U)Int*", "Float*", "String", "Date", "DateTime", "Tuple"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_argMaxMany = {
        "Array of `arg` values corresponding to the N largest `val` values, in descending order of `val`.",
        {"Array"}
    };
    FunctionDocumentation::Examples examples_argMaxMany = {
        {
            "Basic usage",
            R"(
SELECT argMaxMany(2)(user, salary) FROM salary;
            )",
            R"(
┌─argMaxMany(2)(user, salary)─┐
│ ['director','manager']      │
└─────────────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in_argMaxMany = {26, 6};
    FunctionDocumentation::Category category_argMaxMany = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_argMaxMany = {
        description_argMaxMany,
        syntax_argMaxMany,
        arguments_argMaxMany,
        parameters_argMaxMany,
        returned_value_argMaxMany,
        examples_argMaxMany,
        introduced_in_argMaxMany,
        category_argMaxMany
    };

    factory.registerFunction(
        "argMaxMany",
        {[](const std::string & name, const DataTypes & argument_types, const Array & params, const Settings * settings)
         { return createAggregateFunctionArgMinMaxMany(false, name, argument_types, params, settings); },
         documentation_argMaxMany,
         properties});

    FunctionDocumentation::Description description_argMinMany = R"(
Returns an array of the `arg` values corresponding to the `N` smallest `val` values, sorted in ascending order of `val`.
If there are fewer than `N` rows, all `arg` values are returned.
Null `val` values are ignored.

**See also**

- [argMin](/sql-reference/aggregate-functions/reference/argMin.md)
- [argMaxMany](/sql-reference/aggregate-functions/reference/argMaxMany.md)
    )";
    FunctionDocumentation::Syntax syntax_argMinMany = "argMinMany(N)(arg, val)";
    FunctionDocumentation::Parameters parameters_argMinMany = {
        {"N", "The maximum number of elements to return.", {"UInt64"}}
    };
    FunctionDocumentation::Arguments arguments_argMinMany = {
        {"arg", "Argument values to collect.", {"Any"}},
        {"val", "Values used to determine the bottom N rows.", {"(U)Int*", "Float*", "String", "Date", "DateTime", "Tuple"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_argMinMany = {
        "Array of `arg` values corresponding to the N smallest `val` values, in ascending order of `val`.",
        {"Array"}
    };
    FunctionDocumentation::Examples examples_argMinMany = {
        {
            "Basic usage",
            R"(
SELECT argMinMany(2)(user, salary) FROM salary;
            )",
            R"(
┌─argMinMany(2)(user, salary)─┐
│ ['worker','intern']         │
└─────────────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in_argMinMany = {26, 6};
    FunctionDocumentation::Category category_argMinMany = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_argMinMany = {
        description_argMinMany,
        syntax_argMinMany,
        arguments_argMinMany,
        parameters_argMinMany,
        returned_value_argMinMany,
        examples_argMinMany,
        introduced_in_argMinMany,
        category_argMinMany
    };

    factory.registerFunction(
        "argMinMany",
        {[](const std::string & name, const DataTypes & argument_types, const Array & params, const Settings * settings)
         { return createAggregateFunctionArgMinMaxMany(true, name, argument_types, params, settings); },
         documentation_argMinMany,
         properties});
}

}
