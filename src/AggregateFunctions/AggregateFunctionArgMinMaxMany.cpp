#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <cmath>


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

/// `NaN`-aware three-way comparison of two `val` values.
///
/// It agrees with `Field`'s own ordering everywhere except for `NaN`: `Field::operator<` hardcodes
/// "`NaN` is greater than every real number" (see `nan_direction_hint` in `Field.cpp`), while
/// `argMaxMany`/`argMinMany` -- consistently with `argMax`/`argMin` and `max`/`min` -- have to treat
/// a `NaN` as the *worst* candidate, so that it is evicted in favor of any real value and is kept
/// only when there are not enough real values to fill the result. `nan_direction` selects that:
/// `-1` orders `NaN` before every real number, `+1` after it. `NaN` compares equal to `NaN`.
///
/// The comparison recurses into `Array`, `Tuple` and `Map` values, so that a `NaN` nested inside an
/// accepted composite `val` type such as `Tuple(Float64, UInt8)` or `Array(Float64)` follows the
/// same rule as a top-level `Float64` one, instead of falling back to `Field`'s ordering and
/// outranking every real value.
int compareVals(const Field & lhs, const Field & rhs, int nan_direction);

int compareValSequences(const FieldVector & lhs, const FieldVector & rhs, int nan_direction)
{
    const size_t common_size = std::min(lhs.size(), rhs.size());
    for (size_t i = 0; i < common_size; ++i)
    {
        if (int res = compareVals(lhs[i], rhs[i], nan_direction); res != 0)
            return res;
    }

    if (lhs.size() == rhs.size())
        return 0;
    return lhs.size() < rhs.size() ? -1 : 1;
}

int compareVals(const Field & lhs, const Field & rhs, int nan_direction)
{
    /// Values of different types are ordered by the type tag first, exactly like `Field` does.
    if (lhs.getType() != rhs.getType())
        return lhs.getType() < rhs.getType() ? -1 : 1;

    switch (lhs.getType())
    {
        case Field::Types::Float64:
        {
            const Float64 lhs_value = lhs.safeGet<Float64>();
            const Float64 rhs_value = rhs.safeGet<Float64>();
            const bool lhs_is_nan = std::isnan(lhs_value);
            const bool rhs_is_nan = std::isnan(rhs_value);

            if (lhs_is_nan || rhs_is_nan)
            {
                if (lhs_is_nan && rhs_is_nan)
                    return 0;
                return lhs_is_nan ? nan_direction : -nan_direction;
            }

            if (lhs_value < rhs_value)
                return -1;
            return lhs_value > rhs_value ? 1 : 0;
        }
        case Field::Types::Array:
            return compareValSequences(lhs.safeGet<Array>(), rhs.safeGet<Array>(), nan_direction);
        case Field::Types::Tuple:
            return compareValSequences(lhs.safeGet<Tuple>(), rhs.safeGet<Tuple>(), nan_direction);
        case Field::Types::Map:
            return compareValSequences(lhs.safeGet<Map>(), rhs.safeGet<Map>(), nan_direction);
        default:
        {
            if (lhs < rhs)
                return -1;
            return rhs < lhs ? 1 : 0;
        }
    }
}

/// `NaN` is the worst candidate for `argMaxMany`, which keeps the greatest `val` values, so it is
/// ordered before every real number.
inline bool valGreater(const Field & a, const Field & b)
{
    return compareVals(a, b, -1) > 0;
}

/// `NaN` is the worst candidate for `argMinMany`, which keeps the smallest `val` values, so it is
/// ordered after every real number.
inline bool valLess(const Field & a, const Field & b)
{
    return compareVals(a, b, 1) < 0;
}

/// Comparator for min-heap on val: used by argMaxMany to keep the N largest val values.
/// A min-heap puts the smallest val at the root, so we can easily evict it when a larger
/// val arrives.
struct MinHeapComparator
{
    bool operator()(const Entry & a, const Entry & b) const { return valGreater(a.val, b.val); }
};

/// Comparator for max-heap on val: used by argMinMany to keep the N smallest val values.
/// A max-heap puts the largest val at the root, so we can easily evict it when a smaller
/// val arrives.
struct MaxHeapComparator
{
    bool operator()(const Entry & a, const Entry & b) const { return valLess(a.val, b.val); }
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
            /// Replace root if the new val is smaller. Use the same `NaN`-aware comparator policy
            /// as `add`, otherwise the raw `Field` ordering (which treats `NaN` as the largest value)
            /// would keep a `NaN` root and reject real values on the merge path.
            if (valLess(new_entry.val, data.entries[0].val))
            {
                std::pop_heap(data.entries.begin(), data.entries.end(), MaxHeapComparator{});
                data.entries.back() = std::move(new_entry);
                std::push_heap(data.entries.begin(), data.entries.end(), MaxHeapComparator{});
            }
        }
        else
        {
            /// Min-heap: root is the smallest val among the N largest we keep.
            /// Replace root if the new val is larger. Use the same `NaN`-aware comparator policy
            /// as `add`, otherwise the raw `Field` ordering (which treats `NaN` as the largest value)
            /// would keep a `NaN` root and reject real values on the merge path.
            if (valGreater(new_entry.val, data.entries[0].val))
            {
                std::pop_heap(data.entries.begin(), data.entries.end(), MinHeapComparator{});
                data.entries.back() = std::move(new_entry);
                std::push_heap(data.entries.begin(), data.entries.end(), MinHeapComparator{});
            }
        }
    }

public:
    AggregateFunctionArgMinMaxMany(const DataTypes & argument_types_, const Array & parameters_, UInt64 max_elems_)
        : Base(argument_types_, parameters_, std::make_shared<DataTypeArray>(argument_types_[0]))
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

        /// Reject `Dynamic`/`Variant`/`Object` anywhere inside the `val` type, not only at the top
        /// level: `val` is stored and ranked as a plain `Field`, and these types can hold values of
        /// different data types in a single column, so `Field` ordering (which compares by
        /// `Field::Types::Which` first) does not agree with the column semantics used by
        /// `argMin`/`argMax`, `min`/`max` and `ORDER BY`. This is the same set of types that
        /// `canUseFieldForValueData` (`SingleValueData.cpp`) excludes from the `Field`-based path.
        auto check_val_type = [&](const IDataType & type)
        {
            if (isDynamic(type) || isVariant(type) || isObject(type))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of argument of aggregate function {} because the values of that data type can contain values with "
                    "different data types. Consider using typed subcolumns or cast column to a specific data type",
                    data_type_val->getName(),
                    getName());
        };
        check_val_type(*data_type_val);
        data_type_val->forEachChild(check_val_type);

        /// Reject `Variant` and `Object` anywhere inside the `arg` type. `arg` values are stored in
        /// the state as plain `Field`s, which is lossy for both:
        /// - `SerializationVariant` does not implement `Field`-based binary serialization, so
        ///   serializing the state (e.g. `argMaxManyState` or distributed merges) would throw
        ///   `NOT_IMPLEMENTED`. Moreover, a `Field` cannot record which variant alternative was
        ///   active, so emitting the result could reconstruct a different alternative.
        /// - `ColumnObject::operator[]` collapses a dynamic path holding `NULL` into "path absent"
        ///   (`ColumnObject.cpp`), so a `JSON` document round-tripped through a `Field` can lose
        ///   paths, and the original document could never be returned.
        /// This is the same set of types for which `canUseFieldForValueData` (`SingleValueData.cpp`)
        /// makes `argMin`/`argMax` switch to their column-backed representation. `Dynamic` is fine
        /// here: its serialization encodes the value type together with the value, and
        /// `ColumnDynamic` accepts `Field` insertion.
        auto check_arg_type = [&](const IDataType & type)
        {
            if (isVariant(type) || isObject(type))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of first argument of aggregate function {} because {} values cannot be losslessly stored in the "
                    "aggregation state. Consider using typed subcolumns or cast column to a specific data type",
                    data_type_arg->getName(),
                    getName(),
                    isObject(type) ? "Object" : "Variant");
        };
        check_arg_type(*data_type_arg);
        data_type_arg->forEachChild(check_arg_type);
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

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
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

        const auto & entries = this->data(place).entries;

        /// Sort a copy: `insertResultInto` must not mutate the aggregate state, because window
        /// aggregation over a growing frame reuses the same state (and its heap invariant) across
        /// the rows of the frame after each result is written.
        VectorWithMemoryTracking<Entry> sorted(entries.begin(), entries.end());
        if constexpr (isMin)
            std::sort(sorted.begin(), sorted.end(), [](const Entry & a, const Entry & b) { return valLess(a.val, b.val); });
        else
            std::sort(sorted.begin(), sorted.end(), [](const Entry & a, const Entry & b) { return valGreater(a.val, b.val); });

        for (const auto & entry : sorted)
            col_data.insert(entry.arg);

        offsets.push_back(offsets.back() + sorted.size());
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

    UInt64 max_elems = 0;
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
        return std::make_shared<AggregateFunctionArgMinMaxMany<true>>(argument_types, parameters, max_elems);
    else
        return std::make_shared<AggregateFunctionArgMinMaxMany<false>>(argument_types, parameters, max_elems);
}

}

void registerAggregateFunctionsArgMinMaxMany(AggregateFunctionFactory & factory);
void registerAggregateFunctionsArgMinMaxMany(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = {.returns_default_when_only_null = false, .is_order_dependent = true};

    FunctionDocumentation::Description description_argMaxMany = R"(
Returns an array of the `arg` values corresponding to the `N` largest `val` values, sorted in descending order of `val`.
If there are fewer than `N` rows, all `arg` values are returned.
If there are multiple rows with equal `val`, which of the associated `arg` values are returned, and in which order, is not deterministic.
Both parts the `arg` and the `val` behave as [aggregate functions](/reference/functions/aggregate-functions): rows where a `Nullable` `arg` or `val` is `NULL` are [skipped](/reference/functions/aggregate-functions#null-processing).
A `NULL` stored inside a `Dynamic` `arg` is not a `Nullable` value and is kept.

**See also**

- [argMax](/reference/functions/aggregate-functions/argMax)
- [argMinMany](/reference/functions/aggregate-functions/argMinMany)
    )";
    FunctionDocumentation::Syntax syntax_argMaxMany = "argMaxMany(N)(arg, val)";
    FunctionDocumentation::Parameters parameters_argMaxMany = {
        {"N", "The maximum number of elements to return.", {"UInt64"}}
    };
    FunctionDocumentation::Arguments arguments_argMaxMany = {
        {"arg", "Argument values to collect. Any type except `Variant` and `JSON` (also when nested inside another type).", {"Any"}},
        {"val", "Values used to determine the top N rows. Any comparable type except `Dynamic`, `Variant` and `JSON` (also when nested inside another type).", {"(U)Int*", "Float*", "String", "Date", "DateTime", "Tuple"}}
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
    FunctionDocumentation::IntroducedIn introduced_in_argMaxMany = {26, 9};
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
If there are multiple rows with equal `val`, which of the associated `arg` values are returned, and in which order, is not deterministic.
Both parts the `arg` and the `val` behave as [aggregate functions](/reference/functions/aggregate-functions): rows where a `Nullable` `arg` or `val` is `NULL` are [skipped](/reference/functions/aggregate-functions#null-processing).
A `NULL` stored inside a `Dynamic` `arg` is not a `Nullable` value and is kept.

**See also**

- [argMin](/reference/functions/aggregate-functions/argMin)
- [argMaxMany](/reference/functions/aggregate-functions/argMaxMany)
    )";
    FunctionDocumentation::Syntax syntax_argMinMany = "argMinMany(N)(arg, val)";
    FunctionDocumentation::Parameters parameters_argMinMany = {
        {"N", "The maximum number of elements to return.", {"UInt64"}}
    };
    FunctionDocumentation::Arguments arguments_argMinMany = {
        {"arg", "Argument values to collect. Any type except `Variant` and `JSON` (also when nested inside another type).", {"Any"}},
        {"val", "Values used to determine the bottom N rows. Any comparable type except `Dynamic`, `Variant` and `JSON` (also when nested inside another type).", {"(U)Int*", "Float*", "String", "Date", "DateTime", "Tuple"}}
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
    FunctionDocumentation::IntroducedIn introduced_in_argMinMany = {26, 9};
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
