#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/HelpersMinMaxAny.h>
#include <Common/Concepts.h>
#include <Common/findExtreme.h>

namespace DB
{
struct Settings;

namespace
{

template <typename Data>
class AggregateFunctionsSingleValueMax final : public AggregateFunctionsSingleValue<Data>
{
    using Parent = AggregateFunctionsSingleValue<Data>;

public:
    explicit AggregateFunctionsSingleValueMax(const DataTypePtr & type) : Parent(type) { }

    /// Specializations for native numeric types
    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** __restrict columns,
        Arena * arena,
        ssize_t if_argument_pos) const override;

    void addBatchSinglePlaceNotNull(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** __restrict columns,
        const UInt8 * __restrict null_map,
        Arena * arena,
        ssize_t if_argument_pos) const override;
};

// NOLINTBEGIN(bugprone-macro-parentheses)
#define SPECIALIZE(TYPE) \
template <> \
void AggregateFunctionsSingleValueMax<typename DB::AggregateFunctionMaxData<SingleValueDataFixed<TYPE>>>::addBatchSinglePlace( \
    size_t row_begin, \
    size_t row_end, \
    AggregateDataPtr __restrict place, \
    const IColumn ** __restrict columns, \
    Arena *, \
    ssize_t if_argument_pos) const \
{ \
    const auto & column = assert_cast<const DB::AggregateFunctionMaxData<SingleValueDataFixed<TYPE>>::ColVecType &>(*columns[0]); \
    std::optional<TYPE> opt; \
    if (if_argument_pos >= 0) \
    { \
        const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData(); \
        opt = findExtremeMaxIf(column.getData().data(), flags.data(), row_begin, row_end); \
    } \
    else \
        opt = findExtremeMax(column.getData().data(), row_begin, row_end); \
    if (opt.has_value()) \
        this->data(place).changeIfGreater(opt.value()); \
}
// NOLINTEND(bugprone-macro-parentheses)

FOR_BASIC_NUMERIC_TYPES(SPECIALIZE)
#undef SPECIALIZE

template <typename Data>
void AggregateFunctionsSingleValueMax<Data>::addBatchSinglePlace(
    size_t row_begin,
    size_t row_end,
    AggregateDataPtr __restrict place,
    const IColumn ** __restrict columns,
    Arena * arena,
    ssize_t if_argument_pos) const
{
    if constexpr (!is_any_of<typename Data::Impl, SingleValueDataString, SingleValueDataGeneric>)
    {
        /// Leave other numeric types (large integers, decimals, etc) to keep doing the comparison as it's
        /// faster than doing a permutation
        return Parent::addBatchSinglePlace(row_begin, row_end, place, columns, arena, if_argument_pos);
    }

    constexpr int nan_direction_hint = 1;
    auto const & column = *columns[0];
    if (if_argument_pos >= 0)
    {
        size_t index = row_begin;
        const auto & if_flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
        while (if_flags[index] == 0 && index < row_end)
            index++;
        if (index >= row_end)
            return;

        for (size_t i = index + 1; i < row_end; i++)
        {
            if ((if_flags[i] != 0) && (column.compareAt(i, index, column, nan_direction_hint) > 0))
                index = i;
        }
        this->data(place).changeIfGreater(column, index, arena);
    }
    else
    {
        if (row_begin >= row_end)
            return;

        /// TODO: Introduce row_begin and row_end to getPermutation
        if (row_begin != 0 || row_end != column.size())
        {
            size_t index = row_begin;
            for (size_t i = index + 1; i < row_end; i++)
            {
                if (column.compareAt(i, index, column, nan_direction_hint) > 0)
                    index = i;
            }
            this->data(place).changeIfGreater(column, index, arena);
        }
        else
        {
            constexpr IColumn::PermutationSortDirection direction = IColumn::PermutationSortDirection::Descending;
            constexpr IColumn::PermutationSortStability stability = IColumn::PermutationSortStability::Unstable;
            IColumn::Permutation permutation;
            constexpr UInt64 limit = 1;
            column.getPermutation(direction, stability, limit, nan_direction_hint, permutation);
            this->data(place).changeIfGreater(column, permutation[0], arena);
        }
    }
}

// NOLINTBEGIN(bugprone-macro-parentheses)
#define SPECIALIZE(TYPE) \
template <> \
void AggregateFunctionsSingleValueMax<typename DB::AggregateFunctionMaxData<SingleValueDataFixed<TYPE>>>::addBatchSinglePlaceNotNull( \
    size_t row_begin, \
    size_t row_end, \
    AggregateDataPtr __restrict place, \
    const IColumn ** __restrict columns, \
    const UInt8 * __restrict null_map, \
    Arena *, \
    ssize_t if_argument_pos) const \
{ \
    const auto & column = assert_cast<const DB::AggregateFunctionMaxData<SingleValueDataFixed<TYPE>>::ColVecType &>(*columns[0]); \
    std::optional<TYPE> opt; \
    if (if_argument_pos >= 0) \
    { \
        const auto * if_flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data(); \
        auto final_flags = std::make_unique<UInt8[]>(row_end); \
        for (size_t i = row_begin; i < row_end; ++i) \
            final_flags[i] = (!null_map[i]) & !!if_flags[i]; \
        opt = findExtremeMaxIf(column.getData().data(), final_flags.get(), row_begin, row_end); \
    } \
    else \
        opt = findExtremeMaxNotNull(column.getData().data(), null_map, row_begin, row_end); \
    if (opt.has_value()) \
        this->data(place).changeIfGreater(opt.value()); \
}
// NOLINTEND(bugprone-macro-parentheses)

FOR_BASIC_NUMERIC_TYPES(SPECIALIZE)
#undef SPECIALIZE

template <typename Data>
void AggregateFunctionsSingleValueMax<Data>::addBatchSinglePlaceNotNull(
    size_t row_begin,
    size_t row_end,
    AggregateDataPtr __restrict place,
    const IColumn ** __restrict columns,
    const UInt8 * __restrict null_map,
    Arena * arena,
    ssize_t if_argument_pos) const
{
    if constexpr (!is_any_of<typename Data::Impl, SingleValueDataString, SingleValueDataGeneric>)
    {
        /// Leave other numeric types (large integers, decimals, etc) to keep doing the comparison as it's
        /// faster than doing a permutation
        return Parent::addBatchSinglePlaceNotNull(row_begin, row_end, place, columns, null_map, arena, if_argument_pos);
    }

    constexpr int nan_direction_hint = 1;
    auto const & column = *columns[0];
    if (if_argument_pos >= 0)
    {
        size_t index = row_begin;
        const auto & if_flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
        while ((if_flags[index] == 0 || null_map[index] != 0) && (index < row_end))
            index++;
        if (index >= row_end)
            return;

        for (size_t i = index + 1; i < row_end; i++)
        {
            if ((if_flags[i] != 0) && (null_map[i] == 0) && (column.compareAt(i, index, column, nan_direction_hint) > 0))
                index = i;
        }
        this->data(place).changeIfGreater(column, index, arena);
    }
    else
    {
        size_t index = row_begin;
        while ((null_map[index] != 0) && (index < row_end))
            index++;
        if (index >= row_end)
            return;

        for (size_t i = index + 1; i < row_end; i++)
        {
            if ((null_map[i] == 0) && (column.compareAt(i, index, column, nan_direction_hint) > 0))
                index = i;
        }
        this->data(place).changeIfGreater(column, index, arena);
    }
}

AggregateFunctionPtr createAggregateFunctionMax(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    return AggregateFunctionPtr(createAggregateFunctionSingleValue<AggregateFunctionsSingleValueMax, AggregateFunctionMaxData>(name, argument_types, parameters, settings));
}

AggregateFunctionPtr createAggregateFunctionArgMax(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    return AggregateFunctionPtr(createAggregateFunctionArgMinMax<AggregateFunctionMaxData>(name, argument_types, parameters, settings));
}

}

void registerAggregateFunctionsMax(AggregateFunctionFactory & factory)
{
    factory.registerFunction("max", createAggregateFunctionMax, AggregateFunctionFactory::CaseInsensitive);

    /// The functions below depend on the order of data.
    AggregateFunctionProperties properties = { .returns_default_when_only_null = false, .is_order_dependent = true };
    factory.registerFunction("argMax", { createAggregateFunctionArgMax, properties });
}

}
