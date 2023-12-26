#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/HelpersMinMaxAny.h>
#include <AggregateFunctions/findNumeric.h>

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
    ALWAYS_INLINE inline void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** __restrict columns,
        Arena * arena,
        ssize_t if_argument_pos) const override;

    ALWAYS_INLINE inline void addBatchSinglePlaceNotNull(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** __restrict columns,
        const UInt8 * __restrict null_map,
        Arena * arena,
        ssize_t if_argument_pos) const override;
};

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
        opt = findNumericMaxIf(column.getData().data(), flags.data(), row_begin, row_end); \
    } \
    else \
        opt = findNumericMax(column.getData().data(), row_begin, row_end); \
    if (opt.has_value()) \
        this->data(place).changeIfGreater(opt.value()); \
}

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
    return Parent::addBatchSinglePlace(row_begin, row_end, place, columns, arena, if_argument_pos);
}

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
        opt = findNumericMaxIf(column.getData().data(), final_flags.get(), row_begin, row_end); \
    } \
    else \
        opt = findNumericMaxNotNull(column.getData().data(), null_map, row_begin, row_end); \
    if (opt.has_value()) \
        this->data(place).changeIfGreater(opt.value()); \
}

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
    return Parent::addBatchSinglePlaceNotNull(row_begin, row_end, place, columns, null_map, arena, if_argument_pos);
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
