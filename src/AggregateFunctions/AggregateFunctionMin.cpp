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
class AggregateFunctionMin final : public IAggregateFunctionDataHelper<Data, AggregateFunctionMin<Data>>
{
private:
    SerializationPtr serialization;

public:
    explicit AggregateFunctionMin(const DataTypePtr & type)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionMin<Data>>({type}, {}, type), serialization(type->getDefaultSerialization())
    {
        if (!type->isComparable())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of aggregate function {} because the values of that data type are not comparable",
                type->getName(),
                getName());
    }

    String getName() const override { return "min"; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        this->data(place).setIfSmaller(*columns[0], row_num, arena);
    }

    void addManyDefaults(AggregateDataPtr __restrict place, const IColumn ** columns, size_t, Arena * arena) const override
    {
        this->data(place).setIfSmaller(*columns[0], 0, arena);
    }

    /// Specializations for native numeric types
    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** __restrict columns,
        Arena * arena,
        ssize_t if_argument_pos) const override
    {
        if (if_argument_pos >= 0)
        {
            const auto & if_map = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            this->data(place).setSmallestNotNullIf(*columns[0], nullptr, if_map.data(), row_begin, row_end, arena);
        }
        else
        {
            this->data(place).setSmallest(*columns[0], row_begin, row_end, arena);
        }
    }

    void addBatchSinglePlaceNotNull(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** __restrict columns,
        const UInt8 * __restrict null_map,
        Arena * arena,
        ssize_t if_argument_pos) const override
    {
        if (if_argument_pos >= 0)
        {
            const auto & if_map = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            this->data(place).setSmallestNotNullIf(*columns[0], null_map, if_map.data(), row_begin, row_end, arena);
        }
        else
        {
            this->data(place).setSmallestNotNullIf(*columns[0], null_map, nullptr, row_begin, row_end, arena);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        this->data(place).setIfSmaller(this->data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).write(buf, *serialization);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        this->data(place).read(buf, *serialization, arena);
    }

    bool allocatesMemoryInArena() const override { return Data::allocatesMemoryInArena(); }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        this->data(place).insertResultInto(to);
    }
};

//// NOLINTBEGIN(bugprone-macro-parentheses)
//#define SPECIALIZE(TYPE) \
//    template <> \
//    void AggregateFunctionsSingleValueMin<SingleValueDataFixed<TYPE>>::addBatchSinglePlace( \
//        size_t row_begin, \
//        size_t row_end, \
//        AggregateDataPtr __restrict place, \
//        const IColumn ** __restrict columns, \
//        Arena *, \
//        ssize_t if_argument_pos) const \
//    { \
//        const auto & column = assert_cast<const DB::AggregateFunctionMinData<SingleValueDataFixed<TYPE>>::ColVecType &>(*columns[0]); \
//        std::optional<TYPE> opt; \
//        if (if_argument_pos >= 0) \
//        { \
//            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData(); \
//            opt = findExtremeMinIf(column.getData().data(), flags.data(), row_begin, row_end); \
//        } \
//        else \
//            opt = findExtremeMin(column.getData().data(), row_begin, row_end); \
//        if (opt.has_value()) \
//            this->data(place).setIfSmaller(opt.value()); \
//    }
//// NOLINTEND(bugprone-macro-parentheses)
//
//FOR_BASIC_NUMERIC_TYPES(SPECIALIZE)
//#undef SPECIALIZE
//
//template <typename Data>
//void AggregateFunctionsSingleValueMin<Data>::addBatchSinglePlace(
//    size_t row_begin,
//    size_t row_end,
//    AggregateDataPtr __restrict place,
//    const IColumn ** __restrict columns,
//    Arena * arena,
//    ssize_t if_argument_pos) const
//{
////    if constexpr (!is_any_of<typename Data::Impl, SingleValueDataString, SingleValueDataGeneric>)
////    {
////        /// Leave other numeric types (large integers, decimals, etc) to keep doing the comparison as it's
////        /// faster than doing a permutation
////        return Parent::addBatchSinglePlace(row_begin, row_end, place, columns, arena, if_argument_pos);
////    }
//
//    constexpr int nan_direction_hint = 1;
//    auto const & column = *columns[0];
//    if (if_argument_pos >= 0)
//    {
//        size_t index = row_begin;
//        const auto & if_flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
//        while (if_flags[index] == 0 && index < row_end)
//            index++;
//        if (index >= row_end)
//            return;
//
//        for (size_t i = index + 1; i < row_end; i++)
//        {
//            if ((if_flags[i] != 0) && (column.compareAt(i, index, column, nan_direction_hint) < 0))
//                index = i;
//        }
//        this->data(place).setIfSmaller(column, index, arena);
//    }
//    else
//    {
//        if (row_begin >= row_end)
//            return;
//
//        /// TODO: Introduce row_begin and row_end to getPermutation
//        if (row_begin != 0 || row_end != column.size())
//        {
//            size_t index = row_begin;
//            for (size_t i = index + 1; i < row_end; i++)
//            {
//                if (column.compareAt(i, index, column, nan_direction_hint) < 0)
//                    index = i;
//            }
//            this->data(place).setIfSmaller(column, index, arena);
//        }
//        else
//        {
//            constexpr IColumn::PermutationSortDirection direction = IColumn::PermutationSortDirection::Ascending;
//            constexpr IColumn::PermutationSortStability stability = IColumn::PermutationSortStability::Unstable;
//            IColumn::Permutation permutation;
//            constexpr UInt64 limit = 1;
//            column.getPermutation(direction, stability, limit, nan_direction_hint, permutation);
//            this->data(place).setIfSmaller(column, permutation[0], arena);
//        }
//    }
//}
//
//// NOLINTBEGIN(bugprone-macro-parentheses)
//#define SPECIALIZE(TYPE) \
//    template <> \
//    void AggregateFunctionsSingleValueMin<typename DB::AggregateFunctionMinData<SingleValueDataFixed<TYPE>>>::addBatchSinglePlaceNotNull( \
//        size_t row_begin, \
//        size_t row_end, \
//        AggregateDataPtr __restrict place, \
//        const IColumn ** __restrict columns, \
//        const UInt8 * __restrict null_map, \
//        Arena *, \
//        ssize_t if_argument_pos) const \
//    { \
//        const auto & column = assert_cast<const DB::AggregateFunctionMinData<SingleValueDataFixed<TYPE>>::ColVecType &>(*columns[0]); \
//        std::optional<TYPE> opt; \
//        if (if_argument_pos >= 0) \
//        { \
//            const auto * if_flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data(); \
//            auto final_flags = std::make_unique<UInt8[]>(row_end); \
//            for (size_t i = row_begin; i < row_end; ++i) \
//                final_flags[i] = (!null_map[i]) & !!if_flags[i]; \
//            opt = findExtremeMinIf(column.getData().data(), final_flags.get(), row_begin, row_end); \
//        } \
//        else \
//            opt = findExtremeMinNotNull(column.getData().data(), null_map, row_begin, row_end); \
//        if (opt.has_value()) \
//            this->data(place).setIfSmaller(opt.value()); \
//    }
//// NOLINTEND(bugprone-macro-parentheses)
//
//FOR_BASIC_NUMERIC_TYPES(SPECIALIZE)
//#undef SPECIALIZE
//
//template <typename Data>
//void AggregateFunctionsSingleValueMin<Data>::addBatchSinglePlaceNotNull(
//    size_t row_begin,
//    size_t row_end,
//    AggregateDataPtr __restrict place,
//    const IColumn ** __restrict columns,
//    const UInt8 * __restrict null_map,
//    Arena * arena,
//    ssize_t if_argument_pos) const
//{
////    if constexpr (!is_any_of<typename Data::Impl, SingleValueDataString, SingleValueDataGeneric>)
////    {
////        /// Leave other numeric types (large integers, decimals, etc) to keep doing the comparison as it's
////        /// faster than doing a permutation
////        return Parent::addBatchSinglePlaceNotNull(row_begin, row_end, place, columns, null_map, arena, if_argument_pos);
////    }
//
//    constexpr int nan_direction_hint = 1;
//    auto const & column = *columns[0];
//    if (if_argument_pos >= 0)
//    {
//        size_t index = row_begin;
//        const auto & if_flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
//        while ((if_flags[index] == 0 || null_map[index] != 0) && (index < row_end))
//            index++;
//        if (index >= row_end)
//            return;
//
//        for (size_t i = index + 1; i < row_end; i++)
//        {
//            if ((if_flags[i] != 0) && (null_map[index] == 0) && (column.compareAt(i, index, column, nan_direction_hint) < 0))
//                index = i;
//        }
//        this->data(place).setIfSmaller(column, index, arena);
//    }
//    else
//    {
//        size_t index = row_begin;
//        while ((null_map[index] != 0) && (index < row_end))
//            index++;
//        if (index >= row_end)
//            return;
//
//        for (size_t i = index + 1; i < row_end; i++)
//        {
//            if ((null_map[i] == 0) && (column.compareAt(i, index, column, nan_direction_hint) < 0))
//                index = i;
//        }
//        this->data(place).setIfSmaller(column, index, arena);
//    }
//}

AggregateFunctionPtr
createAggregateFunctionMin(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    return AggregateFunctionPtr(createAggregateFunctionSingleValue<AggregateFunctionMin>(name, argument_types, parameters, settings));
}

//AggregateFunctionPtr createAggregateFunctionArgMin(
//    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
//{
//    return AggregateFunctionPtr(createAggregateFunctionArgMinMax<AggregateFunctionMinData>(name, argument_types, parameters, settings));
//}
}

void registerAggregateFunctionsMin(AggregateFunctionFactory & factory)
{
    factory.registerFunction("min", createAggregateFunctionMin, AggregateFunctionFactory::CaseInsensitive);

    //    /// The functions below depend on the order of data.
    //    AggregateFunctionProperties properties = { .returns_default_when_only_null = false, .is_order_dependent = true };
    //    factory.registerFunction("argMin", { createAggregateFunctionArgMin, properties });
}

}
