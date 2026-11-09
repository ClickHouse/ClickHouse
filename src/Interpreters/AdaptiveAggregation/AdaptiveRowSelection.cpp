#include <Interpreters/AdaptiveAggregation/AdaptiveRowSelection.h>

#include <optional>
#include <type_traits>

#include <AggregateFunctions/AggregateFunctionCount.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnsNumber.h>
#include <Common/PODArray.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <Core/callOnTypeIndex.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeIPv4andIPv6.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTime.h>
#include <DataTypes/DataTypeTime64.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{

namespace
{

/// The value types by which a row selection orders the rows (see `orderedImage`): the integers and the
/// decimals of up to 64 bits, and `IPv4`. The types stored as one of them compare as it within a column:
/// `Bool`, `Enum8`, `Enum16`, the dates, `Time` and the intervals are integers, and `DateTime64` and `Time64`
/// decimals, whose scale is the same for a column.
template <typename T>
concept RowSelectionOrderingValue
    = ((is_integer<T> || is_decimal<T>) && sizeof(T) <= sizeof(Int64)) || std::is_same_v<T, IPv4>;

/// Calls `f` with `std::type_identity` of the value type of `type` when that type orders a row selection, and
/// returns whether it does.
template <typename F>
bool withOrderingValueType(TypeIndex type, F && f)
{
    return callOnIndexAndDataType<void>(type, [&](const auto & types)
    {
        using DataType = typename std::decay_t<decltype(types)>::LeftType;
        if constexpr (requires { typename DataType::FieldType; })
        {
            using T = typename DataType::FieldType;
            if constexpr (RowSelectionOrderingValue<T>)
            {
                f(std::type_identity<T>{});
                return true;
            }
            else
                return false;
        }
        else
            return false;
    });
}

/// An ordering value as an `Int64` in the same order: an unsigned 64-bit value with its top bit flipped, a
/// decimal by its raw value, an `IPv4` by its `UInt32`, and the other types as they are.
template <RowSelectionOrderingValue T>
Int64 ALWAYS_INLINE orderedImage(T value)
{
    if constexpr (std::is_same_v<T, UInt64>)
        return static_cast<Int64>(value ^ (1ULL << 63));
    else if constexpr (is_decimal<T>)
        return static_cast<Int64>(value.value);
    else if constexpr (std::is_same_v<T, IPv4>)
        return static_cast<Int64>(value.toUnderType());
    else
        return static_cast<Int64>(value);
}

}

/// A `Nullable` argument skips its own `NULL` rows, which can make the calls select different rows, and the
/// combinators change what is selected, so both keep one record per missed row.
std::unique_ptr<const AdaptiveRowSelection> adaptiveRowSelectionOf(const AggregateDescriptions & aggregates)
{
    std::optional<AdaptiveRowSelection> selection;
    std::vector<size_t> count_aggregates;
    for (size_t i = 0; i < aggregates.size(); ++i)
    {
        const auto & function = *aggregates[i].function;
        for (const auto & type : function.getArgumentTypes())
            if (isNullableOrLowCardinalityNullable(type))
                return {};

        if (typeid_cast<const AggregateFunctionCount *>(&function))
        {
            count_aggregates.push_back(i);
            continue;
        }

        const String name = function.getName();
        size_t argument = 0;
        bool largest = true;
        if (name == "argMax" || name == "argMin")
        {
            argument = 1;
            largest = name == "argMax";
        }
        else if (name == "max" || name == "min")
        {
            largest = name == "max";
        }
        else if (name == "any")
        {
            /// Any row of the group serves `any`, the best one too.
            continue;
        }
        else
        {
            return {};
        }

        if (!selection)
        {
            selection = AdaptiveRowSelection{
                .ordering_aggregate = i, .ordering_argument = argument, .largest = largest, .count_aggregates = {}};
            continue;
        }
        const auto & ordering_name = aggregates[selection->ordering_aggregate].argument_names[selection->ordering_argument];
        if (largest != selection->largest || aggregates[i].argument_names[argument] != ordering_name)
            return {};
    }

    if (!selection)
        return {};
    const auto & ordering_type
        = aggregates[selection->ordering_aggregate].function->getArgumentTypes()[selection->ordering_argument];
    if (!withOrderingValueType(ordering_type->getTypeId(), [](auto) {}))
        return {};
    selection->ordering_type = ordering_type->getTypeId();
    selection->count_aggregates = std::move(count_aggregates);
    return std::make_unique<const AdaptiveRowSelection>(std::move(*selection));
}

void fillRowSelectionRanks(
    const AdaptiveRowSelection & selection, const IColumn & column, size_t row_begin, size_t row_end, PaddedPODArray<Int64> & ranks)
{
    ranks.resize(row_end);
    /// The complement, `x ^ -1`, reverses the order of the images exactly, so the best row has the largest
    /// rank.
    const Int64 complement = selection.largest ? 0 : -1;
    const bool ordered = withOrderingValueType(selection.ordering_type, [&]<typename T>(std::type_identity<T>)
    {
        using OrderingColumn = ColumnVectorOrDecimal<T>;
        if (const auto * sparse = typeid_cast<const ColumnSparse *>(&column))
        {
            /// The values of a sparse column start with the default, which `getValueIndex` gives the rows
            /// without a value.
            const auto & values = assert_cast<const OrderingColumn &>(sparse->getValuesColumn()).getData();
            auto iterator = sparse->getIterator(row_begin);
            for (size_t i = row_begin; i < row_end; ++i, ++iterator)
                ranks[i] = orderedImage(values[iterator.getValueIndex()]) ^ complement;
            return;
        }
        const auto & data = assert_cast<const OrderingColumn &>(column).getData();
        for (size_t i = row_begin; i < row_end; ++i)
            ranks[i] = orderedImage(data[i]) ^ complement;
    });
    chassert(ordered);
}

}
