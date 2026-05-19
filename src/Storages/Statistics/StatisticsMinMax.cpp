#include <Storages/Statistics/StatisticsMinMax.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/convertFieldToType.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/FieldVisitorToString.h>


namespace DB
{


StatisticsMinMax::StatisticsMinMax(const SingleStatisticsDescription & description, const DataTypePtr & data_type_)
    : IStatistics(description)
    , data_type(removeNullable(data_type_))
{
}

StatisticsMinMax::StatisticsMinMax(Field min_, Field max_, UInt64 row_count_)
    : IStatistics(SingleStatisticsDescription(StatisticsType::MinMax, nullptr, false))
    , min(std::move(min_))
    , max(std::move(max_))
    , row_count(row_count_)
{
}

void StatisticsMinMax::build(const ColumnPtr & column)
{
    Field min_field;
    Field max_field;

    column->getExtremes(min_field, max_field, 0, column->size());

    if (!min_field.isNull())
    {
        if (min.isNull() || min_field < min)
            min = min_field;
    }

    if (!max_field.isNull())
    {
        if (max.isNull() || max_field > max)
            max = max_field;
    }

    row_count += column->size();
}

void StatisticsMinMax::merge(const StatisticsPtr & other_stats)
{
    const StatisticsMinMax * other = typeid_cast<const StatisticsMinMax *>(other_stats.get());
    if (!other->min.isNull() && (min.isNull() || other->min < min))
        min = other->min;
    if (!other->max.isNull() && (max.isNull() || other->max > max))
        max = other->max;
    row_count += other->row_count;
}

void StatisticsMinMax::serialize(WriteBuffer & buf)
{
    writeIntBinary(row_count, buf);
    writeStringBinary(data_type->getName(), buf);
    writeFieldBinary(min, buf);
    writeFieldBinary(max, buf);
}

void StatisticsMinMax::deserialize(ReadBuffer & buf, StatisticsFileVersion version)
{
    readIntBinary(row_count, buf);

    if (version == StatisticsFileVersion::V1)
    {
        /// V1 format: min and max were stored as Float64
        Float64 min_val;
        Float64 max_val;
        readFloatBinary(min_val, buf);
        readFloatBinary(max_val, buf);
        min = min_val;
        max = max_val;
        return;
    }

    /// V2+ format: type name followed by Field-typed min and max
    String stored_type_name;
    readStringBinary(stored_type_name, buf);
    min = readFieldBinary(buf);
    max = readFieldBinary(buf);

    if (stored_type_name != data_type->getName())
    {
        /// Column type has changed — try to convert min/max to the new type
        auto stored_type = DataTypeFactory::instance().get(stored_type_name);
        if (!min.isNull())
        {
            Field converted = convertFieldToType(min, *data_type, stored_type.get());
            min = std::move(converted); /// null on conversion failure → effectively resets the bound
        }
        if (!max.isNull())
        {
            Field converted = convertFieldToType(max, *data_type, stored_type.get());
            max = std::move(converted);
        }
    }
}

namespace
{

/// Maps each integer type to a wider type for safe subtraction without overflow.
template <typename T> struct WiderIntType { using type = T; };
template <> struct WiderIntType<UInt64>  { using type = UInt128; };
template <> struct WiderIntType<Int64>   { using type = Int128; };
template <> struct WiderIntType<UInt128> { using type = UInt256; };
template <> struct WiderIntType<Int128>  { using type = Int256; };

/// Computes (v - mn) / (mx - mn) * row_count as Float64.
/// For integer types, widens to a larger type before subtracting to reduce precision
/// loss when converting to Float64 (important for large integers like UInt64, Int64, etc.).
/// Assumes mn <= v <= mx and mn < mx.
template <typename T>
Float64 interpolateLinear(Field val, Field min, Field max, UInt64 row_count)
{
    T v = val.safeGet<T>();
    T mn = min.safeGet<T>();
    T mx = max.safeGet<T>();
    if (v < mn) return 0.0;
    if (v > mx) return static_cast<Float64>(row_count);
    if (mn == mx) return (v == mx) ? static_cast<Float64>(row_count) : 0.0;
    using W = typename WiderIntType<T>::type;
    return static_cast<Float64>(static_cast<W>(v) - static_cast<W>(mn))
         / static_cast<Float64>(static_cast<W>(mx) - static_cast<W>(mn))
         * static_cast<Float64>(row_count);
}

}

std::optional<Float64> StatisticsMinMax::estimateLess(const Field & val) const
{
    if (row_count == 0 || min.isNull() || max.isNull())
        return std::nullopt;

    /// If all three fields share the same numeric type, use native arithmetic to
    /// preserve precision (especially important for large integers like UInt64, Int64).
    if (val.getType() == min.getType() && val.getType() == max.getType())
    {
        switch (val.getType())
        {
            case Field::Types::UInt64:  return interpolateLinear<UInt64>(val, min, max, row_count);
            case Field::Types::Int64:   return interpolateLinear<Int64>(val, min, max, row_count);
            case Field::Types::UInt128: return interpolateLinear<UInt128>(val, min, max, row_count);
            case Field::Types::Int128:  return interpolateLinear<Int128>(val, min, max, row_count);
            case Field::Types::UInt256: return interpolateLinear<UInt256>(val, min, max, row_count);
            case Field::Types::Int256:  return interpolateLinear<Int256>(val, min, max, row_count);
            case Field::Types::Float64: return interpolateLinear<Float64>(val, min, max, row_count);
            default: break;
        }
    }

    /// Fallback: convert to Float64 (e.g. for mismatched types or Decimal).
    /// tryConvertToFloat64 handles conversion errors internally and returns nullopt on failure.
    auto val_as_float = StatisticsUtils::tryConvertToFloat64(val, data_type);
    auto min_as_float = StatisticsUtils::tryConvertToFloat64(min, data_type);
    auto max_as_float = StatisticsUtils::tryConvertToFloat64(max, data_type);
    if (!val_as_float || !min_as_float || !max_as_float)
        return std::nullopt;
    return interpolateLinear<Float64>(*val_as_float, *min_as_float, *max_as_float, row_count);
}

String StatisticsMinMax::getNameForLogs() const
{
    return fmt::format("MinMax: ({}, {})", applyVisitor(FieldVisitorToString(), min), applyVisitor(FieldVisitorToString(), max));
}

bool minMaxStatisticsValidator(const SingleStatisticsDescription & /*description*/, const DataTypePtr & data_type)
{
    auto inner_data_type = removeNullable(data_type);
    inner_data_type = removeLowCardinalityAndNullable(inner_data_type);
    return inner_data_type->isValueRepresentedByNumber();
}

StatisticsPtr minMaxStatisticsCreator(const SingleStatisticsDescription & description, const DataTypePtr & data_type)
{
    return std::make_shared<StatisticsMinMax>(description, data_type);
}

}
