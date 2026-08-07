#include <Storages/Statistics/StatisticsMinMax.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/convertFieldToType.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
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

    /// getExtremes skips NaN, so a NaN in this block would be invisible in [min, max]. Record it
    /// separately so part pruning keeps the part under a negated float range (issue #106533).
    if (!has_nan)
        has_nan = StatisticsUtils::columnHasNaN(column);

    row_count += column->size();
}

void StatisticsMinMax::merge(const StatisticsPtr & other_stats)
{
    const StatisticsMinMax * other = typeid_cast<const StatisticsMinMax *>(other_stats.get());
    if (!other->min.isNull() && (min.isNull() || other->min < min))
        min = other->min;
    if (!other->max.isNull() && (max.isNull() || other->max > max))
        max = other->max;
    has_nan |= other->has_nan;
    row_count += other->row_count;
}

void StatisticsMinMax::serialize(WriteBuffer & buf)
{
    writeIntBinary(row_count, buf);
    writeStringBinary(data_type->getName(), buf);
    writeFieldBinary(min, buf);
    writeFieldBinary(max, buf);
    /// Written unconditionally to keep one layout; a reader that stops after `max` skips this byte
    /// via the per-stat size prefix in the enclosing ColumnStatistics framing.
    writeBinary(has_nan, buf);
}

StatisticsFileVersion StatisticsMinMax::requiredFileVersion() const
{
    /// Only a float can hold a NaN; for any other type an older reader derives the same false.
    if (data_type && isFloat(removeLowCardinality(data_type)))
        return StatisticsFileVersion::V5;
    return StatisticsFileVersion::V4;
}

void StatisticsMinMax::deserialize(ReadBuffer & buf, StatisticsFileVersion version)
{
    readIntBinary(row_count, buf);

    if (version == StatisticsFileVersion::V1)
    {
        /// V1 format: min and max were stored as Float64
        Float64 min_val = 0;
        Float64 max_val = 0;
        readFloatBinary(min_val, buf);
        readFloatBinary(max_val, buf);
        min = min_val;
        max = max_val;
        /// V1 predates the `has_nan` flag too, so a float part with a hidden NaN reads a finite
        /// [min, max]. Apply the same conservative fallback as the V2..V4 path below.
        if (isFloat(removeLowCardinality(data_type)))
            has_nan = true;
        return;
    }

    /// V2+ format: type name followed by Field-typed min and max
    String stored_type_name;
    readStringBinary(stored_type_name, buf);
    if (stored_type_name != data_type->getName())
    {
        return;
    }
    min = readFieldBinary(buf);
    max = readFieldBinary(buf);

    /// V5+ always appends `has_nan` right after `max`, so read exactly that one byte. Reading via
    /// eof() would be wrong: `buf` is the shared file buffer that may still hold later stats.
    if (version >= StatisticsFileVersion::V5)
    {
        readBinary(has_nan, buf);
    }
    else if (isFloat(removeLowCardinality(data_type)))
    {
        /// No flag stored: `[1.0, nan, 3.0]` was written with a finite [min, max] hiding the NaN, so
        /// assume one may be there. A conservative keep, never a wrong skip.
        has_nan = true;
    }
}

std::optional<Float64> StatisticsMinMax::estimateLess(const Field & val) const
{
    if (row_count == 0 || min.isNull() || max.isNull())
        return std::nullopt;

    return StatisticsUtils::interpolateLessLinear(val, min, max, row_count, data_type);
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
