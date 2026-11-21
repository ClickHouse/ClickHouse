#include <Storages/Statistics/StatisticsMinMax.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>
#include <Common/FieldVisitorWriteBinary.h>
#include <Common/FieldVisitorToString.h>


namespace DB
{


StatisticsMinMax::StatisticsMinMax(const SingleStatisticsDescription & description, const DataTypePtr & data_type_)
    : IStatistics(description)
    , data_type(removeNullable(data_type_))
    , value_type(removeLowCardinalityAndNullable(data_type))
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

    StatisticsUtils::updateMin(min, min_field);
    StatisticsUtils::updateMax(max, max_field);

    row_count += column->size();
}

void StatisticsMinMax::merge(const StatisticsPtr & other_stats)
{
    const StatisticsMinMax * other = typeid_cast<const StatisticsMinMax *>(other_stats.get());
    StatisticsUtils::updateMin(min, other->min);
    StatisticsUtils::updateMax(max, other->max);
    row_count += other->row_count;
}

void StatisticsMinMax::serialize(WriteBuffer & buf, StatisticsFileVersion version)
{
    writeIntBinary(row_count, buf);
    if (version >= StatisticsFileVersion::V5)
    {
        StatisticsUtils::serializeBounds(min, max, *value_type, buf);
        return;
    }

    /// V2 - V4 format: type name followed by min and max in the `Field` encoding
    writeStringBinary(data_type->getName(), buf);
    writeFieldBinary(min, buf);
    writeFieldBinary(max, buf);
}

std::optional<UInt64> StatisticsMinMax::getSerializedSize(StatisticsFileVersion version) const
{
    if (version >= StatisticsFileVersion::V5)
        return sizeof(row_count) + StatisticsUtils::getSerializedBoundsSize(min, max, *value_type);

    const String type_name = data_type->getName();
    return sizeof(row_count)
        + getLengthOfVarUInt(type_name.size()) + type_name.size()
        + getFieldBinarySize(min)
        + getFieldBinarySize(max);
}

void StatisticsMinMax::deserialize(ReadBuffer & buf, StatisticsFileVersion version)
{
    readIntBinary(row_count, buf);

    if (version == StatisticsFileVersion::V1)
    {
        /// V1 format: min and max were stored as `Float64` for any column type. The bounds are not exact (the
        /// conversion to `Float64` may round a value of an integer or decimal type in either direction), so they
        /// can neither answer `min` and `max` nor prune, and they cannot be written with the serialization of the
        /// column type. They are skipped, and the statistics are dropped (see `ColumnStatistics::deserialize`).
        Float64 min_val = 0;
        Float64 max_val = 0;
        readFloatBinary(min_val, buf);
        readFloatBinary(max_val, buf);
        is_unusable = true;
        return;
    }

    if (version >= StatisticsFileVersion::V5)
    {
        StatisticsUtils::deserializeBounds(min, max, *value_type, buf);
        return;
    }

    /// V2 - V4 format: type name followed by min and max in the `Field` encoding
    String stored_type_name;
    readStringBinary(stored_type_name, buf);
    if (stored_type_name != data_type->getName())
    {
        /// Built for another column type: without the bounds, the statistics would exclude the values of this part.
        is_unusable = true;
        return;
    }
    min = readFieldBinary(buf);
    max = readFieldBinary(buf);

    /// Older versions rewrote the `Float64` bounds of `V1` unchanged in `V2` - `V4`. Such bounds are not exact,
    /// and they cannot be written with the serialization of the column type.
    if (!StatisticsUtils::isBoundOfType(min, *value_type) || !StatisticsUtils::isBoundOfType(max, *value_type))
    {
        min = Field();
        max = Field();
        is_unusable = true;
    }
}

std::optional<Float64> StatisticsMinMax::estimateLess(const Field & val) const
{
    if (row_count == 0 || min.isNull() || max.isNull())
        return std::nullopt;

    return StatisticsUtils::interpolateLessLinear(val, min, max, row_count, data_type);
}

size_t StatisticsMinMax::allocatedBytes() const
{
    return sizeof(*this) + fieldAllocatedBytes(min) + fieldAllocatedBytes(max);
}

String StatisticsMinMax::getNameForLogs() const
{
    return fmt::format("MinMax: ({}, {})", applyVisitor(FieldVisitorToString(), min), applyVisitor(FieldVisitorToString(), max));
}

bool minMaxStatisticsValidator(const SingleStatisticsDescription & /*description*/, const DataTypePtr & data_type)
{
    return canStatisticsTrackMinMax(data_type);
}

StatisticsPtr minMaxStatisticsCreator(const SingleStatisticsDescription & description, const DataTypePtr & data_type)
{
    return std::make_shared<StatisticsMinMax>(description, data_type);
}

}
