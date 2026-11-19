#include <Storages/Statistics/StatisticsMinMax.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
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
        Float64 min_val = 0;
        Float64 max_val = 0;
        readFloatBinary(min_val, buf);
        readFloatBinary(max_val, buf);
        min = min_val;
        max = max_val;
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
