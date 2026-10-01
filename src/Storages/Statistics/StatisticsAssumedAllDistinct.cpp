#include <Storages/Statistics/StatisticsAssumedAllDistinct.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/Statistics/Statistics.h>
#include <Storages/Statistics/StatisticsUniqBuildProbe.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace
{

UInt64 countNonNullRowsForAssumedAllDistinct(const ColumnPtr & column)
{
    UInt64 result = 0;
    for (size_t i = 0, size = column->size(); i < size; ++i)
        if (!column->isNullAt(i))
            ++result;
    return result;
}

class StatisticsAssumedAllDistinct final : public IStatistics
{
public:
    explicit StatisticsAssumedAllDistinct(const SingleStatisticsDescription & description, UInt64 cardinality_)
        : IStatistics(description)
        , cardinality(cardinality_)
    {
    }

    void build(const ColumnPtr & column) override { cardinality += countNonNullRowsForAssumedAllDistinct(column); }

    void merge(const StatisticsPtr & other_stats) override { cardinality += other_stats->estimateCardinality(); }

    void serialize(WriteBuffer & buf) override { writeIntBinary(cardinality, buf); }

    void deserialize(ReadBuffer & buf, StatisticsFileVersion /*version*/) override { readIntBinary(cardinality, buf); }

    UInt64 estimateCardinality() const override { return cardinality; }

    String getNameForLogs() const override { return stat.getTypeName() + "(assumed_all_distinct) : " + std::to_string(cardinality); }

private:
    UInt64 cardinality = 0;
};

}

StatisticsPtr createAssumedAllDistinctStatistics(const SingleStatisticsDescription & description, UInt64 cardinality)
{
    return std::make_shared<StatisticsAssumedAllDistinct>(description, cardinality);
}

bool isAssumedAllDistinctStatistics(const IStatistics & statistics)
{
    return typeid_cast<const StatisticsAssumedAllDistinct *>(&statistics) != nullptr;
}

StatisticsType getSerializedStatisticsType(const IStatistics & statistics)
{
    if (isAssumedAllDistinctStatistics(statistics))
        return getAssumedAllDistinctSerializedStatisticsType(statistics.getDescription().type);
    return statistics.getDescription().type;
}

bool supportsAssumedAllDistinctStatistics(const SingleStatisticsDescription & description, const DataTypePtr & data_type)
{
    return isUniqLikeStatisticsType(description.type) && dataTypeSupportsUniqStatistics(data_type);
}

}
