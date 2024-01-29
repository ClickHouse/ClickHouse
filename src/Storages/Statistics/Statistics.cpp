#include <optional>
#include <numeric>

#include <DataTypes/DataTypeNullable.h>
#include <Storages/Statistics/Statistics.h>
#include <Storages/Statistics/Estimator.h>
#include <Storages/Statistics/TDigestStatistic.h>
#include <Storages/Statistics/UniqStatistic.h>
#include <Storages/StatisticsDescription.h>
#include <Storages/ColumnsDescription.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
    extern const int ILLEGAL_STATISTIC;
}

enum StatisticsFileVersion : UInt16
{
    V0 = 0,
};

/// Version / bitmask of statistics / data of statistics /

ColumnStatistics::ColumnStatistics(const StatisticsDescription & stats_desc_)
    : stats_desc(stats_desc_), counter(0)
{
}

void ColumnStatistics::update(const ColumnPtr & column)
{
    counter += column->size();
    for (auto iter : stats)
    {
        iter.second->update(column);
    }
}

Float64 ColumnStatistics::estimateLess(Float64 val) const
{
    if (stats.contains(TDigest))
        return std::static_pointer_cast<TDigestStatistic>(stats.at(TDigest))->estimateLess(val);
    return counter * ConditionEstimator::default_normal_cond_factor;
}

Float64 ColumnStatistics::estimateGreater(Float64 val) const
{
    return counter - estimateLess(val);
}

Float64 ColumnStatistics::estimateEqual(Float64 val) const
{
    if (stats.contains(Uniq) && stats.contains(TDigest))
    {
        auto uniq_static = std::static_pointer_cast<UniqStatistic>(stats.at(Uniq));
        Int64 ndv = uniq_static->getNDV();
        if (ndv < 2048)
        {
            auto tdigest_static = std::static_pointer_cast<TDigestStatistic>(stats.at(TDigest));
            return tdigest_static->estimateEqual(val);
        }
    }
    if (val < - ConditionEstimator::threshold || val > ConditionEstimator::threshold)
        return counter * ConditionEstimator::default_normal_cond_factor;
    else
        return counter * ConditionEstimator::default_good_cond_factor;
}

void ColumnStatistics::serialize(WriteBuffer & buf)
{
    writeIntBinary(V0, buf);
    UInt64 stat_types_mask = 0;
    for (const auto & [type, _]: stats)
    {
        stat_types_mask |= 1 << type;
    }
    writeIntBinary(stat_types_mask, buf);
    /// We write some basic statistics
    writeIntBinary(counter, buf);
    /// We write complex statistics
    for (const auto & [type, stat_ptr]: stats)
    {
        stat_ptr->serialize(buf);
    }
}

void ColumnStatistics::deserialize(ReadBuffer &buf)
{
    UInt16 version;
    readIntBinary(version, buf);
    if (version != V0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown file format version: {}", version);

    UInt64 stat_types_mask = 0;
    readIntBinary(stat_types_mask, buf);
    readIntBinary(counter, buf);
    for (auto it = stats.begin(); it != stats.end();)
    {
        if (!(stat_types_mask & 1 << (it->first)))
        {
            stats.erase(it ++);
        }
        else
        {
            it->second->deserialize(buf);
            ++ it;
        }
    }
}

void MergeTreeStatisticsFactory::registerCreator(StatisticType stat_type, Creator creator)
{
    if (!creators.emplace(stat_type, std::move(creator)).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreeStatisticsFactory: the statistic creator type {} is not unique", stat_type);
}

void MergeTreeStatisticsFactory::registerValidator(StatisticType stat_type, Validator validator)
{
    if (!validators.emplace(stat_type, std::move(validator)).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreeStatisticsFactory: the statistic validator type {} is not unique", stat_type);

}

StatisticPtr TDigestCreator(const StatisticDescription & stat, DataTypePtr)
{
    return StatisticPtr(new TDigestStatistic(stat));
}

void TDigestValidator(const StatisticDescription &, DataTypePtr data_type)
{
    data_type = removeNullable(data_type);
    if (!data_type->isValueRepresentedByNumber())
        throw Exception(ErrorCodes::ILLEGAL_STATISTIC, "TDigest does not support type {}", data_type->getName());
}

void UniqValidator(const StatisticDescription &, DataTypePtr)
{
    /// TODO(hanfei): check something
}

StatisticPtr UniqCreator(const StatisticDescription & stat, DataTypePtr data_type)
{
    return StatisticPtr(new UniqStatistic(stat, data_type));
}

MergeTreeStatisticsFactory::MergeTreeStatisticsFactory()
{
    registerCreator(TDigest, TDigestCreator);
    registerCreator(Uniq, UniqCreator);
    registerValidator(TDigest, TDigestValidator);
    registerValidator(Uniq, UniqValidator);
}

MergeTreeStatisticsFactory & MergeTreeStatisticsFactory::instance()
{
    static MergeTreeStatisticsFactory instance;
    return instance;
}

void MergeTreeStatisticsFactory::validate(const StatisticsDescription & stats, DataTypePtr data_type) const
{
    for (const auto & [type, desc] : stats.stats)
    {
        auto it = validators.find(type);
        if (it == validators.end())
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown Statistic type '{}'", type);
        }
        it->second(desc, data_type);
    }
}

ColumnStatisticsPtr MergeTreeStatisticsFactory::get(const StatisticsDescription & stats) const
{
    ColumnStatisticsPtr column_stat = std::make_shared<ColumnStatistics>(stats);
    for (const auto & [type, desc] : stats.stats)
    {
        auto it = creators.find(type);
        if (it == creators.end())
        {
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                    "Unknown Statistic type '{}'. Available types: tdigest", type);
        }
        auto stat_ptr = (it->second)(desc, stats.data_type);
        column_stat->stats[type] = stat_ptr;
    }
    return column_stat;
}

std::vector<ColumnStatisticsPtr> MergeTreeStatisticsFactory::getMany(const ColumnsDescription & columns) const
{
    std::vector<ColumnStatisticsPtr> result;
    for (const auto & col : columns)
        if (!col.stats.empty())
            result.push_back(get(col.stats));
    return result;
}

}
