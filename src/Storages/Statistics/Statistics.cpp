#include <optional>
#include <numeric>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>
#include <Storages/Statistics/CountMinSketchStatistics.h>
#include <Storages/Statistics/Statistics.h>
#include <Storages/Statistics/TDigestStatistics.h>
#include <Storages/Statistics/UniqStatistics.h>
#include <Storages/StatisticsDescription.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
}

/// Version / bitmask of statistics / data of statistics /
enum StatisticsFileVersion : UInt16
{
    V0 = 0,
};

std::optional<Float64> IStatistics::getFloat64(const Field & f)
{
    const auto type = f.getType();
    Float64 value;
    if (type == Field::Types::Int64)
        value = f.get<Int64>();
    else if (type == Field::Types::UInt64)
        value = f.get<UInt64>();
    else if (type == Field::Types::Float64)
        value = f.get<Float64>();
    else
        return {};
    return value;
}

std::optional<String> IStatistics::getString(const Field & f)
{
    if (f.getType() == Field::Types::String)
        return f.get<String>();
    return {};
}

IStatistics::IStatistics(const SingleStatisticsDescription & stat_) : stat(stat_) {}

ColumnStatistics::ColumnStatistics(const ColumnStatisticsDescription & stats_desc_)
    : stats_desc(stats_desc_), rows(0)
{
}

void ColumnStatistics::update(const ColumnPtr & column)
{
    rows += column->size();
    for (const auto & iter : stats)
    {
        iter.second->update(column);
    }
}

Float64 ColumnStatistics::estimateLess(Float64 val) const
{
    if (stats.contains(StatisticsType::TDigest))
        return std::static_pointer_cast<TDigestStatistics>(stats.at(StatisticsType::TDigest))->estimateLess(val);
    return rows * ConditionSelectivityEstimator::default_normal_cond_factor;
}

Float64 ColumnStatistics::estimateGreater(Float64 val) const
{
    return rows - estimateLess(val);
}

Float64 ColumnStatistics::estimateEqual(Field val) const
{
    auto float_val = IStatistics::getFloat64(val);
    if (float_val && stats.contains(StatisticsType::Uniq) && stats.contains(StatisticsType::TDigest))
    {
        auto uniq_static = std::static_pointer_cast<UniqStatistics>(stats.at(StatisticsType::Uniq));
        /// 2048 is the default number of buckets in TDigest. In this case, TDigest stores exactly one value (with many rows)
        /// for every bucket.
        if (uniq_static->getCardinality() < 2048)
        {
            auto tdigest_static = std::static_pointer_cast<TDigestStatistics>(stats.at(StatisticsType::TDigest));
            return tdigest_static->estimateEqual(float_val.value());
        }
    }
#if USE_DATASKETCHES
    if (stats.contains(StatisticsType::CountMinSketch))
    {
        auto count_min_sketch_static = std::static_pointer_cast<CountMinSketchStatistics>(stats.at(StatisticsType::CountMinSketch));
        if (!count_min_sketch_static->checkType(val))
            return rows * ConditionSelectivityEstimator::default_normal_cond_factor;
        else
            return count_min_sketch_static->estimateEqual(val);
    }
#endif
    if (val < - ConditionSelectivityEstimator::threshold || val > ConditionSelectivityEstimator::threshold)
        return rows * ConditionSelectivityEstimator::default_normal_cond_factor;
    else
        return rows * ConditionSelectivityEstimator::default_good_cond_factor;
}

void ColumnStatistics::serialize(WriteBuffer & buf)
{
    writeIntBinary(V0, buf);
    UInt64 stat_types_mask = 0;
    for (const auto & [type, _]: stats)
        stat_types_mask |= 1 << UInt8(type);
    writeIntBinary(stat_types_mask, buf);
    /// We write some basic statistics
    writeIntBinary(rows, buf);
    /// We write complex statistics
    for (const auto & [type, stat_ptr]: stats)
        stat_ptr->serialize(buf);
}

void ColumnStatistics::deserialize(ReadBuffer &buf)
{
    UInt16 version;
    readIntBinary(version, buf);
    if (version != V0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown file format version: {}", version);

    UInt64 stat_types_mask = 0;
    readIntBinary(stat_types_mask, buf);
    readIntBinary(rows, buf);
    for (auto it = stats.begin(); it != stats.end();)
    {
        if (!(stat_types_mask & 1 << UInt8(it->first)))
        {
            stats.erase(it++);
        }
        else
        {
            it->second->deserialize(buf);
            ++it;
        }
    }
}

String ColumnStatistics::getFileName() const
{
    return STATS_FILE_PREFIX + columnName();
}

const String & ColumnStatistics::columnName() const
{
    return stats_desc.column_name;
}

UInt64 ColumnStatistics::rowCount() const
{
    return rows;
}

void MergeTreeStatisticsFactory::registerCreator(StatisticsType stats_type, Creator creator)
{
    if (!creators.emplace(stats_type, std::move(creator)).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreeStatisticsFactory: the statistics creator type {} is not unique", stats_type);
}

void MergeTreeStatisticsFactory::registerValidator(StatisticsType stats_type, Validator validator)
{
    if (!validators.emplace(stats_type, std::move(validator)).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreeStatisticsFactory: the statistics validator type {} is not unique", stats_type);

}

MergeTreeStatisticsFactory::MergeTreeStatisticsFactory()
{
    registerCreator(StatisticsType::TDigest, TDigestCreator);
    registerCreator(StatisticsType::Uniq, UniqCreator);
    registerValidator(StatisticsType::TDigest, TDigestValidator);
    registerValidator(StatisticsType::Uniq, UniqValidator);
#if USE_DATASKETCHES
    registerCreator(StatisticsType::CountMinSketch, CountMinSketchCreator);
    registerValidator(StatisticsType::CountMinSketch, CountMinSketchValidator);
#endif
}

MergeTreeStatisticsFactory & MergeTreeStatisticsFactory::instance()
{
    static MergeTreeStatisticsFactory instance;
    return instance;
}

void MergeTreeStatisticsFactory::validate(const ColumnStatisticsDescription & stats, DataTypePtr data_type) const
{
    for (const auto & [type, desc] : stats.types_to_desc)
    {
        auto it = validators.find(type);
        if (it == validators.end())
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown Statistic type '{}'", type);
        }
        it->second(desc, data_type);
    }
}

ColumnStatisticsPtr MergeTreeStatisticsFactory::get(const ColumnStatisticsDescription & stats) const
{
    ColumnStatisticsPtr column_stat = std::make_shared<ColumnStatistics>(stats);
    for (const auto & [type, desc] : stats.types_to_desc)
    {
        auto it = creators.find(type);
        if (it == creators.end())
        {
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                    "Unknown Statistic type '{}'. Available types: tdigest, uniq", type);
        }
        auto stat_ptr = (it->second)(desc, stats.data_type);
        column_stat->stats[type] = stat_ptr;
    }
    return column_stat;
}

ColumnsStatistics MergeTreeStatisticsFactory::getMany(const ColumnsDescription & columns) const
{
    ColumnsStatistics result;
    for (const auto & col : columns)
        if (!col.statistics.empty())
            result.push_back(get(col.statistics));
    return result;
}

}
