#include <Storages/Statistics/Statistics.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>
#include <Storages/Statistics/StatisticsTDigest.h>
#include <Storages/Statistics/StatisticsUniq.h>
#include <Storages/StatisticsDescription.h>
#include <Storages/ColumnsDescription.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
}

enum StatisticsFileVersion : UInt16
{
    V0 = 0,
    V1 = 1, /// current format
};

IStatistics::IStatistics(const SingleStatisticsDescription & stat_)
    : stat(stat_)
{
}

std::optional<UInt64> IStatistics::estimateCardinality() const
{
    return {};
}

std::optional<Float64> IStatistics::estimateEqual(Float64 /*val*/) const
{
    return {};
}

std::optional<Float64> IStatistics::estimateLess(Float64 /*val*/) const
{
    return {};
}

ColumnStatistics::ColumnStatistics(const ColumnStatisticsDescription & stats_desc_)
    : stats_desc(stats_desc_)
{
}

void ColumnStatistics::update(const ColumnPtr & column)
{
    rows += column->size();
    for (const auto & stat : statistics)
        stat->update(column);
}

Float64 ColumnStatistics::estimateLess(Float64 val) const
{
    for (const auto & stat : statistics)
    {
        /// Return the estimation of the first statistics object which provides an estimation.
        /// TODO: In the (unlikely) case multiple statistics objects provide an estimation, we need some heuristics which
        ///       returns the most accurate estimation (this necessitates some notion of "estimation quality").
        auto estimation = stat->estimateLess(val);
        if (estimation)
            return *estimation;
    }
    return rows * ConditionSelectivityEstimator::default_normal_cond_factor;
}

Float64 ColumnStatistics::estimateGreater(Float64 val) const
{
    return rows - estimateLess(val);
}

Float64 ColumnStatistics::estimateEqual(Float64 val) const
{
    for (const auto & stat : statistics)
    {
        /// Return the estimation of the first statistics object which provides an estimation.
        /// TODO: In the (unlikely) case multiple statistics objects provide an estimation, we need some heuristics which
        ///       returns the most accurate estimation (this necessitates some notion of "estimation quality").
        auto estimation = stat->estimateEqual(val);
        if (estimation)
            return *estimation;
    }

    if (val < - ConditionSelectivityEstimator::threshold || val > ConditionSelectivityEstimator::threshold)
        return rows * ConditionSelectivityEstimator::default_normal_cond_factor;
    else
        return rows * ConditionSelectivityEstimator::default_good_cond_factor;


}

void ColumnStatistics::serialize(WriteBuffer & buf)
{
    writeIntBinary(V1, buf);

    /// as the column row count is always useful, save it in any case
    writeIntBinary(rows, buf);

    writeIntBinary(statistics.size(), buf);
    for (const auto & stat : statistics)
    {
        writeIntBinary(static_cast<UInt8>(stat->getType()), buf);
        stat->serialize(buf);
    }
}

void ColumnStatistics::deserialize(ReadBuffer & buf)
{
    UInt16 version;
    readIntBinary(version, buf);
    if (version != V1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown file format version: {}", version);

    readIntBinary(rows, buf);

    size_t stats_count;
    readIntBinary(stats_count, buf);

    for (size_t i = 0; i < stats_count; ++i)
    {
        UInt8 type_read;
        readIntBinary(type_read, buf);
        StatisticsType type = static_cast<StatisticsType>(type_read);

        auto it = std::find_if(statistics.begin(), statistics.end(), [type](const auto & stat) {return type == stat->getType() ;});
        if (it != std::end(statistics))
            (*it)->deserialize(buf);
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
    registerValidator(StatisticsType::TDigest, TDigestValidator);
    registerCreator(StatisticsType::TDigest, TDigestCreator);

    registerValidator(StatisticsType::Uniq, UniqValidator);
    registerCreator(StatisticsType::Uniq, UniqCreator);
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
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown statistic type '{}'", type);
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
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Unknown statistic type '{}'. Available types: 'tdigest' 'uniq'", type);
        auto stat_ptr = it->second(desc, stats.data_type);
        column_stat->statistics.insert(stat_ptr);
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
