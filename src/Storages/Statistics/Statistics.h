#pragma once

#include <cstddef>
#include <memory>
#include <optional>

#include <boost/core/noncopyable.hpp>

#include <Core/Block.h>
#include <Common/logger_useful.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <Storages/StatisticsDescription.h>


/// this is for user-defined statistic.
constexpr auto STAT_FILE_PREFIX = "statistic_";
constexpr auto STAT_FILE_SUFFIX = ".stat";

namespace DB
{

class IStatistic;
using StatisticPtr = std::shared_ptr<IStatistic>;
/// using Statistics = std::vector<StatisticPtr>;

/// Statistic contains the distribution of values in a column.
/// right now we support
/// - tdigest
/// - uniq(hyperloglog)
class IStatistic
{
public:
    explicit IStatistic(const StatisticDescription & stat_)
        : stat(stat_)
    {
    }
    virtual ~IStatistic() = default;

    virtual void serialize(WriteBuffer & buf) = 0;

    virtual void deserialize(ReadBuffer & buf) = 0;

    virtual void update(const ColumnPtr & column) = 0;

protected:

    StatisticDescription stat;

};

class ColumnStatistics;
using ColumnStatisticsPtr = std::shared_ptr<ColumnStatistics>;

class ColumnStatistics
{
    friend class MergeTreeStatisticsFactory;
    StatisticsDescription stats_desc;
    std::map<StatisticType, StatisticPtr> stats;
    UInt64 counter;
public:
    explicit ColumnStatistics(const StatisticsDescription & stats_);
    void serialize(WriteBuffer & buf);
    void deserialize(ReadBuffer & buf);
    String getFileName() const
    {
        return STAT_FILE_PREFIX + columnName();
    }

    const String & columnName() const
    {
        return stats_desc.column_name;
    }

    UInt64 count() const { return counter; }

    void update(const ColumnPtr & column);

    /// void merge(ColumnStatisticsPtr other_column_stats);

    Float64 estimateLess(Float64 val) const;

    Float64 estimateGreater(Float64 val) const;

    Float64 estimateEqual(Float64 val) const;
};

class ColumnsDescription;

class MergeTreeStatisticsFactory : private boost::noncopyable
{
public:
    static MergeTreeStatisticsFactory & instance();

    void validate(const StatisticsDescription & stats, DataTypePtr data_type) const;

    using Creator = std::function<StatisticPtr(const StatisticDescription & stat, DataTypePtr data_type)>;

    using Validator = std::function<void(const StatisticDescription & stat, DataTypePtr data_type)>;

    ColumnStatisticsPtr get(const StatisticsDescription & stat) const;

    std::vector<ColumnStatisticsPtr> getMany(const ColumnsDescription & columns) const;

    void registerCreator(StatisticType type, Creator creator);
    void registerValidator(StatisticType type, Validator validator);

protected:
    MergeTreeStatisticsFactory();

private:
    using Creators = std::unordered_map<StatisticType, Creator>;
    using Validators = std::unordered_map<StatisticType, Validator>;
    Creators creators;
    Validators validators;
};

}
