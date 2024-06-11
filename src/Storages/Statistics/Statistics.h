#pragma once

#include <cstddef>
#include <memory>
#include <optional>

#include <boost/core/noncopyable.hpp>

#include <AggregateFunctions/QuantileTDigest.h>
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
using Statistics = std::vector<StatisticPtr>;

/// Statistic contains the distribution of values in a column.
/// right now we support
/// - tdigest
class IStatistic
{
public:
    explicit IStatistic(const StatisticDescription & stat_)
        : stat(stat_)
    {
    }
    virtual ~IStatistic() = default;

    String getFileName() const
    {
        return STAT_FILE_PREFIX + columnName();
    }

    const String & columnName() const
    {
        return stat.column_name;
    }

    virtual void serialize(WriteBuffer & buf) = 0;

    virtual void deserialize(ReadBuffer & buf) = 0;

    virtual void update(const ColumnPtr & column) = 0;

    virtual UInt64 count() = 0;

protected:

    StatisticDescription stat;

};

class ColumnsDescription;

class MergeTreeStatisticsFactory : private boost::noncopyable
{
public:
    static MergeTreeStatisticsFactory & instance();

    void validate(const StatisticDescription & stat, DataTypePtr data_type) const;

    using Creator = std::function<StatisticPtr(const StatisticDescription & stat)>;

    using Validator = std::function<void(const StatisticDescription & stat, DataTypePtr data_type)>;

    StatisticPtr get(const StatisticDescription & stat) const;

    Statistics getMany(const ColumnsDescription & columns) const;

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
