#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <AggregateFunctions/QuantileTDigest.h>
#include <Core/Block.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <Storages/StatisticsDescription.h>
#include <Common/logger_useful.h>

#include <boost/core/noncopyable.hpp>

/// this is for user-defined statistic.
constexpr auto STAT_FILE_PREFIX = "statistic_";
constexpr auto STAT_FILE_SUFFIX = ".stat";

namespace DB
{

class IStatistic;
using StatisticPtr = std::shared_ptr<IStatistic>;
using Statistics = std::vector<StatisticPtr>;

/// Statistic for a column
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

    /// statistic_[col_name]_[type]
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

    virtual void update(const Block & block) = 0;

    virtual UInt64 count() = 0;

protected:

    StatisticDescription stat;

};

class TDigestStatistic : public IStatistic
{
    QuantileTDigest<Float64> data;
public:
    explicit TDigestStatistic(const StatisticDescription & stat_) : IStatistic(stat_)
    {
    }

    Float64 estimateLess(Float64 val) const
    {
        return data.getCountLessThan(val);
    }

    void serialize(WriteBuffer & buf) override
    {
        data.serialize(buf);
    }

    void deserialize(ReadBuffer & buf) override
    {
        data.deserialize(buf);
    }

    void update(const Block & block) override
    {
        const auto & column_with_type = block.getByName(columnName());
        size_t size = block.rows();

        for (size_t i = 0; i < size; ++i)
        {
            /// TODO: support more types.
            Float64 value = column_with_type.column->getFloat64(i);
            data.add(value, 1);
        }
    }

    UInt64 count() override
    {
        return static_cast<UInt64>(data.count);
    }
};

class ColumnsDescription;

class MergeTreeStatisticFactory : private boost::noncopyable
{
public:
    static MergeTreeStatisticFactory & instance();

    void validate(const StatisticDescription & stat, DataTypePtr data_type) const;

    using Creator = std::function<StatisticPtr(const StatisticDescription & stat)>;

    using Validator = std::function<void(const StatisticDescription & stat, DataTypePtr data_type)>;

    StatisticPtr get(const StatisticDescription & stat) const;

    Statistics getMany(const ColumnsDescription & columns) const;

    void registerCreator(StatisticType type, Creator creator);
    void registerValidator(StatisticType type, Validator validator);

protected:
    MergeTreeStatisticFactory();

private:
    using Creators = std::unordered_map<StatisticType, Creator>;
    using Validators = std::unordered_map<StatisticType, Validator>;
    Creators creators;
    Validators validators;
};

}
