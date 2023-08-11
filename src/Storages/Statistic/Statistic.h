#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <AggregateFunctions/QuantileTDigest.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <Storages/StatisticsDescription.h>
#include "Common/Exception.h"
#include <Common/logger_useful.h>

#include <boost/core/noncopyable.hpp>

/// this is for user-defined statistic.
/// For auto collected statisic, we can use 'auto_statistic_'
constexpr auto STAT_FILE_PREFIX = "statistic_";
constexpr auto STAT_FILE_SUFFIX = ".stat";

namespace DB
{

class IStatistic;
using StatisticPtr = std::shared_ptr<IStatistic>;
using Statistics = std::vector<StatisticPtr>;

class IStatistic
{
public:
    explicit IStatistic(const StatisticDescription & stat_)
        : statistics(stat_)
    {
    }
    virtual ~IStatistic() = default;

    String getFileName() const
    {
        return STAT_FILE_PREFIX + name();
    }

    const String & name() const
    {
        return statistics.name;
    }

    const String & columnName() const
    {
        return statistics.column_names[0];
    }
    /// const String& type() const = 0;
    /// virtual StatisticType statisticType() const = 0;

    virtual void serialize(WriteBuffer & buf) = 0;
    virtual void deserialize(ReadBuffer & buf) = 0;
    virtual void update(const Block & block) = 0;
    virtual UInt64 count() = 0;

protected:

    const StatisticDescription & statistics;

};

class TDigestStatistic : public IStatistic
{
    QuantileTDigest<Float64> data;
public:
    explicit TDigestStatistic(const StatisticDescription & stat) : IStatistic(stat)
    {
    }

    struct Range
    {
        Float64 left, right;
    };

    /// FIXME: implement correct count estimate method.
    Float64 estimateLess(Float64 val) const
    {
        return data.getCountLessThan(val);
    }

    void serialize(WriteBuffer & buf) override
    {
        data.serialize(buf);
        LOG_DEBUG(&Poco::Logger::get("t-digest"), "serialize into {} data", buf.offset());
    }

    void deserialize(ReadBuffer & buf) override
    {
        data.deserialize(buf);
    }

    void update(const Block & block) override
    {
        const auto & column_with_type = block.getByName(statistics.column_names[0]);
        size_t size = block.rows();

        for (size_t i = 0; i < size; ++i)
        {
            /// TODO: support more types.
            Float64 value = column_with_type.column->getFloat64(i);
            data.add(value, 1);
        }

        LOG_DEBUG(&Poco::Logger::get("t-digest"), "write into {} data", size);
    }

    UInt64 count() override
    {
        return static_cast<UInt64>(data.count);
    }
};

class MergeTreeStatisticFactory : private boost::noncopyable
{
public:
    static MergeTreeStatisticFactory & instance();

    using Creator = std::function<StatisticPtr(const StatisticDescription & stat)>;

    StatisticPtr get(const StatisticDescription & stat) const;

    Statistics getMany(const std::vector<StatisticDescription> & stats) const;

    void registerCreator(const std::string & type, Creator creator);

protected:
    MergeTreeStatisticFactory();

private:
    using Creators = std::unordered_map<std::string, Creator>;
    Creators creators;
};

class RPNBuilderTreeNode;

class ConditionEstimator
{
private:

    static constexpr auto default_good_cond_factor = 0.1;
    static constexpr auto default_normal_cond_factor = 0.5;
    static constexpr auto default_unknown_cond_factor = 1.0;
    /// Conditions like "x = N" are considered good if abs(N) > threshold.
    /// This is used to assume that condition is likely to have good selectivity.
    static constexpr auto threshold = 2;

    UInt64 total_count;

    struct PartColumnEstimator
    {
        UInt64 part_count;

        std::shared_ptr<TDigestStatistic> t_digest;

        void merge(StatisticPtr statistic)
        {
            UInt64 cur_part_count = statistic->count();
            if (part_count == 0)
                part_count = cur_part_count;

            if (typeid_cast<TDigestStatistic *>(statistic.get()))
            {
                t_digest = std::static_pointer_cast<TDigestStatistic>(statistic);
            }
        }

        Float64 estimateLess(Float64 val) const
        {
            if (t_digest != nullptr)
                return t_digest -> estimateLess(val);
            return part_count * default_normal_cond_factor;
        }

        Float64 estimateGreator(Float64 val) const
        {
            if (t_digest != nullptr)
                return part_count - t_digest -> estimateLess(val);
            return part_count * default_normal_cond_factor;
        }
    };

    struct ColumnEstimator
    {
        std::map<std::string, PartColumnEstimator> estimators;

        void merge(std::string part_name, StatisticPtr statistic)
        {
            estimators[part_name].merge(statistic);
        }
        Float64 estimateLess(Float64 val) const
        {
            if (estimators.empty())
                return default_normal_cond_factor;
            Float64 result = 0;
            for (const auto & [key, estimator] : estimators)
                result += estimator.estimateLess(val);
            return result;
        }

        Float64 estimateGreater(Float64 val) const
        {
            if (estimators.empty())
                return default_normal_cond_factor;
            Float64 result = 0;
            for (const auto & [key, estimator] : estimators)
                result += estimator.estimateGreator(val);
            return result;
        }
    };

    std::map<String, ColumnEstimator> column_estimators;
    std::optional<std::string> extractSingleColumn(const RPNBuilderTreeNode & node) const;
    std::pair<std::string, Float64> extractBinaryOp(const RPNBuilderTreeNode & node, const std::string & column_name) const;

public:

    ConditionEstimator() = default;

    /// TODO: Support the condition consists of CNF/DNF like (cond1 and cond2) or (cond3) ...
    /// Right now we only support simple condition like col = val / col < val
    Float64 estimateSelectivity(const RPNBuilderTreeNode & node) const;

    void merge(std::string part_name, StatisticPtr statistic)
    {
        column_estimators[statistic->columnName()].merge(part_name, statistic);
    }

};


}
