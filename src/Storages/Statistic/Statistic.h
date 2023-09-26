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
        LOG_DEBUG(&Poco::Logger::get("t-digest"), "serialize into {} data", buf.offset());
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

        LOG_DEBUG(&Poco::Logger::get("t-digest"), "write into {} data", size);
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

    UInt64 total_count = 0;

    struct PartColumnEstimator
    {
        UInt64 part_count = 0;

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

    void merge(std::string part_name, UInt64 part_count, StatisticPtr statistic)
    {
        total_count += part_count;
        if (statistic != nullptr)
            column_estimators[statistic->columnName()].merge(part_name, statistic);
    }
};


}
