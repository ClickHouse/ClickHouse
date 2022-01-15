#pragma once

#include <memory>
#include <unordered_map>
#include <base/types.h>
#include <Core/Names.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <Parsers/IAST_fwd.h>
#include <Common/HashTable/HashTable.h>
#include <Storages/StatisticsDescription.h>

constexpr auto PART_STATS_FILE_NAME = "part_stats";
constexpr auto PART_STATS_FILE_EXT = "bin_stats";

namespace DB
{

class IMergeTreeStatistic {
public:
    virtual ~IMergeTreeStatistic() = default;

    virtual const String& name() const = 0;

    virtual bool empty() const = 0;
    virtual void merge(const std::shared_ptr<IMergeTreeStatistic> & other) = 0;

    virtual const String & getColumnsRequiredForStatisticCalculation() const = 0;

    virtual void serializeBinary(WriteBuffer & ostr) const = 0;
    virtual void deserializeBinary(ReadBuffer & istr) = 0;
};

enum class StatisticType
{
    COLUMN_DISRIBUTION = 1,
    //COLUMN_MOST_FREQUENT, -- in list => bad selectivity
    //COLUMN_DISTINCT_COUNT, -- for join/group by
    //COLUMN_LOW_CARDINALITY_COUNT, -- exact per block stats for low cardinality
    //COLUMN_COUNT_SKETCH, -- per block count
    //COLUMN_NULL_COUNT, -- like postgres
    //...
};

// Distribution statistic per one column
class IMergeTreeColumnDistributionStatistic : public IMergeTreeStatistic {
public:
    // some quantile of value smaller than value
    virtual double estimateQuantileLower(const Field& value) const = 0;
    // some quantile of value greater than value
    virtual double estimateQuantileUpper(const Field& value) const = 0;

    // upper bound of probability of lower <= value <= upper
    virtual double estimateProbability(const Field& lower, const Field& upper) const = 0;
};

using IMergeTreeColumnDistributionStatisticPtr = std::shared_ptr<IMergeTreeColumnDistributionStatistic>;
using IMergeTreeColumnDistributionStatisticPtrs = std::vector<IMergeTreeColumnDistributionStatisticPtr>;

class IMergeTreeColumnDistributionStatisticCollector {
public:
    virtual ~IMergeTreeColumnDistributionStatisticCollector() = default;

    virtual const String & name() const = 0;
    virtual const String & column() const = 0;
    virtual bool empty() const = 0;
    virtual IMergeTreeColumnDistributionStatisticPtr getStatisticAndReset() = 0;

    /// Updates the stored info using rows of the specified block.
    /// Reads no more than `limit` rows.
    /// After finishing updating `pos` will store the position of the first row which was not read.
    virtual void update(const Block & block, size_t * pos, size_t limit) = 0;
    virtual void granuleFinished() = 0;
};

using IMergeTreeColumnDistributionStatisticCollectorPtr = std::shared_ptr<IMergeTreeColumnDistributionStatisticCollector>;
using IMergeTreeColumnDistributionStatisticCollectors = std::vector<IMergeTreeColumnDistributionStatisticCollectorPtr>;

class MergeTreeColumnDistributionStatistics /*: public IMergeTreePerColumnStatistics */{
public:
    bool empty() const;

    void merge(const MergeTreeColumnDistributionStatistics & other);

    void serializeBinary(WriteBuffer & ostr) const;
    void deserializeBinary(ReadBuffer & istr);

    std::optional<double> estimateProbability(const String & column, const Field & lower, const Field & upper) const;
    void add(const String & name, const IMergeTreeColumnDistributionStatisticPtr & stat);
    void remove(const String & name);

private:
    std::unordered_map<String, IMergeTreeColumnDistributionStatisticPtr> column_to_stats;
};

// Stats stored for each part
// TODO: all stats, not only merge tree
class MergeTreeStatistics {
public:
    bool empty() const;

    void merge(const std::shared_ptr<MergeTreeStatistics>& other);

    void serializeBinary(WriteBuffer & ostr) const;
    void deserializeBinary(ReadBuffer & istr);

    void setColumnDistributionStatistics(MergeTreeColumnDistributionStatistics && stat);
    const MergeTreeColumnDistributionStatistics & getColumnDistributionStatistics() const;

private:
    MergeTreeColumnDistributionStatistics column_distributions;
};

using MergeTreeStatisticsPtr = std::shared_ptr<MergeTreeStatistics>;

class MergeTreeStatisticFactory : private boost::noncopyable
{
public:
    static MergeTreeStatisticFactory & instance();

    using StatCreator = std::function<IMergeTreeColumnDistributionStatisticPtr(const StatisticDescription & stat)>;
    using CollectorCreator = std::function<IMergeTreeColumnDistributionStatisticCollectorPtr(const StatisticDescription & stat)>;

    IMergeTreeColumnDistributionStatisticPtr getColumnDistributionStatistic(const StatisticDescription & stat) const;
    MergeTreeStatisticsPtr get(const std::vector<StatisticDescription> & stats) const;

    IMergeTreeColumnDistributionStatisticCollectorPtr getColumnDistributionStatisticCollector(
        const StatisticDescription & stat) const;
    IMergeTreeColumnDistributionStatisticCollectors getColumnDistributionStatisticCollectors(
        const std::vector<StatisticDescription> & stats) const;

    void registerCreators(const std::string & stat_type, StatCreator creator, CollectorCreator collector);

protected:
    MergeTreeStatisticFactory();

private:
    using StatCreators = std::unordered_map<std::string, StatCreator>;
    using CollectorCreators = std::unordered_map<std::string, CollectorCreator>;
    StatCreators creators;
    CollectorCreators collectors;
};

}
