#pragma once

#include <memory>
#include <unordered_map>
#include <base/types.h>
#include <Core/Names.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <Parsers/IAST_fwd.h>
#include <Common/HashTable/HashTable.h>
#include "Storages/ColumnsDescription.h"
#include <Storages/StatisticsDescription.h>
#include <Storages/Statistics.h>

constexpr auto PART_STATS_FILE_NAME = "part_stats";
constexpr auto PART_STATS_FILE_EXT = "bin_stats";

namespace DB
{

class IMergeTreeColumnDistributionStatisticCollector {
public:
    virtual ~IMergeTreeColumnDistributionStatisticCollector() = default;

    virtual const String & name() const = 0;
    virtual const String & column() const = 0;
    virtual bool empty() const = 0;
    virtual IColumnDistributionStatisticPtr getStatisticAndReset() = 0;

    /// Updates the stored info using rows of the specified block.
    /// Reads no more than `limit` rows.
    /// After finishing updating `pos` will store the position of the first row which was not read.
    virtual void update(const Block & block, size_t * pos, size_t limit) = 0;
    virtual void granuleFinished() = 0;
};

using IMergeTreeColumnDistributionStatisticCollectorPtr = std::shared_ptr<IMergeTreeColumnDistributionStatisticCollector>;
using IMergeTreeColumnDistributionStatisticCollectorPtrs = std::vector<IMergeTreeColumnDistributionStatisticCollectorPtr>;

class MergeTreeColumnDistributionStatistics : public IColumnDistributionStatistics {
public:
    bool empty() const override;

    void merge(const std::shared_ptr<IColumnDistributionStatistics> & other)  override;

    void serializeBinary(WriteBuffer & ostr) const  override;
    void deserializeBinary(ReadBuffer & istr)  override;

    std::optional<double> estimateProbability(const String & column, const Field & lower, const Field & upper) const override;
    void add(const String & name, const IColumnDistributionStatisticPtr & stat) override;
    void remove(const String & name) override;

private:
    std::unordered_map<String, IColumnDistributionStatisticPtr> column_to_stats;
};

using MergeTreeColumnDistributionStatisticsPtr = std::shared_ptr<MergeTreeColumnDistributionStatistics>;

// Stats stored for each part
// TODO: all stats, not only merge tree
class MergeTreeStatistics : public IStatistics {
public:
    bool empty() const override;

    void merge(const std::shared_ptr<IStatistics>& other) override;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr) override;

    void setColumnDistributionStatistics(IColumnDistributionStatisticsPtr && stat) override;
    IConstColumnDistributionStatisticsPtr getColumnDistributionStatistics() const override;

private:
    MergeTreeColumnDistributionStatisticsPtr column_distributions;
};

using MergeTreeStatisticsPtr = std::shared_ptr<MergeTreeStatistics>;

class MergeTreeStatisticFactory : private boost::noncopyable
{
public:
    static MergeTreeStatisticFactory & instance();

    using StatCreator = std::function<IColumnDistributionStatisticPtr(
        const StatisticDescription & stat, const ColumnDescription & column)>;
    using CollectorCreator = std::function<IMergeTreeColumnDistributionStatisticCollectorPtr(
        const StatisticDescription & stat, const ColumnDescription & column)>;
    using Validator = std::function<void(
        const StatisticDescription & stat, const ColumnDescription & column)>;

    void validate(
        const std::vector<StatisticDescription> & stats,
        const ColumnsDescription & columns) const;
    
    MergeTreeStatisticsPtr get(
        const std::vector<StatisticDescription> & stats,
        const ColumnsDescription & columns) const;

    IMergeTreeColumnDistributionStatisticCollectorPtrs getColumnDistributionStatisticCollectors(
        const std::vector<StatisticDescription> & stats,
        const ColumnsDescription & columns) const;

protected:
    MergeTreeStatisticFactory();

private:
    IColumnDistributionStatisticPtr getColumnDistributionStatistic(
        const StatisticDescription & stat, const ColumnDescription & column) const;

    IMergeTreeColumnDistributionStatisticCollectorPtr getColumnDistributionStatisticCollector(
        const StatisticDescription & stat, const ColumnDescription & column) const;
    
    std::vector<StatisticDescription> getSplittedStatistics(
        const StatisticDescription & stat, const ColumnDescription & column) const;

    void registerCreators(
        const std::string & stat_type,
        StatCreator creator,
        CollectorCreator collector,
        Validator validator);

    using StatCreators = std::unordered_map<std::string, StatCreator>;
    using CollectorCreators = std::unordered_map<std::string, CollectorCreator>;
    using Validators = std::unordered_map<std::string, Validator>;

    StatCreators creators;
    CollectorCreators collectors;
    Validators validators;
};

}
