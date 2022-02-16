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

constexpr auto PART_STATS_FILE_NAME = "part_stat";
constexpr auto PART_STATS_FILE_EXT = "bin_stats";

namespace DB
{

String generateFileNameForStatistics(const String & name);

class IMergeTreeDistributionStatisticCollector {
public:
    virtual ~IMergeTreeDistributionStatisticCollector() = default;

    virtual const String& name() const = 0;

    virtual const String & type() const = 0;
    virtual const String & column() const = 0;
    virtual bool empty() const = 0;
    virtual IDistributionStatisticPtr getStatisticAndReset() = 0;

    /// Updates the stored info using rows of the specified block.
    /// Reads no more than `limit` rows.
    /// After finishing updating `pos` will store the position of the first row which was not read.
    virtual void update(const Block & block, size_t * pos, size_t limit) = 0;
    virtual void granuleFinished() = 0;
};

using IMergeTreeDistributionStatisticCollectorPtr = std::shared_ptr<IMergeTreeDistributionStatisticCollector>;
using IMergeTreeDistributionStatisticCollectorPtrs = std::vector<IMergeTreeDistributionStatisticCollectorPtr>;

class MergeTreeDistributionStatistics : public IDistributionStatistics {
public:
    bool empty() const override;

    void merge(const std::shared_ptr<IDistributionStatistics> & other)  override;

    Names getStatisticsNames() const override;

    void serializeBinary(const String & name, WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr) override;

    std::optional<double> estimateProbability(const String & column, const Field & lower, const Field & upper) const override;
    void add(const String & column, const IDistributionStatisticPtr & stat) override;

private:
    std::unordered_map<String, IDistributionStatisticPtr> column_to_stats;
};

using MergeTreeDistributionStatisticsPtr = std::shared_ptr<MergeTreeDistributionStatistics>;

// Stats stored for each part
class MergeTreeStatistics : public IStatistics {
public:
    bool empty() const override;

    void merge(const std::shared_ptr<IStatistics>& other) override;

    Names getStatisticsNames() const override;

    void serializeBinary(const String & name, WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr) override;

    void setDistributionStatistics(IDistributionStatisticsPtr && stat) override;
    IConstDistributionStatisticsPtr getDistributionStatistics() const override;

private:
    MergeTreeDistributionStatisticsPtr column_distributions;
};

using MergeTreeStatisticsPtr = std::shared_ptr<MergeTreeStatistics>;

class MergeTreeStatisticFactory : private boost::noncopyable
{
public:
    static MergeTreeStatisticFactory & instance();

    using StatCreator = std::function<IDistributionStatisticPtr(
        const StatisticDescription & stat, const ColumnDescription & column)>;
    using CollectorCreator = std::function<IMergeTreeDistributionStatisticCollectorPtr(
        const StatisticDescription & stat, const ColumnDescription & column)>;
    using Validator = std::function<void(
        const StatisticDescription & stat, const ColumnDescription & column)>;

    void validate(
        const std::vector<StatisticDescription> & stats,
        const ColumnsDescription & columns) const;
    
    MergeTreeStatisticsPtr get(
        const std::vector<StatisticDescription> & stats,
        const ColumnsDescription & columns) const;

    // Creates collectors for available pairs (stat, column).
    // Statistics on different columns are computed independently,
    // so collector can calculate statistics only on subset of columns provided in columns_for_collection.
    IMergeTreeDistributionStatisticCollectorPtrs getDistributionStatisticCollectors(
        const std::vector<StatisticDescription> & stats,
        const ColumnsDescription & columns,
        const NamesAndTypesList & columns_for_collection) const;

protected:
    MergeTreeStatisticFactory();

private:
    IDistributionStatisticPtr getDistributionStatistic(
        const StatisticDescription & stat, const ColumnDescription & column) const;

    IMergeTreeDistributionStatisticCollectorPtr getDistributionStatisticCollector(
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
