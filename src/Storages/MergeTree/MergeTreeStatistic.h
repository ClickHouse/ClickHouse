#pragma once

#include <base/types.h>
#include <Common/HashTable/HashTable.h>
#include <Core/Names.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <memory>
#include <Parsers/IAST_fwd.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/Statistics.h>
#include <Storages/StatisticsDescription.h>
#include <unordered_map>

constexpr auto PART_STATS_FILE_NAME = "part_stat";
constexpr auto PART_STATS_FILE_EXT = "bin_stats";

namespace DB
{

enum class MergeTreeDistributionStatisticType
{
    TDIGEST,
    GRANULE_TDIGEST,
};

enum class MergeTreeStringSearchStatisticType
{
    GRANULE_COUNT_MIN_SKETCH_HASH,
};

String generateFileNameForStatistics(const String & name, const String & columns);

class IMergeTreeStatisticCollector
{
public:
    virtual ~IMergeTreeStatisticCollector() = default;

    virtual const String& name() const = 0;

    virtual const String & type() const = 0;
    virtual const String & column() const = 0;
    virtual bool empty() const = 0;

    virtual StatisticType statisticType() const = 0;

    virtual IDistributionStatisticPtr getDistributionStatisticAndReset() { return nullptr; }
    virtual IStringSearchStatisticPtr getStringSearchStatisticAndReset() { return nullptr; }

    /// Updates the stored info using rows of the specified block.
    /// Reads no more than `limit` rows.
    /// After finishing updating `pos` will store the position of the first row which was not read.
    virtual void update(const Block & block, size_t * pos, size_t limit) = 0;
    virtual void granuleFinished() = 0;
};

using IMergeTreeStatisticCollectorPtr = std::shared_ptr<IMergeTreeStatisticCollector>;
using IMergeTreeStatisticCollectorPtrs = std::vector<IMergeTreeStatisticCollectorPtr>;

class MergeTreeDistributionStatistics : public IDistributionStatistics
{
public:
    bool empty() const override;

    void merge(const std::shared_ptr<IDistributionStatistics> & other)  override;

    Names getStatisticsNames() const override;

    void serializeBinary(const String & name, WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr) override;

    std::optional<double> estimateProbability(const String & column, const Field & lower, const Field & upper) const override;

    void add(const String & column, const IDistributionStatisticPtr & stat) override;
    bool has(const String & name) const override;

    size_t getSizeInMemory() const override;
    size_t getSizeInMemoryByName(const String& name) const override;

private:
    std::unordered_map<String, IDistributionStatisticPtr> column_to_stats;
};

using MergeTreeDistributionStatisticsPtr = std::shared_ptr<MergeTreeDistributionStatistics>;

class MergeTreeStringSearchStatistics : public IStringSearchStatistics
{
public:
    bool empty() const override;

    void merge(const std::shared_ptr<IStringSearchStatistics> & other)  override;

    Names getStatisticsNames() const override;

    void serializeBinary(const String & name, WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr) override;

    std::optional<double> estimateStringProbability(const String & column, const String& needle) const override;
    std::optional<double> estimateSubstringsProbability(const String & column, const Strings& needles) const override;

    void add(const String & column, const IStringSearchStatisticPtr & stat) override;
    bool has(const String & name) const override;

    size_t getSizeInMemory() const override;
    size_t getSizeInMemoryByName(const String& name) const override;

private:
    std::unordered_map<String, IStringSearchStatisticPtr> column_to_stats;
};

using MergeTreeStringSearchStatisticsPtr = std::shared_ptr<MergeTreeStringSearchStatistics>;

// Stats stored for each part
class MergeTreeStatistics : public IStatistics
{
public:
    bool empty() const override;

    void merge(const std::shared_ptr<IStatistics>& other) override;

    Names getStatisticsNames() const override;

    void serializeBinary(const String & name, WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr) override;

    void setDistributionStatistics(IDistributionStatisticsPtr && stat) override;
    IConstDistributionStatisticsPtr getDistributionStatistics() const override;

    void setStringSearchStatistics(IStringSearchStatisticsPtr && stat) override;
    IConstStringSearchStatisticsPtr getStringSearchStatistics() const override;

    size_t getSizeInMemory() const override;

private:
    MergeTreeDistributionStatisticsPtr column_distributions;
    MergeTreeStringSearchStatisticsPtr string_search;
};

using MergeTreeStatisticsPtr = std::shared_ptr<MergeTreeStatistics>;

class MergeTreeStatisticFactory : private boost::noncopyable
{
public:
    static MergeTreeStatisticFactory & instance();

    using StatisticsCreator = std::function<IStatisticPtr(
        const StatisticDescription & stat, const ColumnDescription & column)>;
    using CollectorCreator = std::function<IMergeTreeStatisticCollectorPtr(
        const StatisticDescription & stat, const ColumnDescription & column)>;
    using Validator = std::function<void(
        const StatisticDescription & stat, const ColumnDescription & column)>;

    void validate(
        const std::vector<StatisticDescription> & statistics,
        const ColumnsDescription & columns) const;

    MergeTreeStatisticsPtr get(
        const std::vector<StatisticDescription> & statistics,
        const ColumnsDescription & columns) const;

    // Creates collectors for available pairs (stat, column).
    // Statistics on different columns are computed independently,
    // so collector can calculate statistics only on subset of columns provided in columns_for_collection.
    IMergeTreeStatisticCollectorPtrs getStatisticCollectors(
        const std::vector<StatisticDescription> & statistics,
        const ColumnsDescription & columns,
        const NamesAndTypesList & columns_for_collection) const;

protected:
    MergeTreeStatisticFactory();

private:
    IStatisticPtr getStatistic(
        const StatisticDescription & statistic, const ColumnDescription & column) const;

    IMergeTreeStatisticCollectorPtr getStatisticCollector(
        const StatisticDescription & statistic, const ColumnDescription & column) const;

    static std::vector<StatisticDescription> getSplittedStatistics(
        const StatisticDescription & statistic, const ColumnDescription & column);

    void registerCreators(
        const std::string & stat_type,
        StatisticsCreator creator,
        CollectorCreator collector,
        Validator validator);

    using DistributionStatisticsCreators = std::unordered_map<std::string, StatisticsCreator>;
    using CollectorCreators = std::unordered_map<std::string, CollectorCreator>;
    using Validators = std::unordered_map<std::string, Validator>;

    DistributionStatisticsCreators creators;
    CollectorCreators collectors;
    Validators validators;
};

}
