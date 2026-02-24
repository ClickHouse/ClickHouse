#pragma once

#include <Core/Range.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <Storages/StatisticsDescription.h>

#include <boost/core/noncopyable.hpp>

namespace DB
{

constexpr auto STATS_FILE_PREFIX = "statistics_";
constexpr auto STATS_FILE_SUFFIX = ".stats";

class Field;

struct StatisticsUtils
{
    /// Returns std::nullopt if input Field cannot be converted to a concrete value
    /// - `data_type` is the type of the column on which the statistics object was build on
    static std::optional<Float64> tryConvertToFloat64(const Field & value, const DataTypePtr & data_type);
};

class IStatistics;
using StatisticsPtr = std::shared_ptr<IStatistics>;

/// Interface for a single statistics object for a column within a part.
///
/// Statistics describe properties of the values in the column, e.g. how many unique values exist, what are
/// the N most frequent values, how frequent a value V is, etc.
class IStatistics
{
public:
    explicit IStatistics(const SingleStatisticsDescription & stat_);
    virtual ~IStatistics() = default;

    virtual void build(const ColumnPtr & column) = 0;
    virtual void merge(const StatisticsPtr & other_stats) = 0;

    virtual void serialize(WriteBuffer & buf) = 0;
    virtual void deserialize(ReadBuffer & buf) = 0;

    /// Estimate the cardinality of the column.
    /// Throws if the statistics object is not able to do a meaningful estimation.
    virtual UInt64 estimateCardinality() const;

    /// Per-value estimations.
    /// Throws if the statistics object is not able to do a meaningful estimation.
    virtual Float64 estimateEqual(const Field & val) const; /// cardinality of val in the column
    virtual Float64 estimateLess(const Field & val) const;  /// summarized cardinality of values < val in the column
    virtual Float64 estimateRange(const Range & range) const;
    virtual String getNameForLogs() const = 0;

protected:
    SingleStatisticsDescription stat;
};

class ColumnStatistics;
using ColumnStatisticsPtr = std::shared_ptr<ColumnStatistics>;
using ColumnsStatistics = std::vector<ColumnStatisticsPtr>;

struct Estimate
{
    std::set<StatisticsType> types;
    UInt64 rows_count = 0;
    std::optional<UInt64> estimated_cardinality;
    std::optional<Float64> estimated_min;
    std::optional<Float64> estimated_max;
};

using Estimates = std::unordered_map<String, Estimate>;

/// All statistics objects for a column in a part
class ColumnStatistics
{
public:
    explicit ColumnStatistics(const ColumnStatisticsDescription & stats_desc_, const String & column_name_);

    void serialize(WriteBuffer & buf);
    void deserialize(ReadBuffer & buf);

    String getStatisticName() const;
    const String & getColumnName() const;

    UInt64 rowCount() const;

    void build(const ColumnPtr & column);
    void merge(const ColumnStatisticsPtr & other);

    Float64 estimateLess(const Field & val) const;
    Float64 estimateGreater(const Field & val) const;
    Float64 estimateEqual(const Field & val) const;
    Float64 estimateRange(const Range & range) const;
    UInt64 estimateCardinality() const;

    Estimate getEstimate() const;
    String getNameForLogs() const;

private:
    friend class MergeTreeStatisticsFactory;
    ColumnStatisticsDescription stats_desc;
    String column_name;
    std::map<StatisticsType, StatisticsPtr> stats;
    UInt64 rows = 0; /// the number of rows in the column
};

struct ColumnDescription;
class ColumnsDescription;

class MergeTreeStatisticsFactory : private boost::noncopyable
{
public:
    static MergeTreeStatisticsFactory & instance();

    void validate(const ColumnStatisticsDescription & stats, const DataTypePtr & data_type) const;
    ColumnStatisticsDescription cloneWithSupportedStatistics(const ColumnStatisticsDescription & stats, const DataTypePtr & data_type) const;

    using Validator = std::function<bool(const SingleStatisticsDescription & stats, const DataTypePtr & data_type)>;
    using Creator = std::function<StatisticsPtr(const SingleStatisticsDescription & stats, const DataTypePtr & data_type)>;

    ColumnStatisticsPtr get(const ColumnDescription & column_desc) const;
    ColumnStatisticsPtr get(const ColumnStatisticsDescription & stats_desc) const;
    ColumnsStatistics getMany(const ColumnsDescription & columns) const;

    void registerValidator(StatisticsType type, Validator validator);
    void registerCreator(StatisticsType type, Creator creator);

protected:
    MergeTreeStatisticsFactory();

private:
    using Validators = std::unordered_map<StatisticsType, Validator>;
    using Creators = std::unordered_map<StatisticsType, Creator>;
    Validators validators;
    Creators creators;
};

void removeImplicitStatistics(ColumnsDescription & columns);
void addImplicitStatistics(ColumnsDescription & columns, const String & statistics_types_str);

}
