#pragma once

#include <Core/Block.h>
#include <Core/Field.h>
#include <Storages/StatisticsDescription.h>

#include <boost/core/noncopyable.hpp>

namespace DB
{

constexpr auto STATS_FILE_PREFIX = "statistics_";
constexpr auto STATS_FILE_SUFFIX = ".stats";

struct StatisticsUtils
{
    /// Returns std::nullopt if input Field cannot be converted to a concrete value
    /// - `data_type` is the type of the column on which the statistics object was build on
    static std::optional<Float64> tryConvertToFloat64(const Field & value, const DataTypePtr & data_type);
};

class ISingleStatistics;
using SingleStatisticsPtr = std::shared_ptr<ISingleStatistics>;

/// SingleTypeStatistics describe properties of the values in the column,
/// e.g. how many unique values exist,
/// what are the N most frequent values,
/// how frequent is a value V, etc.
class ISingleStatistics
{
public:
    explicit ISingleStatistics(const SingleStatisticsDescription & stat_);
    virtual ~ISingleStatistics() = default;

    virtual void update(const ColumnPtr & column) = 0;
    virtual void merge(const SingleStatisticsPtr & other) = 0;

    virtual void serialize(WriteBuffer & buf) = 0;
    virtual void deserialize(ReadBuffer & buf) = 0;

    /// Estimate the cardinality of the column.
    /// Throws if the statistics object is not able to do a meaningful estimation.
    virtual UInt64 estimateCardinality() const;

    /// Per-value estimations.
    /// Throws if the statistics object is not able to do a meaningful estimation.
    virtual Float64 estimateEqual(const Field & val) const; /// cardinality of val in the column
    virtual Float64 estimateLess(const Field & val) const;  /// summarized cardinality of values < val in the column

    String getTypeName() const;

protected:
    SingleStatisticsDescription stat;
};


class ColumnStatistics
{
public:
    explicit ColumnStatistics(const ColumnStatisticsDescription & stats_desc_, const String & column_name_);

    void serialize(WriteBuffer & buf);
    void deserialize(ReadBuffer & buf);

    String getFileName() const;
    const String & columnName() const;

    UInt64 rowCount() const;

    void update(const ColumnPtr & column);
    void merge(const ColumnStatistics & other);

    Float64 estimateLess(const Field & val) const;
    Float64 estimateGreater(const Field & val) const;
    Float64 estimateEqual(const Field & val) const;

private:
    friend class MergeTreeStatisticsFactory;
    ColumnStatisticsDescription stats_desc;
    String column_name;
    std::map<SingleStatisticsType, SingleStatisticsPtr> stats;
    UInt64 rows = 0; /// the number of rows in the column
};

struct ColumnDescription;
class ColumnsDescription;
using ColumnStatisticsPtr = std::shared_ptr<ColumnStatistics>;
using ColumnsStatistics = std::vector<ColumnStatisticsPtr>;

/// Statistics represent the statistics for a part, table or node in query plan.
/// It is a computable data structure which means:
///   1. it can accumulate statistics for a dedicated column of multiple parts.
///   2. it can support more calculations which is needed in estimating complex predicates like `a > 1 and a < 100 or a in (1, 3, 500).
///      For example column 'a' has a minmax statistics with range [1, 100], after estimate 'a>50' the range is [51, 100].
class Statistics
{
public:
    explicit Statistics() : total_row_count(0) { }
    explicit Statistics(UInt64 total_row_count_) : total_row_count(total_row_count_) { }
    explicit Statistics(UInt64 total_row_count_, const std::unordered_map<String, ColumnStatisticsPtr> & column_stats_)
        : total_row_count(total_row_count_)
        , column_stats(column_stats_)
    {
    }

    void merge(const Statistics & other);
    void addColumnStat(const ColumnStatisticsPtr & column_stat_);

    UInt64 getTotalRowCount() const { return total_row_count; }
    ColumnStatisticsPtr getColumnStat(const String & column_name) const;

private:
    UInt64 total_row_count;
    /// column_name -> column_statistics
    std::unordered_map<String, ColumnStatisticsPtr> column_stats;
};

using StatisticsPtr = std::shared_ptr<Statistics>;

class MergeTreeStatisticsFactory : private boost::noncopyable
{
public:
    static MergeTreeStatisticsFactory & instance();

    void validate(const ColumnStatisticsDescription & stats, const DataTypePtr & data_type) const;

    using Validator = std::function<void(const SingleStatisticsDescription & stats, const DataTypePtr & data_type)>;
    using Creator = std::function<SingleStatisticsPtr(const SingleStatisticsDescription & stats, const DataTypePtr & data_type)>;

    ColumnStatisticsPtr get(const ColumnDescription & column_desc) const;
    ColumnsStatistics getMany(const ColumnsDescription & columns) const;

    void registerValidator(SingleStatisticsType type, Validator validator);
    void registerCreator(SingleStatisticsType type, Creator creator);

protected:
    MergeTreeStatisticsFactory();

private:
    using Validators = std::unordered_map<SingleStatisticsType, Validator>;
    using Creators = std::unordered_map<SingleStatisticsType, Creator>;
    Validators validators;
    Creators creators;
};

}
