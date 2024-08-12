#pragma once

#include <Core/Block.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <Storages/StatisticsDescription.h>

#include <boost/core/noncopyable.hpp>

namespace DB
{

constexpr auto STATS_FILE_PREFIX = "statistics_";
constexpr auto STATS_FILE_SUFFIX = ".stats";

/// Statistics describe properties of the values in the column,
/// e.g. how many unique values exist,
/// what are the N most frequent values,
/// how frequent is a value V, etc.
class IStatistics
{
public:
    explicit IStatistics(const SingleStatisticsDescription & stat_);
    virtual ~IStatistics() = default;

    virtual void update(const ColumnPtr & column) = 0;

    virtual void serialize(WriteBuffer & buf) = 0;
    virtual void deserialize(ReadBuffer & buf) = 0;

    /// Estimate the cardinality of the column.
    /// Throws if the statistics object is not able to do a meaningful estimation.
    virtual UInt64 estimateCardinality() const;

    /// Per-value estimations.
    /// Throws if the statistics object is not able to do a meaningful estimation.
    virtual Float64 estimateEqual(Float64 val) const; /// cardinality of val in the column
    virtual Float64 estimateLess(Float64 val) const;  /// summarized cardinality of values < val in the column

protected:
    SingleStatisticsDescription stat;
};

using StatisticsPtr = std::shared_ptr<IStatistics>;

class ColumnStatistics
{
public:
    explicit ColumnStatistics(const ColumnStatisticsDescription & stats_desc_);

    void serialize(WriteBuffer & buf);
    void deserialize(ReadBuffer & buf);

    String getFileName() const;
    const String & columnName() const;

    UInt64 rowCount() const;

    void update(const ColumnPtr & column);

    Float64 estimateLess(Float64 val) const;
    Float64 estimateGreater(Float64 val) const;
    Float64 estimateEqual(Float64 val) const;

private:
    friend class MergeTreeStatisticsFactory;
    ColumnStatisticsDescription stats_desc;
    std::map<StatisticsType, StatisticsPtr> stats;
    UInt64 rows = 0; /// the number of rows in the column
};

class ColumnsDescription;
using ColumnStatisticsPtr = std::shared_ptr<ColumnStatistics>;
using ColumnsStatistics = std::vector<ColumnStatisticsPtr>;

class MergeTreeStatisticsFactory : private boost::noncopyable
{
public:
    static MergeTreeStatisticsFactory & instance();

    void validate(const ColumnStatisticsDescription & stats, DataTypePtr data_type) const;

    using Validator = std::function<void(const SingleStatisticsDescription & stats, DataTypePtr data_type)>;
    using Creator = std::function<StatisticsPtr(const SingleStatisticsDescription & stats, DataTypePtr data_type)>;

    ColumnStatisticsPtr get(const ColumnStatisticsDescription & stats) const;
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

}
