#pragma once

#include <Core/Block.h>
#include <Core/Field.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
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

    virtual void serialize(WriteBuffer & buf) = 0;
    virtual void deserialize(ReadBuffer & buf) = 0;

    /// Estimate the cardinality of the column.
    /// Throws if the statistics object is not able to do a meaningful estimation.
    virtual UInt64 estimateCardinality() const;

    /// Per-value estimations.
    /// Throws if the statistics object is not able to do a meaningful estimation.
    virtual Float64 estimateEqual(const Field & val) const; /// cardinality of val in the column
    virtual Float64 estimateLess(const Field & val) const;  /// summarized cardinality of values < val in the column

protected:
    SingleStatisticsDescription stat;
};

using StatisticsPtr = std::shared_ptr<IStatistics>;

/// All statistics objects for a column in a part
class ColumnPartStatistics
{
public:
    explicit ColumnPartStatistics(const ColumnStatisticsDescription & stats_desc_, const String & column_name_);

    void serialize(WriteBuffer & buf);
    void deserialize(ReadBuffer & buf);

    String getFileName() const;
    const String & columnName() const;

    UInt64 rowCount() const;

    void build(const ColumnPtr & column);

    Float64 estimateLess(const Field & val) const;
    Float64 estimateGreater(const Field & val) const;
    Float64 estimateEqual(const Field & val) const;

private:
    friend class MergeTreeStatisticsFactory;
    ColumnStatisticsDescription stats_desc;
    String column_name;
    std::map<StatisticsType, StatisticsPtr> stats;
    UInt64 rows = 0; /// the number of rows in the column
};

using ColumnStatisticsPartPtr = std::shared_ptr<ColumnPartStatistics>;
using ColumnsStatistics = std::vector<ColumnStatisticsPartPtr>;

struct ColumnDescription;
class ColumnsDescription;

class MergeTreeStatisticsFactory : private boost::noncopyable
{
public:
    static MergeTreeStatisticsFactory & instance();

    void validate(const ColumnStatisticsDescription & stats, const DataTypePtr & data_type) const;

    using Validator = std::function<void(const SingleStatisticsDescription & stats, const DataTypePtr & data_type)>;
    using Creator = std::function<StatisticsPtr(const SingleStatisticsDescription & stats, const DataTypePtr & data_type)>;

    ColumnStatisticsPartPtr get(const ColumnDescription & column_desc) const;
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
