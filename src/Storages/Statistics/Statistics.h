#pragma once

#include <memory>
#include <boost/core/noncopyable.hpp>

#include <Core/Block.h>
#include <Common/logger_useful.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <Storages/StatisticsDescription.h>


namespace DB
{

/// this is for user-defined statistic.
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

    virtual void serialize(WriteBuffer & buf) = 0;

    virtual void deserialize(ReadBuffer & buf) = 0;

    virtual void update(const ColumnPtr & column) = 0;

protected:
    SingleStatisticsDescription stat;
};

using StatisticsPtr = std::shared_ptr<IStatistics>;

class ColumnStatistics
{
public:
    explicit ColumnStatistics(const ColumnStatisticsDescription & stats_);
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
    UInt64 rows; /// the number of rows of the column
};

class ColumnsDescription;
using ColumnStatisticsPtr = std::shared_ptr<ColumnStatistics>;
using ColumnsStatistics = std::vector<ColumnStatisticsPtr>;

class MergeTreeStatisticsFactory : private boost::noncopyable
{
public:
    static MergeTreeStatisticsFactory & instance();

    void validate(const ColumnStatisticsDescription & stats, DataTypePtr data_type) const;

    using Creator = std::function<StatisticsPtr(const SingleStatisticsDescription & stats, DataTypePtr data_type)>;

    using Validator = std::function<void(const SingleStatisticsDescription & stats, DataTypePtr data_type)>;

    ColumnStatisticsPtr get(const ColumnStatisticsDescription & stats) const;

    ColumnsStatistics getMany(const ColumnsDescription & columns) const;

    void registerCreator(StatisticsType type, Creator creator);
    void registerValidator(StatisticsType type, Validator validator);

protected:
    MergeTreeStatisticsFactory();

private:
    using Creators = std::unordered_map<StatisticsType, Creator>;
    using Validators = std::unordered_map<StatisticsType, Validator>;
    Creators creators;
    Validators validators;
};

}
