#pragma once

#include <memory>
#include <unordered_map>
#include <base/types.h>
#include <Core/Names.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/StatisticsDescription.h>

namespace DB
{

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

class IStatistic {
public:
    virtual ~IStatistic() = default;

    virtual const String& name() const = 0;

    virtual bool empty() const = 0;
    virtual void merge(const std::shared_ptr<IStatistic> & other) = 0;

    virtual const String & getColumnsRequiredForStatisticCalculation() const = 0;

    virtual void serializeBinary(WriteBuffer & ostr) const = 0;
    virtual void deserializeBinary(ReadBuffer & istr) = 0;
};

using IStatisticPtr = std::shared_ptr<IStatistic>;

class IColumnDistributionStatistic : public IStatistic {
public:
    // some quantile of value smaller than value 
    virtual double estimateQuantileLower(const Field& value) const = 0;
    // some quantile of value greater than value
    virtual double estimateQuantileUpper(const Field& value) const = 0;
    // upper bound of probability of lower <= value <= upper
    virtual double estimateProbability(const Field & lower, const Field & upper) const = 0;
};

using IColumnDistributionStatisticPtr = std::shared_ptr<IColumnDistributionStatistic>;
using IColumnDistributionStatisticPtrs = std::vector<IColumnDistributionStatisticPtr>;


class IColumnDistributionStatistics {
public:
    virtual ~IColumnDistributionStatistics() = default;

    virtual bool empty() const = 0;

    virtual void merge(const std::shared_ptr<IColumnDistributionStatistics> & other) = 0;

    virtual void serializeBinary(WriteBuffer & ostr) const = 0;
    virtual void deserializeBinary(ReadBuffer & istr) = 0;

    virtual std::optional<double> estimateProbability(const String & column, const Field & lower, const Field & upper) const = 0;
    virtual void add(const String & name, const IColumnDistributionStatisticPtr & stat) = 0;
    virtual void remove(const String & name) = 0;
};

using IColumnDistributionStatisticsPtr = std::shared_ptr<IColumnDistributionStatistics>;
using IConstColumnDistributionStatisticsPtr = std::shared_ptr<const IColumnDistributionStatistics>;

class IStatistics {
public:
    virtual ~IStatistics() = default;

    virtual bool empty() const = 0;

    virtual void merge(const std::shared_ptr<IStatistics>& other) = 0;

    virtual void serializeBinary(WriteBuffer & ostr) const = 0;
    virtual void deserializeBinary(ReadBuffer & istr) = 0;

    virtual void setColumnDistributionStatistics(IColumnDistributionStatisticsPtr && stat) = 0;
    virtual IConstColumnDistributionStatisticsPtr getColumnDistributionStatistics() const = 0;
};

using IStatisticsPtr = std::shared_ptr<IStatistics>;

}
