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
    // Distribution for values in column
    COLUMN_DISRIBUTION = 1,
    // The same but value is counted only once per block
    BLOCK_DISTRIBUTION = 2,
    //COLUMN_MOST_FREQUENT, -- in list => bad selectivity
    //COLUMN_DISTINCT_COUNT, -- for join/group by
    //COLUMN_LOW_CARDINALITY_COUNT, -- exact per block stats for low cardinality
    //COLUMN_COUNT_SKETCH, -- per block count
    //COLUMN_NULL_COUNT, -- like postgres
    //COLUMN_CORRELATION_WITH_PRIMARY, -- like postgres
    //...
    LAST,
};


// Basic class for column statistics
class IStatistic {
public:
    virtual ~IStatistic() = default;

    virtual const String& name() const = 0;

    virtual const String& type() const = 0;

    virtual bool empty() const = 0;
    virtual void merge(const std::shared_ptr<IStatistic> & other) = 0;

    virtual const String & getColumnsRequiredForStatisticCalculation() const = 0;

    virtual void serializeBinary(WriteBuffer & ostr) const = 0;
    virtual void deserializeBinary(ReadBuffer & istr) = 0;
};

using IStatisticPtr = std::shared_ptr<IStatistic>;


class IDistributionStatistic : public IStatistic {
public:
    // some quantile of value smaller than value 
    virtual double estimateQuantileLower(const Field& value) const = 0;
    // some quantile of value greater than value
    virtual double estimateQuantileUpper(const Field& value) const = 0;
    // upper bound of probability of lower <= value <= upper
    virtual double estimateProbability(const Field & lower, const Field & upper) const = 0;
};

using IDistributionStatisticPtr = std::shared_ptr<IDistributionStatistic>;
using IDistributionStatisticPtrs = std::vector<IDistributionStatisticPtr>;


class IDistributionStatistics {
public:
    virtual ~IDistributionStatistics() = default;

    virtual bool empty() const = 0;

    virtual void merge(const std::shared_ptr<IDistributionStatistics> & other) = 0;

    virtual Names getStatisticsNames() const = 0;
    virtual void serializeBinary(const String & name, WriteBuffer & ostr) const = 0;
    virtual void deserializeBinary(ReadBuffer & istr) = 0;

    virtual std::optional<double> estimateProbability(const String & column, const Field & lower, const Field & upper) const = 0;
    virtual void add(const String & name, const IDistributionStatisticPtr & stat) = 0;
};

using IDistributionStatisticsPtr = std::shared_ptr<IDistributionStatistics>;
using IConstDistributionStatisticsPtr = std::shared_ptr<const IDistributionStatistics>;


class IStatistics {
public:
    virtual ~IStatistics() = default;

    virtual bool empty() const = 0;

    virtual void merge(const std::shared_ptr<IStatistics>& other) = 0;

    virtual Names getStatisticsNames() const = 0;
    virtual void serializeBinary(const String & name, WriteBuffer & ostr) const = 0;
    virtual void deserializeBinary(ReadBuffer & istr) = 0;

    virtual void setDistributionStatistics(IDistributionStatisticsPtr && stat) = 0;
    virtual IConstDistributionStatisticsPtr getDistributionStatistics() const = 0;
};

using IStatisticsPtr = std::shared_ptr<IStatistics>;

}
