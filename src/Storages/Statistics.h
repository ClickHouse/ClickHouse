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
    /// Distribution for values in column
    NUMERIC_COLUMN_DISRIBUTION = 1,
    STRING_SEARCH = 2,
    LAST,
};


/// Basic class for column statistics
class IStatistic
{
public:
    virtual ~IStatistic() = default;

    virtual const String& name() const = 0;
    virtual const String& type() const = 0;
    virtual StatisticType statisticType() const = 0;

    virtual bool empty() const = 0;
    virtual void merge(const std::shared_ptr<IStatistic> & other) = 0;

    virtual const String & getColumnsRequiredForStatisticCalculation() const = 0;

    virtual void serializeBinary(WriteBuffer & ostr) const = 0;
    /// Checks if statistic has right type
    virtual bool validateTypeBinary(ReadBuffer & istr) const = 0;
    virtual void deserializeBinary(ReadBuffer & istr) = 0;

    virtual size_t getSizeInMemory() const = 0;
};

using IStatisticPtr = std::shared_ptr<IStatistic>;


class IDistributionStatistic : public IStatistic
{
public:
    StatisticType statisticType() const override { return StatisticType::NUMERIC_COLUMN_DISRIBUTION; }

    /// Some quantile of value smaller than value
    virtual double estimateQuantileLower(double value) const = 0;
    /// Some quantile of value greater than value
    virtual double estimateQuantileUpper(double value) const = 0;
    /// Upper bound of probability of lower <= value <= upper
    virtual double estimateProbability(const Field & lower, const Field & upper) const = 0;
};

using IDistributionStatisticPtr = std::shared_ptr<IDistributionStatistic>;
using IDistributionStatisticPtrs = std::vector<IDistributionStatisticPtr>;


class IDistributionStatistics
{
public:
    virtual ~IDistributionStatistics() = default;

    virtual bool empty() const = 0;

    virtual void merge(const std::shared_ptr<IDistributionStatistics> & other) = 0;

    virtual Names getStatisticsNames() const = 0;
    virtual void serializeBinary(const String & name, WriteBuffer & ostr) const = 0;
    virtual void deserializeBinary(ReadBuffer & istr) = 0;

    /// Estimates probability that lower <= item  <= right.
    virtual std::optional<double> estimateProbability(const String & column, const Field & lower, const Field & upper) const = 0;

    virtual void add(const String & name, const IDistributionStatisticPtr & stat) = 0;
    virtual bool has(const String & name) const = 0;

    virtual size_t getSizeInMemory() const = 0;
    virtual size_t getSizeInMemoryByName(const String& name) const = 0;
};

using IDistributionStatisticsPtr = std::shared_ptr<IDistributionStatistics>;
using IConstDistributionStatisticsPtr = std::shared_ptr<const IDistributionStatistics>;


class IStringSearchStatistic : public IStatistic
{
public:
    StatisticType statisticType() const override { return StatisticType::STRING_SEARCH; }

    virtual double estimateStringProbability(const String& needle) const = 0;

    virtual std::optional<double> estimateSubstringsProbability(const Strings& needles) const = 0;
};

using IStringSearchStatisticPtr = std::shared_ptr<IStringSearchStatistic>;
using IStringSearchStatisticPtrs = std::vector<IStringSearchStatisticPtr>;


class IStringSearchStatistics
{
public:
    virtual ~IStringSearchStatistics() = default;

    virtual bool empty() const = 0;

    virtual void merge(const std::shared_ptr<IStringSearchStatistics> & other) = 0;

    virtual Names getStatisticsNames() const = 0;
    virtual void serializeBinary(const String & name, WriteBuffer & ostr) const = 0;
    virtual void deserializeBinary(ReadBuffer & istr) = 0;

    virtual std::optional<double> estimateStringProbability(const String & column, const String& needle) const = 0;
    virtual std::optional<double> estimateSubstringsProbability(const String & column, const Strings& needles) const = 0;

    virtual void add(const String & name, const IStringSearchStatisticPtr & stat) = 0;
    virtual bool has(const String & name) const = 0;

    virtual size_t getSizeInMemory() const = 0;
    virtual size_t getSizeInMemoryByName(const String& name) const = 0;
};

using IStringSearchStatisticsPtr = std::shared_ptr<IStringSearchStatistics>;
using IConstStringSearchStatisticsPtr = std::shared_ptr<const IStringSearchStatistics>;


class IStatistics
{
public:
    virtual ~IStatistics() = default;

    virtual bool empty() const = 0;

    virtual void merge(const std::shared_ptr<IStatistics>& other) = 0;

    virtual Names getStatisticsNames() const = 0;
    virtual void serializeBinary(const String & name, WriteBuffer & ostr) const = 0;
    virtual void deserializeBinary(ReadBuffer & istr) = 0;

    virtual void setDistributionStatistics(IDistributionStatisticsPtr && stat) = 0;
    virtual IConstDistributionStatisticsPtr getDistributionStatistics() const = 0;

    virtual void setStringSearchStatistics(IStringSearchStatisticsPtr && stat) = 0;
    virtual IConstStringSearchStatisticsPtr getStringSearchStatistics() const = 0;

    virtual size_t getSizeInMemory() const = 0;
};

using IStatisticsPtr = std::shared_ptr<IStatistics>;

}
