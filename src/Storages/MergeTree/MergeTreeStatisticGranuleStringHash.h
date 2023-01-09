#pragma once

#include <base/types.h>
#include <Storages/MergeTree/MergeTreeStatistic.h>
#include <Storages/Statistics.h>
#include "IO/ReadBuffer.h"
#include <optional>
#include <unordered_set>

namespace DB
{

class CountMinSketch
{
public:
    CountMinSketch();

    void addString(const String& str);

    size_t getStringCount(const String& str) const;
    size_t getSizeInMemory() const;

    void merge(const CountMinSketch& other);

    void serialize(WriteBuffer& wb) const;
    void deserialize(ReadBuffer& rb);

private:
    static size_t getStringHash(const String& str);
    static size_t getUintHash(size_t hash, size_t seed);

    std::vector<UInt32> data;
};

/*
Sketch for finding fraction of granules that can contain strings (todo: or substrings).
*/
class MergeTreeGranuleStringHashStatistic : public IStringSearchStatistic
{
public:
    MergeTreeGranuleStringHashStatistic(
        const String & name_,
        const String & column_name_);
    MergeTreeGranuleStringHashStatistic(
        const String & name_,
        const String & column_name_,
        size_t total_granules_,
        CountMinSketch&& sketch_);

    const String& name() const override;
    const String& type() const override;
    bool empty() const override;
    void merge(const IStatisticPtr & other) override;

    const String& getColumnsRequiredForStatisticCalculation() const override;

    void serializeBinary(WriteBuffer & ostr) const override;
    bool validateTypeBinary(ReadBuffer & istr) const override;
    void deserializeBinary(ReadBuffer & istr) override;

    double estimateStringProbability(const String& needle) const override;
    std::optional<double> estimateSubstringsProbability(const Strings& needles) const override;

    size_t getSizeInMemory() const override;

private:
    const String statistic_name;
    const String column_name;
    size_t total_granules;
    CountMinSketch sketch;
    bool is_empty;
};

class MergeTreeGranuleStringHashStatisticCollector : public IMergeTreeStatisticCollector
{
public:
    MergeTreeGranuleStringHashStatisticCollector(
        const String & name_,
        const String & column_name_);

    const String& name() const override;
    const String & type() const override;

    StatisticType statisticType() const override;

    const String & column() const override;
    bool empty() const override;
    IStringSearchStatisticPtr getStringSearchStatisticAndReset() override;

    void update(const Block & block, size_t * pos, size_t limit) override;
    void granuleFinished() override;

private:
    const String statistic_name;
    const String column_name;
    std::unordered_set<std::string> granule_string_set;
    CountMinSketch sketch;
    size_t total_granules;
};

IStringSearchStatisticPtr creatorGranuleStringHashStatistic(
    const StatisticDescription & statistic, const ColumnDescription & column);
IMergeTreeStatisticCollectorPtr creatorGranuleStringHashStatisticCollector(
    const StatisticDescription & statistic, const ColumnDescription & column);
void validatorGranuleStringHashStatistic(
    const StatisticDescription & statistic, const ColumnDescription & column);
}
