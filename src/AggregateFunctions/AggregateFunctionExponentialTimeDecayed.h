#pragma once

#include <AggregateFunctions/IAggregateFunction.h>


namespace DB
{

struct Settings;

class ExperimentalTimeDecayAggregateFunctionMetadataScope
{
public:
    explicit ExperimentalTimeDecayAggregateFunctionMetadataScope(bool enabled_);
    ~ExperimentalTimeDecayAggregateFunctionMetadataScope();

    ExperimentalTimeDecayAggregateFunctionMetadataScope(const ExperimentalTimeDecayAggregateFunctionMetadataScope &) = delete;
    ExperimentalTimeDecayAggregateFunctionMetadataScope & operator=(const ExperimentalTimeDecayAggregateFunctionMetadataScope &) = delete;

private:
    bool enabled;
    bool previous_value;
};

AggregateFunctionPtr createAggregateFunctionExponentialTimeDecayedSum(
    const String & name,
    const DataTypes & argument_types,
    const Array & parameters,
    const Settings * settings);

AggregateFunctionPtr createAggregateFunctionExponentialTimeDecayingFloat64(
    const String & name,
    const DataTypes & argument_types,
    const Array & parameters,
    const Settings * settings);

AggregateFunctionPtr createAggregateFunctionExponentialTimeDecayedAvg(
    const String & name,
    const DataTypes & argument_types,
    const Array & parameters,
    const Settings * settings);

AggregateFunctionPtr createAggregateFunctionExponentialTimeDecayedCount(
    const String & name,
    const DataTypes & argument_types,
    const Array & parameters,
    const Settings * settings);

}
