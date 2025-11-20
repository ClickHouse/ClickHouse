#pragma once

#include <Core/Field.h>
#include <Interpreters/StorageID.h>
#include <Storages/TimeSeries/PrometheusQueryEvaluationRange.h>


namespace DB
{

struct PrometheusQueryEvaluationSettings
{
    StorageID time_series_storage_id = StorageID::createEmpty();

    /// Either `evaluation_time` or `evaluation_range` should be set.
    /// Evaluate a prometheus query at a specified evaluation time.
    std::optional<DecimalField<DateTime64>> evaluation_time;

    /// Evaluate a prometheus query over a range of time.
    std::optional<PrometheusQueryEvaluationRange> evaluation_range;

    DecimalField<Decimal64> lookback_delta{5*60};   /// 5 minutes
    DecimalField<Decimal64> default_resolution{15}; /// 15 seconds

    DataTypePtr result_timestamp_type;

    std::optional<size_t> limit;
};

}
