#pragma once

#include <Processors/QueryPlan/StepStatisticsModel.h>
#include <Common/JSONBuilder.h>

#include <memory>


namespace DB
{

/// Renders the statistics of a single plan step as JSON. The sibling of StepStatisticsASCIIPrinter, which
/// renders the same value as text, so that both read from one analysis rather than one of them
/// re-deriving it.
///
/// Values are written raw: nanoseconds, rows and bytes as numbers, never as "4.04 ms" or
/// "142.86 thousand". The point of storing the statistics as JSON rather than as the rendered plan
/// is that they can be aggregated by a query, which formatted values would prevent.
class StepStatisticsJSONPrinter
{
public:
    static std::unique_ptr<JSONBuilder::JSONMap> toJSON(const AnalyzedStepData & step_data);
};

}
