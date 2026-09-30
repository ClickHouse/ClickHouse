#pragma once

#include <string>
#include <Processors/QueryPlan/StepStatisticsModel.h>


namespace DB
{

class WriteBuffer;

/// Renders the statistics of a single plan step as the text `EXPLAIN ANALYZE` prints.
/// It reads nothing but the value it is handed, so it needs neither the plan nor the pipeline the
/// statistics came from, and can run long after both are gone.
class StepStatisticsASCIIPrinter
{
public:
    static void print(const AnalyzedStepData & step_data, WriteBuffer & out, const std::string & prefix, bool processors_info);
};

}
