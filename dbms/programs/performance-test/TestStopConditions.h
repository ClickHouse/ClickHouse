#pragma once
#include "StopConditionsSet.h"
#include <Poco/Util/XMLConfiguration.h>

namespace DB
{
/// Stop conditions for a test run. The running test will be terminated in either of two conditions:
/// 1. All conditions marked 'all_of' are fulfilled
/// or
/// 2. Any condition  marked 'any_of' is  fulfilled

using ConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;

class TestStopConditions
{
public:
    void loadFromConfig(ConfigurationPtr & stop_conditions_config);
    inline bool empty() const
    {
        return !conditions_all_of.initialized_count && !conditions_any_of.initialized_count;
    }

#define DEFINE_REPORT_FUNC(FUNC_NAME, CONDITION)                      \
    void FUNC_NAME(UInt64 value)                                      \
    {                                                                 \
        conditions_all_of.report(value, conditions_all_of.CONDITION); \
        conditions_any_of.report(value, conditions_any_of.CONDITION); \
    }

    DEFINE_REPORT_FUNC(reportTotalTime, total_time_ms)
    DEFINE_REPORT_FUNC(reportRowsRead, rows_read)
    DEFINE_REPORT_FUNC(reportBytesReadUncompressed, bytes_read_uncompressed)
    DEFINE_REPORT_FUNC(reportIterations, iterations)
    DEFINE_REPORT_FUNC(reportMinTimeNotChangingFor, min_time_not_changing_for_ms)
    DEFINE_REPORT_FUNC(reportMaxSpeedNotChangingFor, max_speed_not_changing_for_ms)
    DEFINE_REPORT_FUNC(reportAverageSpeedNotChangingFor, average_speed_not_changing_for_ms)

#undef REPORT

    bool areFulfilled() const;

    void reset()
    {
        conditions_all_of.reset();
        conditions_any_of.reset();
    }

    /// Return max exec time for these conditions
    /// Return zero if max time cannot be determined
    UInt64 getMaxExecTime() const;

private:
    StopConditionsSet conditions_all_of;
    StopConditionsSet conditions_any_of;
};

}
