#pragma once
#include "StopConditionsSet.h"
#include <Poco/Logger.h>
#include <Poco/Util/XMLConfiguration.h>

namespace DB
{
/// Stop conditions for a test run. The running test will be terminated in either of two conditions:
/// 1. All conditions marked 'all_of' are fulfilled
/// or
/// 2. Any condition  marked 'any_of' is  fulfilled

using ConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;

class ConnectionTestStopConditions
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
        conditions_all_of.reportMinimalThresholdCondition(value, conditions_all_of.CONDITION); \
        conditions_any_of.reportMinimalThresholdCondition(value, conditions_any_of.CONDITION); \
    }

    DEFINE_REPORT_FUNC(reportRowsRead, rows_read)
    DEFINE_REPORT_FUNC(reportIterations, iterations)
    DEFINE_REPORT_FUNC(reportTotalTime, total_time_ms)
    DEFINE_REPORT_FUNC(reportBytesReadUncompressed, bytes_read_uncompressed)
    DEFINE_REPORT_FUNC(reportMinTimeNotChangingFor, min_time_not_changing_for_ms)
    DEFINE_REPORT_FUNC(reportMaxSpeedNotChangingFor, max_speed_not_changing_for_ms)
    DEFINE_REPORT_FUNC(reportAverageSpeedNotChangingFor, average_speed_not_changing_for_ms)

#undef REPORT

#define DEFINE_INITIALIZED_FUNC(FUNC_NAME, CONDITION)                 \
    bool FUNC_NAME() const                                            \
    {                                                                 \
        return conditions_all_of.CONDITION.initialized || conditions_any_of.CONDITION.initialized; \
    }

        DEFINE_INITIALIZED_FUNC(isInitializedRowsRead, rows_read)
        DEFINE_INITIALIZED_FUNC(isInitializedIterations, iterations)
        DEFINE_INITIALIZED_FUNC(isInitializedTotalTime, total_time_ms)
        DEFINE_INITIALIZED_FUNC(isInitializedBytesReadUncompressed, bytes_read_uncompressed)
        DEFINE_INITIALIZED_FUNC(isInitializedMinTimeNotChangingFor, min_time_not_changing_for_ms)
        DEFINE_INITIALIZED_FUNC(isInitializedMaxSpeedNotChangingFor, max_speed_not_changing_for_ms)
        DEFINE_INITIALIZED_FUNC(isInitializedAverageSpeedNotChangingFor, average_speed_not_changing_for_ms)
        DEFINE_INITIALIZED_FUNC(isInitializedTTestWithConfidenceLevel, t_test_with_confidence_level)


#undef INITIALIZED

#define DEFINE_FULFILLED_FUNC(FUNC_NAME, CONDITION)                   \
    bool FUNC_NAME() const                                            \
    {                                                                 \
        return conditions_all_of.CONDITION.fulfilled || conditions_any_of.CONDITION.fulfilled; \
    }

    DEFINE_FULFILLED_FUNC(isFullfiledRowsRead, rows_read)
    DEFINE_FULFILLED_FUNC(isFullfiledIterations, iterations)
    DEFINE_FULFILLED_FUNC(isFullfiledTotalTime, total_time_ms)
    DEFINE_FULFILLED_FUNC(isFullfiledBytesReadUncompressed, bytes_read_uncompressed)
    DEFINE_FULFILLED_FUNC(isFullfiledMinTimeNotChangingFor, min_time_not_changing_for_ms)
    DEFINE_FULFILLED_FUNC(isFullfiledMaxSpeedNotChangingFor, max_speed_not_changing_for_ms)
    DEFINE_FULFILLED_FUNC(isFullfiledAverageSpeedNotChangingFor, average_speed_not_changing_for_ms)
    DEFINE_FULFILLED_FUNC(isFullfiledTTestWithConfidenceLevel, t_test_with_confidence_level)


#undef FULFILLED

    void reportTTest(StudentTTest & t_test)
    {
        conditions_all_of.reportTTestCondition(t_test, conditions_all_of.t_test_with_confidence_level);
        conditions_any_of.reportTTestCondition(t_test, conditions_any_of.t_test_with_confidence_level);
    }

    bool areFulfilled() const;

    void reset()
    {
        conditions_all_of.reset();
        conditions_any_of.reset();
    }

    /// Return max exec time for these conditions
    /// Return zero if max time cannot be determined
    UInt64 getMaxExecTime() const;

    StopConditionsSet conditions_all_of;
    StopConditionsSet conditions_any_of;
};

using TestStopConditions = std::vector<ConnectionTestStopConditions>;

}
