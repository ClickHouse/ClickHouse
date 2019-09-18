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

/// This generates simple report functions for minimal threshold conditions
#define DEFINE_REPORT_FUNC(REPORT_FUNC, CONDITION)                                                       \
    void REPORT_FUNC(UInt64 value)                                                                       \
    {                                                                                                    \
        conditions_all_of.reportMinimalThresholdCondition(value, conditions_all_of.CONDITION);           \
        conditions_any_of.reportMinimalThresholdCondition(value, conditions_any_of.CONDITION);           \
    }                                                                                                    \

    DEFINE_REPORT_FUNC(reportRowsRead, rows_read)
    DEFINE_REPORT_FUNC(reportIterations, iterations)
    DEFINE_REPORT_FUNC(reportTotalTime, total_time_ms)
    DEFINE_REPORT_FUNC(reportBytesReadUncompressed, bytes_read_uncompressed)
    DEFINE_REPORT_FUNC(reportMinTimeNotChangingFor, min_time_not_changing_for_ms)
    DEFINE_REPORT_FUNC(reportMaxSpeedNotChangingFor, max_speed_not_changing_for_ms)
    DEFINE_REPORT_FUNC(reportAverageSpeedNotChangingFor, average_speed_not_changing_for_ms)

#undef DEFINE_REPORT_FUNC

/// This is not that necessary, but may be useful for debugging and future improvement
#define DEFINE_IS_FUNCS(INITIALIZED_FUNC, FULFILLED_FUNC, CONDITION)                                     \
    bool INITIALIZED_FUNC() const                                                                        \
    {                                                                                                    \
        return conditions_all_of.CONDITION.initialized || conditions_any_of.CONDITION.initialized;       \
    }                                                                                                    \
    bool FULFILLED_FUNC() const                                                                          \
    {                                                                                                    \
        return conditions_all_of.CONDITION.fulfilled || conditions_any_of.CONDITION.fulfilled;           \
    }                                                                                                    \

    DEFINE_IS_FUNCS(isInitializedTTestWithConfidenceLevel, isFulfilledTTestWithConfidenceLevel, t_test_with_confidence_level)

#undef DEFINE_IS_FUNCS


    void reportTTest(StudentTTest & t_test)
    {
        conditions_all_of.reportTTestCondition(t_test, conditions_all_of.t_test_with_confidence_level);
        conditions_any_of.reportTTestCondition(t_test, conditions_any_of.t_test_with_confidence_level);
    }

    void reset()
    {
        conditions_all_of.reset();
        conditions_any_of.reset();
    }

    inline bool empty() const
    {
        return !conditions_all_of.initialized_count && !conditions_any_of.initialized_count;
    }

    void loadFromConfig(ConfigurationPtr & stop_conditions_config);
    bool areFulfilled() const;

    /// Return max exec time for these conditions
    /// Return zero if max time cannot be determined
    UInt64 getMaxExecTime() const;

    StopConditionsSet conditions_all_of;
    StopConditionsSet conditions_any_of;
};

using TestStopConditions = std::vector<ConnectionTestStopConditions>;

}
