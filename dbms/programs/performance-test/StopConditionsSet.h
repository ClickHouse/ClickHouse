#pragma once

#include <Core/Types.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Poco/Logger.h>
#include <Common/StudentTTest.h>
#include <common/logger_useful.h>

namespace DB
{

using ConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;

struct StopCondition
{
    UInt64 value = 0;
    bool fulfilled = false;
    bool initialized = false;
};

/// A set of supported stop conditions.
struct StopConditionsSet
{
    void loadFromConfig(const ConfigurationPtr & stop_conditions_view);
    void reset();

    /// I.e. condition is fulfilled when value is exceeded.
    void reportMinimalThresholdCondition(UInt64 value, StopCondition & condition);

    void reportTTestCondition(StudentTTest & t_test, StopCondition & condition);

    StopCondition total_time_ms;
    StopCondition rows_read;
    StopCondition bytes_read_uncompressed;
    StopCondition iterations;
    StopCondition min_time_not_changing_for_ms;
    StopCondition max_speed_not_changing_for_ms;
    StopCondition average_speed_not_changing_for_ms;
    StopCondition t_test_with_confidence_level;

    size_t initialized_count = 0;
    size_t fulfilled_count = 0;
};

}
