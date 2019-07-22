#pragma once

#include <Core/Types.h>
#include <Poco/Util/XMLConfiguration.h>

namespace DB
{

using ConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;

/// A set of supported stop conditions.
struct StopConditionsSet
{
    void loadFromConfig(const ConfigurationPtr & stop_conditions_view);
    void reset();

    /// Note: only conditions with UInt64 minimal thresholds are supported.
    /// I.e. condition is fulfilled when value is exceeded.
    struct StopCondition
    {
        UInt64 value = 0;
        bool fulfilled = false;
    };

    void report(UInt64 value, StopCondition & condition);

    StopCondition total_time_ms;
    StopCondition rows_read;
    StopCondition bytes_read_uncompressed;
    StopCondition iterations;
    StopCondition min_time_not_changing_for_ms;
    StopCondition max_speed_not_changing_for_ms;
    StopCondition average_speed_not_changing_for_ms;

    size_t initialized_count = 0;
    size_t fulfilled_count = 0;
};

}
