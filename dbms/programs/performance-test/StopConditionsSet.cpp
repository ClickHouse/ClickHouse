#include "StopConditionsSet.h"
#include "TestStats.h"

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

void StopConditionsSet::loadFromConfig(const ConfigurationPtr & stop_conditions_view)
{
    Strings keys;
    stop_conditions_view->keys(keys);

    for (const std::string & key : keys)
    {
        if (key == "total_time_ms")
        {
            total_time_ms.initialized = true;
            total_time_ms.value = stop_conditions_view->getUInt64(key);
        }
        else if (key == "rows_read")
        {
            rows_read.initialized = true;
            rows_read.value = stop_conditions_view->getUInt64(key);
        }
        else if (key == "bytes_read_uncompressed")
        {
            bytes_read_uncompressed.initialized = true;
            bytes_read_uncompressed.value = stop_conditions_view->getUInt64(key);
        }
        else if (key == "iterations")
        {
            iterations.initialized = true;
            iterations.value = stop_conditions_view->getUInt64(key);
        }
        else if (key == "min_time_not_changing_for_ms")
        {
            min_time_not_changing_for_ms.initialized = true;
            min_time_not_changing_for_ms.value = stop_conditions_view->getUInt64(key);
        }
        else if (key == "max_speed_not_changing_for_ms")
        {
            max_speed_not_changing_for_ms.initialized = true;
            max_speed_not_changing_for_ms.value = stop_conditions_view->getUInt64(key);
        }
        else if (key == "average_speed_not_changing_for_ms")
        {
            average_speed_not_changing_for_ms.initialized = true;
            average_speed_not_changing_for_ms.value = stop_conditions_view->getUInt64(key);
        }
        else if (key == "t_test_with_confidence_level")
        {
            t_test_with_confidence_level.initialized = true;
            t_test_with_confidence_level.value = stop_conditions_view->getUInt64(key);
            ConnectionTestStats::t_test_confidence_level = t_test_with_confidence_level.value;
        }
        else
            throw Exception("Met unknown stop condition: " + key, ErrorCodes::LOGICAL_ERROR);

        ++initialized_count;
    }
}

void StopConditionsSet::reset()
{
    fulfilled_count = 0;
    rows_read.fulfilled = false;
    iterations.fulfilled = false;
    total_time_ms.fulfilled = false;
    bytes_read_uncompressed.fulfilled = false;
    t_test_with_confidence_level.fulfilled = false;
    min_time_not_changing_for_ms.fulfilled = false;
    max_speed_not_changing_for_ms.fulfilled = false;
    average_speed_not_changing_for_ms.fulfilled = false;
}

void StopConditionsSet::reportMinimalThresholdCondition(UInt64 value, StopCondition & condition)
{
    if (condition.initialized && !condition.fulfilled && value >= condition.value)
    {
        condition.fulfilled = true;
        ++fulfilled_count;
    }
}

void StopConditionsSet::reportTTestCondition(StudentTTest & t_test, StopCondition & condition)
{
    if (condition.initialized && !condition.fulfilled &&
        t_test.distributionsDiffer(ConnectionTestStats::t_test_confidence_level,
                                   ConnectionTestStats::t_test_comparison_precision))
    {
        condition.fulfilled  = true;
        ++fulfilled_count;
    }
}

}
