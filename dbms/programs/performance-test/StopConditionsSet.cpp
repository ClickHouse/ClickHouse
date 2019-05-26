#include "StopConditionsSet.h"
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
            total_time_ms.value = stop_conditions_view->getUInt64(key);
        else if (key == "rows_read")
            rows_read.value = stop_conditions_view->getUInt64(key);
        else if (key == "bytes_read_uncompressed")
            bytes_read_uncompressed.value = stop_conditions_view->getUInt64(key);
        else if (key == "iterations")
            iterations.value = stop_conditions_view->getUInt64(key);
        else if (key == "min_time_not_changing_for_ms")
            min_time_not_changing_for_ms.value = stop_conditions_view->getUInt64(key);
        else if (key == "max_speed_not_changing_for_ms")
            max_speed_not_changing_for_ms.value = stop_conditions_view->getUInt64(key);
        else if (key == "average_speed_not_changing_for_ms")
            average_speed_not_changing_for_ms.value = stop_conditions_view->getUInt64(key);
        else
            throw Exception("Met unkown stop condition: " + key, ErrorCodes::LOGICAL_ERROR);
    }
    ++initialized_count;
}

void StopConditionsSet::reset()
{
    total_time_ms.fulfilled = false;
    rows_read.fulfilled = false;
    bytes_read_uncompressed.fulfilled = false;
    iterations.fulfilled = false;
    min_time_not_changing_for_ms.fulfilled = false;
    max_speed_not_changing_for_ms.fulfilled = false;
    average_speed_not_changing_for_ms.fulfilled = false;

    fulfilled_count = 0;
}

void StopConditionsSet::report(UInt64 value, StopConditionsSet::StopCondition & condition)
{
    if (condition.value && !condition.fulfilled && value >= condition.value)
    {
        condition.fulfilled = true;
        ++fulfilled_count;
    }
}



}
