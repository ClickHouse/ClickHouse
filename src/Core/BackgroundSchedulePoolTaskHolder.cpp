#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <Core/BackgroundSchedulePool.h>

namespace DB
{

BackgroundSchedulePoolTaskHolder::~BackgroundSchedulePoolTaskHolder()
{
    if (task_info)
        task_info->deactivate();
}

}
