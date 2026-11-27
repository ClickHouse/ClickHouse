#pragma once

#include <Interpreters/Context_fwd.h>

#include <base/types.h>

#include <Core/BackgroundSchedulePoolTaskHolder.h>

#include <Common/AggregatedMetrics.h>
#include <Common/DynamicDelay.h>
#include <Common/Logger_fwd.h>

namespace DB
{
class DiskLocal;

class DiskLocalCheckThread : WithContext
{
    void run();

public:
    DiskLocalCheckThread(DiskLocal * disk_, ContextPtr context_, int64_t local_disk_check_period_ms);
    ~DiskLocalCheckThread();

    void startup();
    void shutdown();

private:
    DiskLocal * disk;
    /// Unique per thread: one disk can have two DiskLocal objects, which must not probe one file.
    const String check_file_path;
    DynamicDelay check_period;
    const LoggerPtr log;

    BackgroundSchedulePoolTaskHolder task;

    AggregatedMetrics::GlobalSum is_readonly;
    AggregatedMetrics::GlobalSum is_broken;
};

}
