#pragma once
#include <QueryPipeline/SizeLimits.h>
#include <QueryPipeline/ExecutionSpeedLimits.h>
#include <list>

namespace DB
{

/** What limitations and quotas should be checked.
  * LIMITS_CURRENT - checks amount of data returned by current stream only (BlockStreamProfileInfo is used for check).
  *  Currently it is used in root streams to check max_result_{rows,bytes} limits.
  * LIMITS_TOTAL - checks total amount of read data from leaf streams (i.e. data read from disk and remote servers).
  *  It is checks max_{rows,bytes}_to_read in progress handler and use info from ProcessListElement::progress_in for this.
  *  Currently this check is performed only in leaf streams.
  */
enum class LimitsMode
{
    LIMITS_CURRENT,
    LIMITS_TOTAL,
};

/// It is a subset of limitations from Limits.
struct StreamLocalLimits
{
    LimitsMode mode = LimitsMode::LIMITS_CURRENT;

    SizeLimits size_limits;

    ExecutionSpeedLimits speed_limits;

    OverflowMode timeout_overflow_mode = OverflowMode::THROW;
};

struct StorageLimits
{
    StreamLocalLimits local_limits;
    SizeLimits leaf_limits;
};

using StorageLimitsList = std::list<StorageLimits>;

}
