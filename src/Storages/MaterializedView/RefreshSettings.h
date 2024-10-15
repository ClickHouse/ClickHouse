#pragma once

#include <Core/BaseSettings.h>

namespace DB
{

#define LIST_OF_REFRESH_SETTINGS(M, ALIAS) \
    M(Int64, refresh_retries, 2, "How many times to retry refresh query if it fails. If all attempts fail, wait for the next refresh time according to schedule. 0 to disable retries. -1 for infinite retries.", 0) \
    M(UInt64, refresh_retry_initial_backoff_ms, 100, "Delay before the first retry if refresh query fails (if refresh_retries setting is not zero). Each subsequent retry doubles the delay, up to refresh_retry_max_backoff_ms.", 0) \
    M(UInt64, refresh_retry_max_backoff_ms, 60'000, "Limit on the exponential growth of delay between refresh attempts, if they keep failing and refresh_retries is positive.", 0) \
    M(Bool, all_replicas, /* do not change or existing tables will break */ false, "If the materialized view is in a Replicated database, and APPEND is enabled, this flag controls whether all replicas or one replica will refresh.", 0) \

DECLARE_SETTINGS_TRAITS(RefreshSettingsTraits, LIST_OF_REFRESH_SETTINGS)

struct RefreshSettings : public BaseSettings<RefreshSettingsTraits> {};

}
