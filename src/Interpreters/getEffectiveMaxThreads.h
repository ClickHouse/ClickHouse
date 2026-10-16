#pragma once

#include <cstddef>

namespace DB
{

struct Settings;

/// The number of threads (and streams) to plan a query with.
/// It is `max_threads` lowered under memory pressure (see `max_threads_min_free_memory_per_thread`)
/// and, if `max_threads` is auto, to the fair share of CPU slots of the concurrency control
/// (see `use_concurrency_control_for_max_threads`).
size_t getEffectiveMaxThreads(const Settings & settings);

}
