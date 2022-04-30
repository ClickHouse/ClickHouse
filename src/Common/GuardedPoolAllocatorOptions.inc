#pragma once

#define CLICKHOUSE_GWP_ASAN_OPTIONS(M) \
    M(bool, enabled, true, "Is GWP-ASan enabled? Defaults to true.") \
    M( \
        int, \
        max_simultaneous_allocations, \
        32, \
        "Number of simultaneously-guarded allocations available in the " \
        "pool. Defaults to 32.") \
    M( \
        int, \
        sample_rate, \
        100000, \
        "The probability (1 / sample_rate) that an allocation is " \
        "selected for GWP-ASan sampling. Default is 100000. Sample rates " \
        "up to (2^30 - 1) are supported.") \
    M( \
        int, \
        slot_size, \
        1, \
        "Number of pages reserved for allocation slot (In other words this is a maximum allocation in pages that can be guarded). Defaults to 1 page.") \
    M(bool, help, false, "Print a summary of the available options.")
