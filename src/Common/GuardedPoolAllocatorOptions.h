#pragma once

#include <Common/GuardedPoolAllocatorOptions.inc>

namespace clickhouse_gwp_asan
{

struct Options
{
    /// Read the options from the included definitions file.
    /// Use macros to declare variables.

#define M(TYPE, NAME, DEFAULT, DESCRIPTION) TYPE NAME = DEFAULT;
    CLICKHOUSE_GWP_ASAN_OPTIONS(M)
#undef M

    /// Use macros in setDefaults to set default values
    void setDefaults()
    {
#define M(TYPE, NAME, DEFAULT, DESCRIPTION) NAME = DEFAULT;
    CLICKHOUSE_GWP_ASAN_OPTIONS(M)
#undef M
    }
};

// Parse the options from the GWP_ASAN_OPTIONS environment variable.
bool initOptions();
// Returns the initialised options. Call initOptions() prior to calling this
// function.
const Options & getOptions();

}
