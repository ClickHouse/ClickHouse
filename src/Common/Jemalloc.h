#pragma once

#include "config.h"

#include <cstddef>

namespace DB::Jemalloc
{

constexpr bool default_enable_global_profiler = false;
constexpr bool default_enable_background_threads = true;
constexpr size_t default_max_background_threads_num = 0;
constexpr bool default_collect_global_profile_samples_in_trace_log = false;
constexpr size_t default_profiler_sampling_rate = 19;

/// Config key names used in both `BaseDaemon::initialize` and `ServerSettings` DECLARE macros.
constexpr auto config_enable_global_profiler = "jemalloc_enable_global_profiler";
constexpr auto config_enable_background_threads = "jemalloc_enable_background_threads";
constexpr auto config_max_background_threads_num = "jemalloc_max_background_threads_num";
constexpr auto config_collect_global_profile_samples_in_trace_log = "jemalloc_collect_global_profile_samples_in_trace_log";
constexpr auto config_profiler_sampling_rate = "jemalloc_profiler_sampling_rate";

}

#if USE_JEMALLOC

#include <string_view>
#include <Common/logger_useful.h>
#include <jemalloc/jemalloc.h>

namespace DB
{

struct ServerSettings;

namespace Jemalloc
{

void purgeArenas();

void checkProfilingEnabled();

std::string_view flushProfile(const char * file_prefix);

void setBackgroundThreads(bool enabled);

void setProfileSamplingRate(size_t lg_prof_sample);

void setMaxBackgroundThreads(size_t max_threads);

template <typename T>
void setValue(const char * name, T value)
{
    je_mallctl(name, nullptr, nullptr, reinterpret_cast<void*>(&value), sizeof(T));
}

template <typename T>
T getValue(const char * name)
{
    T value;
    size_t value_size = sizeof(T);
    je_mallctl(name, &value, &value_size, nullptr, 0);
    return value;
}

void setup(
    bool enable_global_profiler,
    bool enable_background_threads,
    size_t max_background_threads_num,
    bool collect_global_profile_samples_in_trace_log,
    size_t profiler_sampling_rate);

/// Verify that the current jemalloc settings match the expected values.
/// Called from Server/Keeper after `setup` was already called in `BaseDaemon::initialize`
/// (and re-applied after the watchdog fork). Fires `chassert` on mismatch.
void verifySetup(
    bool enable_global_profiler,
    bool enable_background_threads,
    size_t max_background_threads_num,
    bool collect_global_profile_samples_in_trace_log,
    size_t profiler_sampling_rate);

/// Each mallctl call consists of string name lookup which can be expensive.
/// This can be avoided by translating name to "Management Information Base" (MIB)
/// and using it in mallctlbymib calls
template <typename T>
struct MibCache
{
    explicit MibCache(const char * name)
    {
        je_mallctlnametomib(name, mib, &mib_length);
    }

    void setValue(T value) const
    {
        je_mallctlbymib(mib, mib_length, nullptr, nullptr, reinterpret_cast<void*>(&value), sizeof(T));
    }

    T getValue() const
    {
        T value;
        size_t value_size = sizeof(T);
        je_mallctlbymib(mib, mib_length, &value, &value_size, nullptr, 0);
        return value;
    }

    void run() const
    {
        je_mallctlbymib(mib, mib_length, nullptr, nullptr, nullptr, 0);
    }

private:
    static constexpr size_t max_mib_length = 4;
    size_t mib[max_mib_length];
    size_t mib_length = max_mib_length;
};

const MibCache<bool> & getThreadProfileActiveMib();
const MibCache<bool> & getThreadProfileInitMib();

void setCollectLocalProfileSamplesInTraceLog(bool value);

std::string_view getLastFlushProfileForThread();


}

}

#endif
