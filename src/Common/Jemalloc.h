#pragma once

#include "config.h"

#if USE_JEMALLOC

#include <string>
#include <Common/logger_useful.h>
#include <jemalloc/jemalloc.h>

namespace DB
{

void purgeJemallocArenas();

void checkJemallocProfilingEnabled();

void setJemallocProfileActive(bool value);

std::string flushJemallocProfile(const std::string & file_prefix);

void setJemallocBackgroundThreads(bool enabled);

void setJemallocMaxBackgroundThreads(size_t max_threads);

template <typename T>
void setJemallocValue(const char * name, T value)
{
    T old_value;
    size_t old_value_size = sizeof(T);
    mallctl(name, &old_value, &old_value_size, reinterpret_cast<void*>(&value), sizeof(T));
    LOG_INFO(getLogger("Jemalloc"), "Value for {} set to {} (from {})", name, value, old_value);
}

template <typename T>
T getJemallocValue(const char * name)
{
    T value;
    size_t value_size = sizeof(T);
    mallctl(name, &value, &value_size, nullptr, 0);
    return value;
}

/// Each mallctl call consists of string name lookup which can be expensive.
/// This can be avoided by translating name to "Management Information Base" (MIB)
/// and using it in mallctlbymib calls
template <typename T>
struct JemallocMibCache
{
    explicit JemallocMibCache(const char * name)
    {
        mallctlnametomib(name, mib, &mib_length);
    }

    void setValue(T value)
    {
        mallctlbymib(mib, mib_length, nullptr, nullptr, reinterpret_cast<void*>(&value), sizeof(T));
    }

    T getValue()
    {
        T value;
        size_t value_size = sizeof(T);
        mallctlbymib(mib, mib_length, &value, &value_size, nullptr, 0);
        return value;
    }

    void run()
    {
        mallctlbymib(mib, mib_length, nullptr, nullptr, nullptr, 0);
    }

private:
    static constexpr size_t max_mib_length = 4;
    size_t mib[max_mib_length];
    size_t mib_length = max_mib_length;
};

}

#endif
