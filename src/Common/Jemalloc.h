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
    if (mallctl(name, &old_value, &old_value_size, reinterpret_cast<void*>(&value), sizeof(T)))
    {
        LOG_WARNING(getLogger("Jemalloc"), "mallctl for {} failed", name);
        return;
    }

    LOG_INFO(getLogger("Jemalloc"), "Value for {} set to {} (from {})", name, value, old_value);
}

template <typename T>
std::optional<T> getJemallocValue(const char * name)
{
    T value;
    size_t value_size = sizeof(T);
    if (mallctl(name, &value, &value_size, nullptr, 0))
    {
        LOG_WARNING(getLogger("Jemalloc"), "mallctl for {} failed", name);
        return std::nullopt;
    }
    return value;
}

}

#endif
