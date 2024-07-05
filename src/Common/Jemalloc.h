#pragma once

#include "config.h"

#if USE_JEMALLOC

#include <string>

namespace DB
{

void purgeJemallocArenas();

void checkJemallocProfilingEnabled();

void setJemallocProfileActive(bool value);

std::string flushJemallocProfile(const std::string & file_prefix);

void setJemallocBackgroundThreads(bool enabled);

void setJemallocMaxBackgroundThreads(size_t max_threads);

}

#endif
