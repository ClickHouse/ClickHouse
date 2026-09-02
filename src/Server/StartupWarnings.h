#pragma once

#include <Interpreters/Context_fwd.h>

#include <string>

namespace Poco
{
class Logger;
}

namespace DB
{

/// Warnings about how the binary was built: debug mode, sanitizers, coverage, thread fuzzer.
void addBuildWarnings(ContextPtr context);

/// Reports a partially created dedicated jemalloc arena pool for MergeTree metadata; call after the pool is initialized.
void addMergeTreeArenaPoolWarnings(ContextPtr context);

/// Warnings about the runtime environment: kernel tunables, disks, memory, dangerous settings.
/// Empty `data_path` or `logs_path` skips the corresponding disk checks.
void addEnvironmentWarnings(ContextPtr context, const Poco::Logger & logger, const std::string & data_path, const std::string & logs_path);

}
