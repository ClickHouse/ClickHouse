#pragma once

#include <optional>
#include <string>

namespace DB
{

/// Appends every supported configuration file extension (`.xml`, `.yaml`, `.yml`) to
/// `path_without_extension` and returns the first path that exists. E.g. for
/// `/etc/clickhouse-server/config` it probes `config.xml`, `config.yaml` and `config.yml`.
std::optional<std::string> tryGetConfigPath(const std::string & path_without_extension);

/// Same as `tryGetConfigPath`, but takes a path with an extension, which is replaced by every supported
/// one. If none of the candidates exists, returns `path` unchanged, so that the caller reports the name
/// it asked for (and the fallback to the configuration embedded into the binary keeps working).
std::string getConfigPathForAnySupportedFormat(const std::string & path);

}
