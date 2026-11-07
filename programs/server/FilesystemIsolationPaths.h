#pragma once

#include <Common/FilesystemIsolation.h>

#include <string>
#include <vector>

namespace Poco::Util
{
class AbstractConfiguration;
}

namespace DB
{

/// The paths the server needs according to its configuration, and the system paths every server needs, to keep
/// visible when it isolates its filesystem. See the `filesystem_isolation` server setting.
///
/// `config_path` is the path of the main configuration file, and `working_directory` is the directory against
/// which the server resolves the relative paths in the configuration.
std::vector<FilesystemIsolationPath> collectFilesystemIsolationPaths(
    const Poco::Util::AbstractConfiguration & config, const std::string & config_path, const std::string & working_directory);

}
