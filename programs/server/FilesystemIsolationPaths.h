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
/// `config_path` is the path of the main configuration file, `working_directory` is the directory against which the
/// server resolves the relative paths in the configuration, and `startup_directory` is the current directory of the
/// process, against which `config_path` and the logs are resolved. They differ with `--daemon`, which changes to the
/// directory of the executable. With `with_supervisor`, the supervisor outside of the isolation owns the pid file and
/// sends the notifications to systemd, so neither is visible.
std::vector<FilesystemIsolationPath> collectFilesystemIsolationPaths(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_path,
    const std::string & working_directory,
    const std::string & startup_directory,
    bool with_supervisor);

/// The absolute path of the pid file from the configuration, or empty if there is none. With the isolation, the
/// supervisor outside of it creates the pid file, rather than the server.
std::string getFilesystemIsolationPidFile(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_path,
    const std::string & working_directory,
    const std::string & startup_directory);

}
