#pragma once

#include <string>
#include <optional>

namespace DB
{

/// Return path to existing configuration file.
std::optional<std::string> getLocalConfigPath(const std::string & home_path);

}
