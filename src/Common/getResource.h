#pragma once

#include <string_view>

/// Get resource from binary if exists. Otherwise return empty string view.
/// Resources are data that is embedded into executable at link time.
std::string_view getResource(std::string_view name);
