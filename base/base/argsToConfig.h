#pragma once

#include <Poco/Util/Application.h>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

namespace Poco::Util
{
class LayeredConfiguration; // NOLINT(cppcoreguidelines-virtual-class-destructor)
}

/// Import extra command line arguments to configuration. These are command line arguments after --.
/// If `ordered_args` is not null, every stored (key, value) pair is also appended to it in command-line
/// order, so the caller can tell which of several keys occurred last (the configuration itself only
/// keeps the last value per key and loses the relative order of different keys).
void argsToConfig(const Poco::Util::Application::ArgVec & argv,
                  Poco::Util::LayeredConfiguration & config,
                  int priority,
                  const std::unordered_set<std::string>* registered_alias_names = nullptr,
                  std::vector<std::pair<std::string, std::string>> * ordered_args = nullptr);
