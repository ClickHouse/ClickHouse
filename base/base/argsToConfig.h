#pragma once

#include <Poco/Util/Application.h>

namespace Poco::Util
{
class LayeredConfiguration;
}

/// Import extra command line arguments to configuration. These are command line arguments after --.
void argsToConfig(const Poco::Util::Application::ArgVec & argv, Poco::Util::LayeredConfiguration & config, int priority);
