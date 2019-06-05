#pragma once
#include <Poco/Util/Application.h>

namespace Poco::Util
{
class LayeredConfiguration;
}

void argsToConfig(const Poco::Util::Application::ArgVec & argv, Poco::Util::LayeredConfiguration & config, int priority);
