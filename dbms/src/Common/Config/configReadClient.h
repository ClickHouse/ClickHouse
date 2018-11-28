#pragma once
#include <string>

namespace Poco { class Logger; namespace Util { class LayeredConfiguration; } }

namespace DB
{
bool configReadClient(Poco::Util::LayeredConfiguration & config, const std::string & home_path);
}
