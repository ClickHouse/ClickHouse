#pragma once
#include <string>

namespace Poco { class Logger; namespace Util { class LayeredConfiguration; } }

namespace DB
{
/// Read configuration files related to clickhouse-client like applications. Returns true if any configuration files were read.
bool configReadClient(Poco::Util::LayeredConfiguration & config, const std::string & home_path);
}
