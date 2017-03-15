#include <DB/Common/config_keys_multi.h>

#include <Poco/Util/AbstractConfiguration.h>
#include <DB/Common/StringUtils.h>

namespace DB
{
std::vector<std::string> config_keys_multi(Poco::Util::AbstractConfiguration & config, const std::string & root, const std::string & name)
{
	std::vector<std::string> values;
	Poco::Util::AbstractConfiguration::Keys config_keys;
	config.keys(root, config_keys);
	for (const auto & key : config_keys)
	{
		if (!startsWith(key.data(), name) || !(key == name || endsWith(key.data(), "]")))
			continue;
		values.emplace_back(key);
	}
	return values;
}
}
