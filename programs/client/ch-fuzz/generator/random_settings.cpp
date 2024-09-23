#include "random_settings.h"

namespace chfuzz {

void SetRandomSetting(RandomGenerator &rg, const std::map<std::string, std::function<void(RandomGenerator&,std::string&)>> &settings,
					  std::string &ret, sql_query_grammar::SetValue *set) {
	std::string first;
	std::function<void(RandomGenerator&,std::string&)> second;
	std::tie(first, second) = rg.PickPairRandomlyFromMap(settings);

	set->set_property(first);
	ret.resize(0);
	second(rg, ret);
	set->set_value(ret);
}

}
