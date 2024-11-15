#include "RandomSettings.h"

namespace BuzzHouse
{

void setRandomSetting(
    RandomGenerator & rg,
    const std::map<std::string, std::function<void(RandomGenerator &, std::string &)>> & settings,
    std::string & ret,
    SetValue * set)
{
    std::string first;
    std::function<void(RandomGenerator &, std::string &)> second;
    std::tie(first, second) = rg.pickPairRandomlyFromMap(settings);

    set->set_property(first);
    ret.resize(0);
    second(rg, ret);
    set->set_value(ret);
}

}
