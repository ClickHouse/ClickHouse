#pragma once
#include <string>

namespace DB
{

void assertProcessUserMatchesDataOwner(
    const std::string & path, std::function<void(const std::string &)> on_warning);

}
