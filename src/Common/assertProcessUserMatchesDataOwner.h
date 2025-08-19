#pragma once

#include <string>
#include <functional>

namespace DB
{

void assertProcessUserMatchesDataOwner(
    const std::string & path, std::function<void(const std::string &)> on_warning);

}
