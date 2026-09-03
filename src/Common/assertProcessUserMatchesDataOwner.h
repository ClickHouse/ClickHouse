#pragma once

#include <functional>
#include <Common/LoggingFormatStringHelpers.h>

namespace DB
{

void assertProcessUserMatchesDataOwner(
    const std::string & path, std::function<void(const PreformattedMessage &)> on_warning);

}
