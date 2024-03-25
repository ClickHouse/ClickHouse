#pragma once

#include <base/types.h>

namespace DB
{

bool tryQuoteUnrecognizedTokens(String & query);

}
