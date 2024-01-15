#pragma once

#include <base/types.h>

namespace DB
{

bool tryQuoteUnrecognizedTokens(const String & query, String & res);

}
