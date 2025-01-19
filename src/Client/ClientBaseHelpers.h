#pragma once

#include <Core/Types.h>
#include "config.h"

#if USE_REPLXX
#   include <Client/ReplxxLineReader.h>
#endif


namespace DB
{

class Context;

/// Should we celebrate a bit?
bool isNewYearMode();

bool isChineseNewYearMode(const String & local_tz);

#if USE_REPLXX
void highlight(const String & query, std::vector<replxx::Replxx::Color> & colors, const Context & context);
#endif

}
