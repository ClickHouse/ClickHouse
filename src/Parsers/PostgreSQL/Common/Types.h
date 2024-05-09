#pragma once
#include <Common/JSONParsers/RapidJSONParser.h>

#if USE_RAPIDJSON
using JSON = DB::RapidJSONParser;
#endif
