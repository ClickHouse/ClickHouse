#pragma once

#include <base/types.h>

#include <Poco/JSON/Object.h>

namespace DB
{

/// `indent = 0` gives a single line.
String toJSONString(const Poco::JSON::Object & json, unsigned indent = 0);

/// Throws `INCORRECT_DATA` if `data` is not a JSON object. `what` names the data for the message, e.g. "Table pointer at /path".
Poco::JSON::Object::Ptr parseJSONObject(const String & data, const String & what);

}
