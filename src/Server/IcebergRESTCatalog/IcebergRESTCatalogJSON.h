#pragma once

#include <base/types.h>

#include <Poco/Dynamic/Var.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>

#include <optional>

namespace DB
{

namespace IcebergRESTCatalog
{

/// `indent = 0` gives a single line.
String toJSONString(const Poco::JSON::Object & json, unsigned indent = 0);

/// Throws `INCORRECT_DATA` if `data` is not a JSON object. `what` names the data for the message, e.g. "Table pointer at /path".
Poco::JSON::Object::Ptr parseJSONObject(const String & data, const String & what);

/// Current time in the unit of Iceberg metadata timestamps.
Int64 nowMs();

/// Helpers to validate client requests. They throw `BAD_ARGUMENTS` for a missing or wrong-typed key.

Int64 getInteger(const Poco::JSON::Object & object, const String & key);
/// Returns nullopt for an absent or null key.
std::optional<Int64> getOptionalInteger(const Poco::JSON::Object & object, const String & key);
/// Unlike `validateString`, allows an empty string.
String getString(const Poco::JSON::Object & object, const String & key);
Poco::JSON::Object::Ptr getObject(const Poco::JSON::Object & object, const String & key);
/// Returns nullptr for an absent or null key. Poco's `getObject` alone would also return nullptr for a wrong type.
Poco::JSON::Object::Ptr getOptionalObject(const Poco::JSON::Object & json, const String & key);
Poco::JSON::Array::Ptr getArray(const Poco::JSON::Object & object, const String & key);
Poco::Dynamic::Var getValue(const Poco::JSON::Object & object, const String & key);

void validateBoolean(const Poco::JSON::Object & object, const String & key);
void validateInteger(const Poco::JSON::Object & object, const String & key);
void validateOptionalInteger(const Poco::JSON::Object & object, const String & key);
/// Also rejects an empty string.
void validateString(const Poco::JSON::Object & object, const String & key);

/// Returns an array with one element.
Poco::JSON::Array::Ptr makeArray(const Poco::JSON::Object::Ptr & element);

}

}
