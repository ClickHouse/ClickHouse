#pragma once

#include <base/types.h>
#include <DataTypes/IDataType_fwd.h>

#include <unordered_map>

namespace DB
{

/// Column types that readers made from their type names, after the state versions of aggregate functions were set.
/// A reader that gets this map takes a type from it and does not parse the name again.
/// A type depends on the server revision and on the context where it was made: the session time zone for a
/// `DateTime` without an explicit zone, and the settings that aggregate functions read. So use one map only with
/// one server revision, inside one query, and do not share it between queries.
using NativeReaderTypeCache = std::unordered_map<String, DataTypePtr>;

}
