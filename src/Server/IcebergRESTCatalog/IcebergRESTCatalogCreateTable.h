#pragma once

#include <base/types.h>

#include <Poco/JSON/Object.h>

#include <map>

namespace DB
{

/// Builds the `metadata.json` of a new table from a CreateTableRequest. Throws `BAD_ARGUMENTS` for an invalid request.
/// `partition_spec` and `write_order` may be nullptr.
Poco::JSON::Object::Ptr buildInitialTableMetadata(
    const String & uuid,
    const String & location,
    Poco::JSON::Object::Ptr schema,
    Poco::JSON::Object::Ptr partition_spec,
    Poco::JSON::Object::Ptr write_order,
    std::map<String, String> properties);

}
