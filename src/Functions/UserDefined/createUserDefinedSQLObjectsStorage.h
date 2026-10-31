#pragma once

#include <Interpreters/Context_fwd.h>

#include <cstdint>
#include <memory>


namespace DB
{

class IUserDefinedSQLObjectsStorage;
enum class UserDefinedSQLObjectType : uint8_t;

/// Creates the storage for user-defined SQL objects of one kind: on disk (`user_defined_path`) or in
/// ZooKeeper (`user_defined_zookeeper_path`). Objects of all kinds share the configured location.
std::unique_ptr<IUserDefinedSQLObjectsStorage> createUserDefinedSQLObjectsStorage(const ContextMutablePtr & global_context, UserDefinedSQLObjectType object_type);

}
