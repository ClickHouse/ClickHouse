#pragma once

#include <base/types.h>

#include <Interpreters/Context_fwd.h>

#include <Parsers/IAST_fwd.h>


namespace DB
{
class IAST;
struct Settings;
enum class UserDefinedSQLObjectType : uint8_t;

/// Interface for a storage of user-defined SQL objects.
/// Implementations: UserDefinedSQLObjectsDiskStorage, UserDefinedSQLObjectsZooKeeperStorage
class IUserDefinedSQLObjectsStorage
{
public:
    virtual ~IUserDefinedSQLObjectsStorage() = default;

    /// Whether this loader can replicate SQL objects to another node.
    virtual bool isReplicated() const { return false; }
    virtual String getReplicationID() const { return ""; }

    /// Loads all objects. Can be called once - if objects are already loaded the function does nothing.
    virtual void loadObjects() = 0;

    /// Get object by name. If no object stored with object_name throws exception.
    virtual ASTPtr get(const String & object_name) const = 0;

    /// Get object by name. If no object stored with object_name return nullptr.
    virtual ASTPtr tryGet(const String & object_name) const = 0;

    /// Check if object with object_name is stored.
    virtual bool has(const String & object_name) const = 0;

    /// Get all user defined object names.
    virtual std::vector<String> getAllObjectNames() const = 0;

    /// Get all user defined objects.
    virtual std::vector<std::pair<String, ASTPtr>> getAllObjects() const = 0;

    /// Check whether any UDFs have been stored.
    virtual bool empty() const = 0;

    /// Stops watching.
    virtual void stopWatching() {}

    /// Immediately reloads all objects, throws an exception if failed.
    virtual void reloadObjects() = 0;

    /// Immediately reloads a specified object only.
    virtual void reloadObject(UserDefinedSQLObjectType object_type, const String & object_name) = 0;

    /// Stores an object (must be called only by UserDefinedSQLFunctionFactory::registerFunction).
    virtual bool storeObject(
        const ContextPtr & current_context,
        UserDefinedSQLObjectType object_type,
        const String & object_name,
        ASTPtr create_object_query,
        bool throw_if_exists,
        bool replace_if_exists,
        const Settings & settings) = 0;

    /// Removes an object (must be called only by UserDefinedSQLFunctionFactory::unregisterFunction).
    virtual bool removeObject(
        const ContextPtr & current_context,
        UserDefinedSQLObjectType object_type,
        const String & object_name,
        bool throw_if_not_exists) = 0;
};
}
