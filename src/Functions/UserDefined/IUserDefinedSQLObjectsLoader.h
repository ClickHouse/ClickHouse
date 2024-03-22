#pragma once

#include <base/types.h>


namespace DB
{
class IAST;
struct Settings;
enum class UserDefinedSQLObjectType;

/// Interface for a loader of user-defined SQL objects.
/// Implementations: UserDefinedSQLLoaderFromDisk, UserDefinedSQLLoaderFromZooKeeper
class IUserDefinedSQLObjectsLoader
{
public:
    virtual ~IUserDefinedSQLObjectsLoader() = default;

    /// Whether this loader can replicate SQL objects to another node.
    virtual bool isReplicated() const { return false; }
    virtual String getReplicationID() const { return ""; }

    /// Loads all objects. Can be called once - if objects are already loaded the function does nothing.
    virtual void loadObjects() = 0;

    /// Stops watching.
    virtual void stopWatching() {}

    /// Immediately reloads all objects, throws an exception if failed.
    virtual void reloadObjects() = 0;

    /// Immediately reloads a specified object only.
    virtual void reloadObject(UserDefinedSQLObjectType object_type, const String & object_name) = 0;

    /// Stores an object (must be called only by UserDefinedSQLFunctionFactory::registerFunction).
    virtual bool storeObject(
        UserDefinedSQLObjectType object_type,
        const String & object_name,
        const IAST & create_object_query,
        bool throw_if_exists,
        bool replace_if_exists,
        const Settings & settings) = 0;

    /// Removes an object (must be called only by UserDefinedSQLFunctionFactory::unregisterFunction).
    virtual bool removeObject(UserDefinedSQLObjectType object_type, const String & object_name, bool throw_if_not_exists) = 0;
};
}
