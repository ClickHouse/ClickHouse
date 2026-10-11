#pragma once

#include <mutex>

#include <Functions/UserDefined/IUserDefinedSQLObjectsStorage.h>
#include <Functions/UserDefined/UserDefinedSQLObjectType.h>
#include <Interpreters/Context_fwd.h>
#include <Core/Types.h>
#include <Common/UnorderedMapWithMemoryTracking.h>

#include <Parsers/IAST_fwd.h>

namespace DB
{

/// The lower-case name of the kind of object a storage holds, for messages: "function", "type".
std::string_view getUserDefinedSQLObjectTypeName(UserDefinedSQLObjectType object_type);

/// The error codes to use when an object of the given kind already exists / does not exist.
int getUserDefinedSQLObjectAlreadyExistsErrorCode(UserDefinedSQLObjectType object_type);
int getUnknownUserDefinedSQLObjectErrorCode(UserDefinedSQLObjectType object_type);

/// Strips the `IF NOT EXISTS` / `OR REPLACE` flags (and normalizes the body) of a `CREATE` query
/// before it is stored, so that the stored definition does not depend on how it was created.
ASTPtr normalizeCreateUserDefinedSQLObjectQuery(const IAST & create_query, UserDefinedSQLObjectType object_type, const ContextPtr & context);

/// A storage holds objects of a single kind (see `UserDefinedSQLObjectType`): objects of different kinds
/// live in different namespaces, so a function and a type may share a name.
class UserDefinedSQLObjectsStorageBase : public IUserDefinedSQLObjectsStorage, private WithContext
{
public:
    UserDefinedSQLObjectsStorageBase(ContextPtr global_context_, UserDefinedSQLObjectType object_type_);
    ASTPtr get(const String & object_name) const override;

    ASTPtr tryGet(const String & object_name) const override;

    bool has(const String & object_name) const override;

    VectorWithMemoryTracking<String> getAllObjectNames() const override;

    VectorWithMemoryTracking<std::pair<String, ASTPtr>> getAllObjects() const override;

    bool empty() const override;

    bool storeObject(
        const ContextPtr & current_context,
        UserDefinedSQLObjectType object_type,
        const String & object_name,
        ASTPtr create_object_query,
        bool throw_if_exists,
        bool replace_if_exists,
        const Settings & settings) override;

    bool removeObject(
        const ContextPtr & current_context,
        UserDefinedSQLObjectType object_type,
        const String & object_name,
        bool throw_if_not_exists) override;

protected:
    virtual bool storeObjectImpl(
        const ContextPtr & current_context,
        UserDefinedSQLObjectType object_type,
        const String & object_name,
        ASTPtr create_object_query,
        bool throw_if_exists,
        bool replace_if_exists,
        const Settings & settings) = 0;

    virtual bool removeObjectImpl(
        const ContextPtr & current_context,
        UserDefinedSQLObjectType object_type,
        const String & object_name,
        bool throw_if_not_exists) = 0;

    using WithContext::getContext;

    std::unique_lock<std::recursive_mutex> getLock() const;
    void setAllObjects(const VectorWithMemoryTracking<std::pair<String, ASTPtr>> & new_objects);
    void setObject(const String & object_name, const IAST & create_object_query);
    void removeObject(const String & object_name);
    void removeAllObjectsExcept(const Strings & object_names_to_keep);

    const UserDefinedSQLObjectType storage_object_type;

    UnorderedMapWithMemoryTracking<String, ASTPtr> object_name_to_create_object_map;
    mutable std::recursive_mutex mutex;
};

}
