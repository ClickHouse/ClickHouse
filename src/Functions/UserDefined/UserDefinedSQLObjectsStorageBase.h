#pragma once

#include <unordered_map>
#include <mutex>

#include <Functions/UserDefined/IUserDefinedSQLObjectsStorage.h>
#include <Interpreters/Context_fwd.h>

#include <Parsers/IAST_fwd.h>

namespace DB
{
using Strings = std::vector<String>;

class UserDefinedSQLObjectsStorageBase : public IUserDefinedSQLObjectsStorage
{
public:
    explicit UserDefinedSQLObjectsStorageBase(ContextPtr global_context_);
    ASTPtr get(const String & object_name) const override;

    ASTPtr tryGet(const String & object_name) const override;

    bool has(const String & object_name) const override;

    std::vector<String> getAllObjectNames() const override;

    std::vector<std::pair<String, ASTPtr>> getAllObjects() const override;

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

    std::unique_lock<std::recursive_mutex> getLock() const;
    void setAllObjects(const std::vector<std::pair<String, ASTPtr>> & new_objects);
    void setObject(const String & object_name, const IAST & create_object_query);
    void removeObject(const String & object_name);
    void removeAllObjectsExcept(const Strings & object_names_to_keep);

    std::unordered_map<String, ASTPtr> object_name_to_create_object_map;
    mutable std::recursive_mutex mutex;

    ContextPtr global_context;
};

}
