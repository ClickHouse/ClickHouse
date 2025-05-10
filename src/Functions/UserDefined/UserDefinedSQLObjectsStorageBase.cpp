#include "Functions/UserDefined/UserDefinedSQLObjectsStorageBase.h"

#include <boost/container/flat_set.hpp>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/NormalizeSelectWithUnionQueryVisitor.h>
#include <Parsers/ASTCreateDriverFunctionQuery.h>
#include <Parsers/ASTCreateFunctionQuery.h>

namespace DB
{
namespace Setting
{
    extern const SettingsSetOperationMode union_default_mode;
}

namespace ErrorCodes
{
    extern const int FUNCTION_ALREADY_EXISTS;
    extern const int UNKNOWN_FUNCTION;
}

namespace
{

ASTPtr normalizeCreateFunctionQuery(const IAST & create_function_query, const ContextPtr & context)
{
    auto ptr = create_function_query.clone();
    auto & res = typeid_cast<ASTCreateFunctionQuery &>(*ptr);
    res.if_not_exists = false;
    res.or_replace = false;
    FunctionNameNormalizer::visit(res.function_core.get());
    NormalizeSelectWithUnionQueryVisitor::Data data{context->getSettingsRef()[Setting::union_default_mode]};
    NormalizeSelectWithUnionQueryVisitor{data}.visit(res.function_core);
    return ptr;
}

ASTPtr normalizeCreateDriverFunctionQuery(const IAST & create_function_query)
{
    auto ptr = create_function_query.clone();
    auto & res = typeid_cast<ASTCreateDriverFunctionQuery &>(*ptr);
    res.if_not_exists = false;
    res.or_replace = false;
    return ptr;
}

}

UserDefinedSQLObjectsStorageBase::UserDefinedSQLObjectsStorageBase(ContextPtr global_context_)
    : global_context(std::move(global_context_))
{}

ASTPtr UserDefinedSQLObjectsStorageBase::get(const String & object_name, UserDefinedSQLObjectType object_type) const
{
    std::lock_guard lock(mutex);

    auto it = object_name_to_create_object_map.find(object_name);
    if (it == object_name_to_create_object_map.end() || it->second.object_type != object_type)
        throw Exception(ErrorCodes::UNKNOWN_FUNCTION,
            "The object name '{}' is not saved",
            object_name);

    return it->second.object;
}

ASTPtr UserDefinedSQLObjectsStorageBase::tryGet(const std::string & object_name, UserDefinedSQLObjectType object_type) const
{
    std::lock_guard lock(mutex);

    auto it = object_name_to_create_object_map.find(object_name);
    if (it == object_name_to_create_object_map.end() || it->second.object_type != object_type)
        return nullptr;

    return it->second.object;
}

bool UserDefinedSQLObjectsStorageBase::has(const String & object_name, UserDefinedSQLObjectType object_type) const
{
    return tryGet(object_name, object_type) != nullptr;
}

std::vector<std::string> UserDefinedSQLObjectsStorageBase::getAllObjectNames(UserDefinedSQLObjectType object_type) const
{
    std::vector<std::string> object_names;

    std::lock_guard lock(mutex);
    object_names.reserve(object_name_to_create_object_map.size());

    for (const auto & [name, typed_query] : object_name_to_create_object_map)
    {
        if (typed_query.object_type == object_type)
        {
            object_names.emplace_back(name);
        }
    }

    return object_names;
}

bool UserDefinedSQLObjectsStorageBase::empty(UserDefinedSQLObjectType object_type) const
{
    std::lock_guard lock(mutex);
    for (const auto & [_, typed_object] : object_name_to_create_object_map)
    {
        if (typed_object.object_type == object_type)
        {
            return false;
        }
    }

    return true;
}

bool UserDefinedSQLObjectsStorageBase::storeObject(
    const ContextPtr & current_context,
    UserDefinedSQLObjectType object_type,
    const String & object_name,
    ASTPtr create_object_query,
    bool throw_if_exists,
    bool replace_if_exists,
    const Settings & settings)
{
    std::lock_guard lock{mutex};
    auto it = object_name_to_create_object_map.find(object_name);
    if (it != object_name_to_create_object_map.end())
    {
        if (throw_if_exists)
            throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "User-defined object '{}' already exists", object_name);
        if (!replace_if_exists)
            return false;
    }

    bool stored = storeObjectImpl(
        current_context,
        object_type,
        object_name,
        create_object_query,
        throw_if_exists,
        replace_if_exists,
        settings);

    if (stored)
        object_name_to_create_object_map[object_name] = {create_object_query, object_type};

    return stored;
}

bool UserDefinedSQLObjectsStorageBase::removeObject(
        const ContextPtr & current_context,
        UserDefinedSQLObjectType object_type,
        const String & object_name,
        bool throw_if_not_exists)
{
    std::lock_guard lock(mutex);
    auto it = object_name_to_create_object_map.find(object_name);
    if (it == object_name_to_create_object_map.end())
    {
        if (throw_if_not_exists)
            throw Exception(ErrorCodes::UNKNOWN_FUNCTION, "User-defined object '{}' doesn't exist", object_name);
        return false;
    }

    bool removed = removeObjectImpl(
        current_context,
        object_type,
        object_name,
        throw_if_not_exists);

    if (removed)
        object_name_to_create_object_map.erase(object_name);

    return removed;
}

std::unique_lock<std::recursive_mutex> UserDefinedSQLObjectsStorageBase::getLock() const
{
    return std::unique_lock{mutex};
}

void UserDefinedSQLObjectsStorageBase::setAllObjects(const std::vector<std::pair<String, UserDefinedSQLTypedObject>> & new_objects)
{
    std::lock_guard lock(mutex);

    for (const auto & [function_name, object] : new_objects)
    {
        UserDefinedSQLTypedObject typed_object;
        const auto & [create_query, type] = object;

        switch (type)
        {
            case UserDefinedSQLObjectType::SQLFunction:
            {
                typed_object = {normalizeCreateFunctionQuery(*create_query, global_context), type};
                break;
            }
            case UserDefinedSQLObjectType::DriverFunction:
            {
                typed_object = {normalizeCreateDriverFunctionQuery(*create_query), type};
                break;
            }
        }

        object_name_to_create_object_map[function_name] = std::move(typed_object);
    }
}

std::vector<std::pair<String, ASTPtr>> UserDefinedSQLObjectsStorageBase::getAllObjects(UserDefinedSQLObjectType object_type) const
{
    std::lock_guard lock{mutex};
    std::vector<std::pair<String, ASTPtr>> all_objects;
    all_objects.reserve(object_name_to_create_object_map.size());

    for (const auto & [name, typed_query] : object_name_to_create_object_map)
    {
        if (typed_query.object_type == object_type)
        {
            all_objects.emplace_back(name, typed_query.object);
        }
    }

    return all_objects;
}

void UserDefinedSQLObjectsStorageBase::setObject(const String & object_name, const IAST & create_object_query, UserDefinedSQLObjectType object_type)
{
    std::lock_guard lock(mutex);

    switch (object_type)
    {
        case UserDefinedSQLObjectType::SQLFunction:
        {
            object_name_to_create_object_map[object_name] = {normalizeCreateFunctionQuery(create_object_query, global_context), object_type};
            break;
        }
        case UserDefinedSQLObjectType::DriverFunction:
        {
            object_name_to_create_object_map[object_name] = {normalizeCreateDriverFunctionQuery(create_object_query), object_type};
            break;
        }
    }
}

void UserDefinedSQLObjectsStorageBase::removeObject(const String & object_name)
{
    std::lock_guard lock(mutex);
    object_name_to_create_object_map.erase(object_name);
}

void UserDefinedSQLObjectsStorageBase::removeAllObjectsExcept(const Strings & object_names_to_keep)
{
    boost::container::flat_set<std::string_view> names_set_to_keep{object_names_to_keep.begin(), object_names_to_keep.end()};
    std::lock_guard lock(mutex);
    for (auto it = object_name_to_create_object_map.begin(); it != object_name_to_create_object_map.end();)
    {
        auto current = it++;
        if (!names_set_to_keep.contains(current->first))
            object_name_to_create_object_map.erase(current);
    }
}

}
