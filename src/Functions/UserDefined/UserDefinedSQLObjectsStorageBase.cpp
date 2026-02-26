#include "Functions/UserDefined/UserDefinedSQLObjectsStorageBase.h"

#include <boost/container/flat_set.hpp>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/NormalizeSelectWithUnionQueryVisitor.h>
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

}

UserDefinedSQLObjectsStorageBase::UserDefinedSQLObjectsStorageBase(ContextPtr global_context_)
    : global_context(std::move(global_context_))
{}

ASTPtr UserDefinedSQLObjectsStorageBase::get(const String & object_name) const
{
    std::lock_guard lock(mutex);

    auto it = object_name_to_create_object_map.find(object_name);
    if (it == object_name_to_create_object_map.end())
        throw Exception(ErrorCodes::UNKNOWN_FUNCTION,
            "The object name '{}' is not saved",
            object_name);

    return it->second;
}

ASTPtr UserDefinedSQLObjectsStorageBase::tryGet(const std::string & object_name) const
{
    std::lock_guard lock(mutex);

    auto it = object_name_to_create_object_map.find(object_name);
    if (it == object_name_to_create_object_map.end())
        return nullptr;

    return it->second;
}

bool UserDefinedSQLObjectsStorageBase::has(const String & object_name) const
{
    return tryGet(object_name) != nullptr;
}

std::vector<std::string> UserDefinedSQLObjectsStorageBase::getAllObjectNames() const
{
    std::vector<std::string> object_names;

    std::lock_guard lock(mutex);
    object_names.reserve(object_name_to_create_object_map.size());

    for (const auto & [name, _] : object_name_to_create_object_map)
        object_names.emplace_back(name);

    return object_names;
}

bool UserDefinedSQLObjectsStorageBase::empty() const
{
    std::lock_guard lock(mutex);
    return object_name_to_create_object_map.empty();
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
        object_name_to_create_object_map[object_name] = create_object_query;

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

void UserDefinedSQLObjectsStorageBase::setAllObjects(const std::vector<std::pair<String, ASTPtr>> & new_objects)
{
    std::unordered_map<String, ASTPtr> normalized_functions;
    for (const auto & [function_name, create_query] : new_objects)
        normalized_functions[function_name] = normalizeCreateFunctionQuery(*create_query, global_context);

    std::lock_guard lock(mutex);
    object_name_to_create_object_map = std::move(normalized_functions);
}

std::vector<std::pair<String, ASTPtr>> UserDefinedSQLObjectsStorageBase::getAllObjects() const
{
    std::lock_guard lock{mutex};
    std::vector<std::pair<String, ASTPtr>> all_objects;
    all_objects.reserve(object_name_to_create_object_map.size());
    std::copy(object_name_to_create_object_map.begin(), object_name_to_create_object_map.end(), std::back_inserter(all_objects));
    return all_objects;
}

void UserDefinedSQLObjectsStorageBase::setObject(const String & object_name, const IAST & create_object_query)
{
    std::lock_guard lock(mutex);
    object_name_to_create_object_map[object_name] = normalizeCreateFunctionQuery(create_object_query, global_context);
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
