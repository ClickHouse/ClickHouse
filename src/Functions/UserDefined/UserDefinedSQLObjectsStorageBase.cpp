#include <Functions/UserDefined/UserDefinedSQLObjectsStorageBase.h>

#include <boost/container/flat_set.hpp>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/NormalizeSelectWithUnionQueryVisitor.h>
#include <DataTypes/UserDefinedTypeFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedWebAssembly.h>
#include <Parsers/ASTCreateSQLFunctionQuery.h>
#include <Parsers/ASTCreateWasmFunctionQuery.h>
#include <Common/quoteString.h>

#include <optional>

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
    extern const int TYPE_ALREADY_EXISTS;
    extern const int UNKNOWN_TYPE;
}

std::string_view getUserDefinedSQLObjectTypeName(UserDefinedSQLObjectType object_type)
{
    switch (object_type)
    {
        case UserDefinedSQLObjectType::Function:
            return "function";
        case UserDefinedSQLObjectType::Type:
            return "type";
    }
}

int getUserDefinedSQLObjectAlreadyExistsErrorCode(UserDefinedSQLObjectType object_type)
{
    switch (object_type)
    {
        case UserDefinedSQLObjectType::Function:
            return ErrorCodes::FUNCTION_ALREADY_EXISTS;
        case UserDefinedSQLObjectType::Type:
            return ErrorCodes::TYPE_ALREADY_EXISTS;
    }
}

int getUnknownUserDefinedSQLObjectErrorCode(UserDefinedSQLObjectType object_type)
{
    switch (object_type)
    {
        case UserDefinedSQLObjectType::Function:
            return ErrorCodes::UNKNOWN_FUNCTION;
        case UserDefinedSQLObjectType::Type:
            return ErrorCodes::UNKNOWN_TYPE;
    }
}

ASTPtr normalizeCreateUserDefinedSQLObjectQuery(const IAST & create_query, UserDefinedSQLObjectType object_type, const ContextPtr & context)
{
    switch (object_type)
    {
        case UserDefinedSQLObjectType::Function:
            return normalizeCreateFunctionQuery(create_query, context);
        case UserDefinedSQLObjectType::Type:
            return normalizeCreateTypeQuery(create_query);
    }
}

namespace
{
    std::optional<UserDefinedWebAssemblyFunctionFactory::RegisteredFunction> prepareWasmFunction(
        const String & function_name,
        ASTPtr create_query,
        const ContextPtr & context)
    {
        if (!create_query->as<ASTCreateWasmFunctionQuery>())
            return std::nullopt;

        /// Startup can load persisted `CREATE FUNCTION ... LANGUAGE WASM` definitions before
        /// `WasmModuleManager` is initialized.
        /// Keep the `AST` in storage and let `loadFunctions` synchronize the runtime registry later.
        if (!context->hasWasmModuleManager())
            return std::nullopt;

        try
        {
            return UserDefinedWebAssemblyFunctionFactory::instance().prepareFunction(std::move(create_query), context->getWasmModuleManager());
        }
        catch (Exception & exception)
        {
            exception.addMessage(fmt::format("while loading user defined function {}", backQuote(function_name)));
            throw;
        }
    }
}

UserDefinedSQLObjectsStorageBase::UserDefinedSQLObjectsStorageBase(ContextPtr global_context_, UserDefinedSQLObjectType object_type_)
    : WithContext(global_context_)
    , storage_object_type(object_type_)
{}

ASTPtr UserDefinedSQLObjectsStorageBase::get(const String & object_name) const
{
    std::lock_guard lock(mutex);

    auto it = object_name_to_create_object_map.find(object_name);
    if (it == object_name_to_create_object_map.end())
        throw Exception(getUnknownUserDefinedSQLObjectErrorCode(storage_object_type),
            "The user-defined {} '{}' is not saved",
            getUserDefinedSQLObjectTypeName(storage_object_type), object_name);

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

VectorWithMemoryTracking<String> UserDefinedSQLObjectsStorageBase::getAllObjectNames() const
{
    VectorWithMemoryTracking<String> object_names;

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
            throw Exception(getUserDefinedSQLObjectAlreadyExistsErrorCode(object_type),
                "User-defined {} '{}' already exists", getUserDefinedSQLObjectTypeName(object_type), object_name);
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
            throw Exception(getUnknownUserDefinedSQLObjectErrorCode(object_type),
                "User-defined {} '{}' doesn't exist", getUserDefinedSQLObjectTypeName(object_type), object_name);
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

void UserDefinedSQLObjectsStorageBase::setAllObjects(const VectorWithMemoryTracking<std::pair<String, ASTPtr>> & new_objects)
{
    UnorderedMapWithMemoryTracking<String, ASTPtr> normalized_functions;
    VectorWithMemoryTracking<UserDefinedWebAssemblyFunctionFactory::RegisteredFunction> wasm_functions;

    for (const auto & [function_name, create_query] : new_objects)
    {
        auto normalized_query = normalizeCreateUserDefinedSQLObjectQuery(*create_query, storage_object_type, getContext());
        if (auto wasm_function = prepareWasmFunction(function_name, normalized_query, getContext()))
            wasm_functions.push_back(std::move(*wasm_function));
        normalized_functions[function_name] = std::move(normalized_query);
    }

    {
        std::lock_guard lock(mutex);
        object_name_to_create_object_map = std::move(normalized_functions);
    }

    /// The WebAssembly function registry mirrors the function storage only.
    if (storage_object_type == UserDefinedSQLObjectType::Function)
        UserDefinedWebAssemblyFunctionFactory::instance().replaceAll(std::move(wasm_functions));
}

VectorWithMemoryTracking<std::pair<String, ASTPtr>> UserDefinedSQLObjectsStorageBase::getAllObjects() const
{
    std::lock_guard lock{mutex};
    VectorWithMemoryTracking<std::pair<String, ASTPtr>> all_objects;
    all_objects.reserve(object_name_to_create_object_map.size());
    std::copy(object_name_to_create_object_map.begin(), object_name_to_create_object_map.end(), std::back_inserter(all_objects));
    return all_objects;
}

void UserDefinedSQLObjectsStorageBase::setObject(const String & object_name, const IAST & create_object_query)
{
    auto normalized_query = normalizeCreateUserDefinedSQLObjectQuery(create_object_query, storage_object_type, getContext());
    auto wasm_function = prepareWasmFunction(object_name, normalized_query, getContext());

    {
        std::lock_guard lock(mutex);
        object_name_to_create_object_map[object_name] = std::move(normalized_query);
    }

    if (storage_object_type != UserDefinedSQLObjectType::Function)
        return;

    if (wasm_function)
        UserDefinedWebAssemblyFunctionFactory::instance().addOrReplace(std::move(*wasm_function));
    else
        UserDefinedWebAssemblyFunctionFactory::instance().dropIfExists(object_name);
}

void UserDefinedSQLObjectsStorageBase::removeObject(const String & object_name)
{
    {
        std::lock_guard lock(mutex);
        object_name_to_create_object_map.erase(object_name);
    }

    if (storage_object_type == UserDefinedSQLObjectType::Function)
        UserDefinedWebAssemblyFunctionFactory::instance().dropIfExists(object_name);
}

void UserDefinedSQLObjectsStorageBase::removeAllObjectsExcept(const Strings & object_names_to_keep)
{
    boost::container::flat_set<std::string_view> names_set_to_keep{object_names_to_keep.begin(), object_names_to_keep.end()};

    {
        std::lock_guard lock(mutex);
        for (auto it = object_name_to_create_object_map.begin(); it != object_name_to_create_object_map.end();)
        {
            auto current = it++;
            if (!names_set_to_keep.contains(current->first))
                object_name_to_create_object_map.erase(current);
        }
    }

    if (storage_object_type != UserDefinedSQLObjectType::Function)
        return;

    for (const auto & registered_function : UserDefinedWebAssemblyFunctionFactory::instance().getAllFunctions())
    {
        if (!names_set_to_keep.contains(registered_function.sql_name))
            UserDefinedWebAssemblyFunctionFactory::instance().dropIfExists(registered_function.sql_name);
    }
}

}
