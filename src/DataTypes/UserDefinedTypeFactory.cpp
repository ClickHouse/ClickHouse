#include <DataTypes/UserDefinedTypeFactory.h>

#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeFactory.h>
#include <Functions/UserDefined/IUserDefinedSQLObjectsStorage.h>
#include <Functions/UserDefined/UserDefinedSQLObjectType.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateTypeQuery.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Poco/String.h>

#include <algorithm>
#include <unordered_set>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNEXPECTED_AST_STRUCTURE;
    extern const int UNKNOWN_TYPE;
}

namespace
{

const ASTCreateTypeQuery & getCreateTypeQuery(const IAST & ast)
{
    const auto * create = ast.as<ASTCreateTypeQuery>();
    if (!create)
        throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "Expected a CREATE TYPE query, got: {}", ast.formatForErrorMessage());
    return *create;
}

size_t getNumberOfParameters(const ASTCreateTypeQuery & create)
{
    if (!create.type_parameters)
        return 0;
    return create.type_parameters->children.size();
}

/// The formal parameters of a definition, checked to be distinct identifiers.
std::unordered_set<String> getParameterNames(const ASTCreateTypeQuery & create)
{
    std::unordered_set<String> names;
    if (!create.type_parameters)
        return names;

    const auto * params_list = create.type_parameters->as<ASTExpressionList>();
    if (!params_list)
        throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE,
                        "Type parameters of user-defined type {} are not an expression list", backQuote(create.name));

    for (const auto & param_ast : params_list->children)
    {
        const auto * param_ident = param_ast->as<ASTIdentifier>();
        if (!param_ident)
            throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE,
                            "Type parameter of user-defined type {} is not an identifier", backQuote(create.name));

        /// `DataTypeFactory` substitutes the actual arguments through a map keyed by the parameter name, so a
        /// repeated name would silently take the value of its last occurrence:
        /// `CREATE TYPE Pair(T, T) AS Tuple(T, T)` would ignore the first argument.
        if (!names.insert(param_ident->name()).second)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Duplicate type parameter {} in the definition of user-defined type {}",
                            backQuote(param_ident->name()), backQuote(create.name));
    }
    return names;
}

/// A reference to a type by name inside a definition: `UInt64`, `Array(T)`, `MyType(String, 2)` ...
struct TypeReference
{
    String name;
    size_t num_arguments = 0;
    ASTPtr node;
    ASTPtr arguments;
};

/// Calls `callback` for every name that is used as a type inside `node`, in the order of occurrence.
/// If the callback returns false, the arguments of that type are not descended into.
template <typename Callback>
void forEachTypeReference(const ASTPtr & node, Callback && callback)
{
    if (!node)
        return;

    if (const auto * identifier = node->as<ASTIdentifier>())
    {
        callback(TypeReference{.name = identifier->name(), .num_arguments = 0, .node = node, .arguments = nullptr});
        return;
    }

    if (const auto * data_type = node->as<ASTDataType>())
    {
        ASTPtr arguments = data_type->getArguments();
        size_t num_arguments = arguments ? arguments->children.size() : 0;
        if (!callback(TypeReference{.name = data_type->name, .num_arguments = num_arguments, .node = node, .arguments = arguments}))
            return;

        if (arguments)
            for (const auto & argument : arguments->children)
                forEachTypeReference(argument, callback);
        return;
    }

    /// Functions (e.g. the `equals` of an `Enum8('a' = 1)` element) and other nodes: only descend.
    for (const auto & child : node->children)
        forEachTypeReference(child, callback);
}

/// Checks that every type the definition refers to exists and is used with a valid number of arguments,
/// and records the user-defined types it references.
void validateDefinition(
    const ASTCreateTypeQuery & create,
    const std::unordered_set<String> & parameter_names,
    const UserDefinedTypeFactory & udt_factory,
    std::unordered_set<String> & referenced_user_defined_types)
{
    const auto & data_type_factory = DataTypeFactory::instance();

    forEachTypeReference(create.base_type, [&](const TypeReference & ref) -> bool
    {
        /// A parameter is substituted with an actual argument when the type is used, so it can not be checked here.
        /// `Array(T)` is a family applied to a parameter, `T` alone is the parameter itself.
        if (ref.num_arguments == 0 && parameter_names.contains(ref.name))
            return false;

        if (auto referenced = udt_factory.tryGet(ref.name))
        {
            referenced_user_defined_types.insert(ref.name);

            size_t expected = getNumberOfParameters(getCreateTypeQuery(*referenced));
            if (expected != ref.num_arguments)
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                                "User-defined type {} expects {} argument(s), but {} provided in the definition of user-defined type {}",
                                backQuote(ref.name), expected, ref.num_arguments, backQuote(create.name));
            return true;
        }

        if (ref.num_arguments == 0)
        {
            /// A complete built-in type without arguments (`UInt64`, `String`, an alias like `INT`, ...).
            if (data_type_factory.tryGet(ref.node))
                return false;

            throw Exception(ErrorCodes::UNKNOWN_TYPE,
                            "Unknown type or type parameter {} in the definition of user-defined type {}",
                            backQuote(ref.name), backQuote(create.name));
        }

        /// A family with arguments. The arguments may contain parameters, so only the family itself can be checked
        /// here; a definition without parameters is additionally instantiated as a whole by the caller.
        /// `hasNameOrAlias` looks the alias up as written, so the lower-cased name is needed for the
        /// case-insensitive aliases (`INT`, `int`, ...).
        if (!data_type_factory.hasNameOrAlias(ref.name) && !data_type_factory.hasNameOrAlias(Poco::toLower(ref.name)))
            throw Exception(ErrorCodes::UNKNOWN_TYPE,
                            "Unknown type family {} in the definition of user-defined type {}",
                            backQuote(ref.name), backQuote(create.name));
        return true;
    });
}

/// The user-defined types a stored definition refers to (its own parameters excluded).
void collectReferencedUserDefinedTypes(const ASTCreateTypeQuery & create, const UserDefinedTypeFactory & udt_factory, std::unordered_set<String> & result)
{
    auto parameter_names = getParameterNames(create);
    forEachTypeReference(create.base_type, [&](const TypeReference & ref) -> bool
    {
        if (ref.num_arguments == 0 && parameter_names.contains(ref.name))
            return false;
        if (udt_factory.has(ref.name))
            result.insert(ref.name);
        return true;
    });
}

/// Throws if `create` (a new definition of `create.name`) refers, directly or through other user-defined
/// types, to `create.name` itself: `DataTypeFactory` would never finish expanding such a type.
void checkNoCycle(const ASTCreateTypeQuery & create, const std::unordered_set<String> & directly_referenced, const UserDefinedTypeFactory & udt_factory)
{
    std::unordered_set<String> visited;
    std::vector<String> to_visit(directly_referenced.begin(), directly_referenced.end());

    while (!to_visit.empty())
    {
        String current = std::move(to_visit.back());
        to_visit.pop_back();

        if (current == create.name)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Cannot create user-defined type {}: its definition refers to itself through other user-defined types",
                            backQuote(create.name));

        if (!visited.insert(current).second)
            continue;

        /// A type may disappear concurrently; then there is nothing to follow.
        auto referenced = udt_factory.tryGet(current);
        if (!referenced)
            continue;

        std::unordered_set<String> next;
        collectReferencedUserDefinedTypes(getCreateTypeQuery(*referenced), udt_factory, next);
        to_visit.insert(to_visit.end(), next.begin(), next.end());
    }
}

/// A use of the type `type_name` in the definition of another user-defined type.
struct DependentUse
{
    String dependent_type_name;
    size_t num_arguments = 0;
};

std::vector<DependentUse> findDependentUses(const IUserDefinedSQLObjectsStorage & storage, const String & type_name)
{
    std::vector<DependentUse> uses;
    for (const auto & [dependent_name, create_query] : storage.getAllObjects())
    {
        if (dependent_name == type_name)
            continue;

        const auto & dependent = getCreateTypeQuery(*create_query);
        auto parameter_names = getParameterNames(dependent);
        forEachTypeReference(dependent.base_type, [&](const TypeReference & ref) -> bool
        {
            if (ref.num_arguments == 0 && parameter_names.contains(ref.name))
                return false;
            if (ref.name == type_name)
                uses.push_back(DependentUse{.dependent_type_name = dependent_name, .num_arguments = ref.num_arguments});
            return true;
        });
    }
    std::sort(uses.begin(), uses.end(), [](const auto & lhs, const auto & rhs) { return lhs.dependent_type_name < rhs.dependent_type_name; });
    return uses;
}

}


ASTPtr normalizeCreateTypeQuery(const IAST & create_type_query)
{
    auto ptr = create_type_query.clone();
    auto & create = ptr->as<ASTCreateTypeQuery &>();
    create.if_not_exists = false;
    create.or_replace = false;
    return ptr;
}


UserDefinedTypeFactory & UserDefinedTypeFactory::instance()
{
    static UserDefinedTypeFactory result;
    return result;
}

const IUserDefinedSQLObjectsStorage * UserDefinedTypeFactory::tryGetStorage() const
{
    auto global_context = Context::getGlobalContextInstance();
    if (!global_context)
        return nullptr;
    return global_context->tryGetUserDefinedTypesStorage();
}

bool UserDefinedTypeFactory::registerType(
    const ContextMutablePtr & current_context,
    const String & type_name,
    const ASTPtr & create_type_query,
    bool throw_if_exists,
    bool replace_if_exists) const
{
    auto & storage = current_context->getUserDefinedTypesStorage();

    /// `IF NOT EXISTS` for an existing type is a no-op: the new definition is not even validated, like for
    /// `CREATE TABLE IF NOT EXISTS`. The storage re-checks the existence under its lock.
    if (!throw_if_exists && !replace_if_exists && storage.has(type_name))
        return false;

    const auto & create = getCreateTypeQuery(*create_type_query);
    if (create.name != type_name)
        throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "The CREATE TYPE query defines type {}, not {}", backQuote(create.name), backQuote(type_name));
    if (!create.base_type)
        throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "Base type not specified for user-defined type {}", backQuote(type_name));

    /// `DataTypeFactory` resolves user-defined types before the built-in ones, so a user-defined type named after
    /// a built-in type, alias or family would hijack every later use of that name: `CREATE TYPE UInt64 AS String`
    /// would change the meaning of `UInt64` everywhere.
    const auto & data_type_factory = DataTypeFactory::instance();
    if (data_type_factory.hasNameOrAlias(type_name) || data_type_factory.hasNameOrAlias(Poco::toLower(type_name)))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Cannot create user-defined type {}: a built-in data type with this name already exists",
                        backQuote(type_name));

    auto parameter_names = getParameterNames(create);

    std::unordered_set<String> referenced_user_defined_types;
    validateDefinition(create, parameter_names, *this, referenced_user_defined_types);

    /// `CREATE TYPE A AS A` is rejected above as unknown (`A` does not exist yet), but a replacement can close a
    /// cycle through other types: `CREATE TYPE B AS A; CREATE TYPE OR REPLACE A AS B`.
    checkNoCycle(create, referenced_user_defined_types, *this);

    /// A definition without parameters is a complete data type expression, so it can be checked exactly by
    /// instantiating it. This rejects definitions like `Map(String)` that name a known family with a wrong
    /// number of arguments. (Referenced user-defined types still expand to their current definitions here.)
    if (parameter_names.empty())
        data_type_factory.get(create.base_type);

    /// Other types keep using the replaced type with the number of arguments they were defined with.
    if (replace_if_exists)
    {
        size_t new_num_parameters = parameter_names.size();
        for (const auto & use : findDependentUses(storage, type_name))
        {
            if (use.num_arguments != new_num_parameters)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "Cannot replace user-defined type {} with a definition taking {} parameter(s): "
                                "user-defined type {} uses it with {} argument(s)",
                                backQuote(type_name), new_num_parameters, backQuote(use.dependent_type_name), use.num_arguments);
        }
    }

    try
    {
        return storage.storeObject(
            current_context,
            UserDefinedSQLObjectType::Type,
            type_name,
            normalizeCreateTypeQuery(create),
            throw_if_exists,
            replace_if_exists,
            current_context->getSettingsRef());
    }
    catch (Exception & exception)
    {
        exception.addMessage(fmt::format("while adding user-defined type {}", backQuote(type_name)));
        throw;
    }
}

bool UserDefinedTypeFactory::unregisterType(const ContextMutablePtr & current_context, const String & type_name, bool throw_if_not_exists) const
{
    auto & storage = current_context->getUserDefinedTypesStorage();

    /// Dropping a type another type is defined through would leave that type registered but unusable.
    auto dependent_uses = findDependentUses(storage, type_name);
    if (!dependent_uses.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Cannot drop user-defined type {}: it is used in the definition of user-defined type {}",
                        backQuote(type_name), backQuote(dependent_uses.front().dependent_type_name));

    try
    {
        return storage.removeObject(current_context, UserDefinedSQLObjectType::Type, type_name, throw_if_not_exists);
    }
    catch (Exception & exception)
    {
        exception.addMessage(fmt::format("while removing user-defined type {}", backQuote(type_name)));
        throw;
    }
}

ASTPtr UserDefinedTypeFactory::tryGet(const String & type_name) const
{
    const auto * storage = tryGetStorage();
    if (!storage)
        return nullptr;
    return storage->tryGet(type_name);
}

ASTPtr UserDefinedTypeFactory::get(const String & type_name) const
{
    auto ast = tryGet(type_name);
    if (!ast)
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "Unknown type {}", backQuote(type_name));
    return ast;
}

bool UserDefinedTypeFactory::has(const String & type_name) const
{
    return tryGet(type_name) != nullptr;
}

std::vector<String> UserDefinedTypeFactory::getAllRegisteredNames() const
{
    std::vector<String> names;

    const auto * storage = tryGetStorage();
    if (!storage)
        return names;

    auto all_names = storage->getAllObjectNames();
    names.assign(all_names.begin(), all_names.end());
    /// The storage keeps the objects in a hash map; sort for a deterministic `SHOW TYPES`.
    std::sort(names.begin(), names.end());
    return names;
}

}
