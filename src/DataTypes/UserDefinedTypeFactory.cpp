#include <DataTypes/UserDefinedTypeFactory.h>

#include <Backups/BackupEntriesCollector.h>
#include <Backups/RestoreSettings.h>
#include <Backups/RestorerFromBackup.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeFactory.h>
#include <Functions/UserDefined/IUserDefinedSQLObjectsStorage.h>
#include <Functions/UserDefined/UserDefinedSQLObjectType.h>
#include <Functions/UserDefined/UserDefinedSQLObjectsBackup.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateTypeQuery.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/ASTObjectTypeArgument.h>
#include <Poco/String.h>

#include <algorithm>
#include <exception>
#include <unordered_map>
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

/// The number of leading arguments of the type family `family_name` that are not types: the optional version and
/// the aggregate function of `AggregateFunction([version, ]sum, UInt64)`, the function of `SimpleAggregateFunction`.
size_t getNumberOfLeadingNonTypeArguments(const String & family_name, const ASTPtr & arguments)
{
    if (!arguments || arguments->children.empty())
        return 0;
    if (family_name == "AggregateFunction")
        return arguments->children[0]->as<ASTLiteral>() ? 2 : 1;
    if (family_name == "SimpleAggregateFunction")
        return 1;
    return 0;
}

/// Calls `callback` for every name that is used as a type inside `node`, in the order of occurrence.
/// If the callback returns false, the arguments of that type are not descended into.
/// Only the positions that hold types are visited: the names of aggregate functions, settings like
/// `Dynamic(max_types = 10)`, `Enum` elements and the skipped paths of `JSON` are not types.
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

        size_t num_non_type_arguments = getNumberOfLeadingNonTypeArguments(data_type->name, arguments);
        for (size_t i = num_non_type_arguments; i < num_arguments; ++i)
            forEachTypeReference(arguments->children[i], callback);
        return;
    }

    /// A `JSON` argument: only the type of a typed path (`a.b UInt32`) is a type.
    if (const auto * object_argument = node->as<ASTObjectTypeArgument>())
    {
        forEachTypeReference(object_argument->path_with_type, callback);
        return;
    }
    if (const auto * typed_path = node->as<ASTObjectTypedPathArgument>())
    {
        forEachTypeReference(typed_path->type, callback);
        return;
    }

    /// Functions (the `equals` of an `Enum8('a' = 1)` element or of a setting like `max_types = 10`) and literals
    /// are values, not types.
    if (node->as<ASTFunction>() || node->as<ASTLiteral>())
        return;

    /// Other nodes (e.g. the name-type pairs of `Nested`): only descend.
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

/// The definitions that are about to be stored, by type name: the definition `registerType` is about to store in place
/// of an existing one, or all the definitions `restore` replays from a backup. While they are set, `tryGet` and
/// `getAllDefinitions` return them instead of the stored definitions of these types, so the types can be validated and
/// expanded as they would be after the definitions are stored.
using PendingDefinitions = std::unordered_map<String, ASTPtr>;
thread_local const PendingDefinitions * pending_definitions = nullptr;

class PendingDefinitionsGuard
{
public:
    /// The new definitions take precedence over the ones set by an enclosing guard.
    explicit PendingDefinitionsGuard(PendingDefinitions definitions_)
        : definitions(std::move(definitions_))
        , previous(pending_definitions)
    {
        if (previous)
            definitions.insert(previous->begin(), previous->end());
        pending_definitions = &definitions;
    }

    explicit PendingDefinitionsGuard(const ASTPtr & definition)
        : PendingDefinitionsGuard(PendingDefinitions{{getCreateTypeQuery(*definition).name, definition}})
    {
    }

    ~PendingDefinitionsGuard() { pending_definitions = previous; }

    PendingDefinitionsGuard(const PendingDefinitionsGuard &) = delete;
    PendingDefinitionsGuard & operator=(const PendingDefinitionsGuard &) = delete;

private:
    PendingDefinitions definitions;
    const PendingDefinitions * previous;
};

/// All definitions of the storage, with the pending definitions in place of the stored ones.
VectorWithMemoryTracking<std::pair<String, ASTPtr>> getAllDefinitions(const IUserDefinedSQLObjectsStorage & storage)
{
    auto objects = storage.getAllObjects();
    if (!pending_definitions)
        return objects;

    for (auto & [name, create_query] : objects)
    {
        if (auto it = pending_definitions->find(name); it != pending_definitions->end())
            create_query = it->second;
    }
    for (const auto & [name, create_query] : *pending_definitions)
    {
        if (!storage.has(name))
            objects.emplace_back(name, create_query);
    }
    return objects;
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
    for (const auto & [dependent_name, create_query] : getAllDefinitions(storage))
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

/// Whether the type expression `node` uses any of `parameter_names`, so it can only be instantiated with actual arguments.
bool dependsOnParameters(const ASTPtr & node, const std::unordered_set<String> & parameter_names)
{
    bool result = false;
    forEachTypeReference(node, [&](const TypeReference & ref) -> bool
    {
        if (ref.num_arguments == 0 && parameter_names.contains(ref.name))
            result = true;
        return !result;
    });
    return result;
}

/// Throws if replacing `type_name` with `new_definition` makes the definition of another user-defined type invalid.
/// Matching the number of parameters is not enough: after
///     CREATE TYPE A(T) AS Array(T); CREATE TYPE B AS A(UInt8);
/// the replacement `CREATE TYPE OR REPLACE A(T) AS Map(T)` would make `B` expand to the invalid `Map(UInt8)`.
/// So every type depending on `type_name` (directly or through other types) is expanded with the new definition in
/// place: as a whole if it has no parameters, otherwise each of its uses of the affected types that does not depend on
/// its own parameters (`Tuple(T, B)` is checked through `B`; `A(T)` can only be checked when the type is used).
void checkDependentsAfterReplacement(const IUserDefinedSQLObjectsStorage & storage, const String & type_name, const ASTPtr & new_definition)
{
    std::unordered_set<String> affected{type_name};
    std::vector<String> dependents;
    std::vector<String> to_visit{type_name};
    while (!to_visit.empty())
    {
        String current = std::move(to_visit.back());
        to_visit.pop_back();

        for (const auto & use : findDependentUses(storage, current))
        {
            if (affected.insert(use.dependent_type_name).second)
            {
                dependents.push_back(use.dependent_type_name);
                to_visit.push_back(use.dependent_type_name);
            }
        }
    }

    if (dependents.empty())
        return;

    std::sort(dependents.begin(), dependents.end());

    const auto & data_type_factory = DataTypeFactory::instance();
    const auto & udt_factory = UserDefinedTypeFactory::instance();
    PendingDefinitionsGuard guard(new_definition);

    for (const auto & dependent_name : dependents)
    {
        /// A type may disappear concurrently; then there is nothing to check.
        auto create_query = udt_factory.tryGet(dependent_name);
        if (!create_query)
            continue;

        const auto & dependent = getCreateTypeQuery(*create_query);
        auto parameter_names = getParameterNames(dependent);

        try
        {
            if (parameter_names.empty())
            {
                data_type_factory.get(dependent.base_type);
                continue;
            }

            forEachTypeReference(dependent.base_type, [&](const TypeReference & ref) -> bool
            {
                if (ref.num_arguments == 0 && parameter_names.contains(ref.name))
                    return false;
                if (!affected.contains(ref.name) || dependsOnParameters(ref.node, parameter_names))
                    return true;
                data_type_factory.get(ref.node);
                return false;
            });
        }
        catch (Exception & exception)
        {
            exception.addMessage(fmt::format(
                "while checking user-defined type {} that depends on the replaced user-defined type {}",
                backQuote(dependent_name), backQuote(type_name)));
            throw;
        }
    }
}

/// Throws if a parameterized definition `create` uses a built-in type family with a number of arguments that the family
/// never accepts, whatever the actual arguments are: `CREATE TYPE Broken(T) AS Map(T)` and
/// `CREATE TYPE Broken(T) AS Map(String)` must be rejected, but the family name alone does not tell that.
///
/// Each use of a family in the definition is instantiated on its own, with all parameters replaced by one of a few
/// probe arguments. A parameter can stand for a type (`Array(T)`) or a value (`FixedString(N)`, `DateTime64(3, TZ)`),
/// and the probes can not cover every valid argument (`Decimal(P, 2)` is valid for `P = 10`, but not for `P = 1`), so
/// only a wrong number of arguments, which does not depend on the values of the arguments, is an error here. Other
/// errors depend on the actual arguments and are reported when the type is used.
void checkNumbersOfArguments(const ASTCreateTypeQuery & create, const std::unordered_set<String> & parameter_names)
{
    const ASTs probes{
        make_intrusive<ASTIdentifier>("UInt8"),
        make_intrusive<ASTLiteral>(Field(UInt64(1))),
        make_intrusive<ASTIdentifier>("String"),
        make_intrusive<ASTLiteral>(Field(String("UTC"))),
    };

    const auto & data_type_factory = DataTypeFactory::instance();
    const auto & udt_factory = UserDefinedTypeFactory::instance();

    forEachTypeReference(create.base_type, [&](const TypeReference & ref) -> bool
    {
        /// Parameters and complete types without arguments are checked by `validateDefinition`, and so is the number
        /// of arguments of a user-defined type.
        if (ref.num_arguments == 0)
            return false;
        if (udt_factory.has(ref.name))
            return true;

        std::exception_ptr wrong_number_of_arguments;
        for (const auto & probe : probes)
        {
            std::unordered_map<String, ASTPtr> substitutions;
            for (const auto & parameter_name : parameter_names)
                substitutions.emplace(parameter_name, probe);

            try
            {
                data_type_factory.get(substituteTypeParameters(ref.node, substitutions));
                return true;
            }
            catch (Exception & exception)
            {
                if (exception.code() != ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH)
                    return true;
                if (!wrong_number_of_arguments)
                {
                    exception.addMessage(fmt::format(
                        "in the definition of user-defined type {}: {} is not a valid data type for any actual arguments",
                        backQuote(create.name), ref.node->formatForErrorMessage()));
                    wrong_number_of_arguments = std::current_exception();
                }
            }
        }
        std::rethrow_exception(wrong_number_of_arguments);
    });
}

/// Orders `objects` (pairs of a name and a `CREATE TYPE` query) so that every type comes after the types of
/// `objects` it is defined through. Types that form a cycle are kept in their original relative order; registering
/// them fails with a proper error.
void sortByDependencies(VectorWithMemoryTracking<std::pair<String, ASTPtr>> & objects)
{
    std::unordered_map<String, size_t> index_by_name;
    for (size_t i = 0; i < objects.size(); ++i)
        index_by_name.emplace(objects[i].first, i);

    std::vector<std::vector<size_t>> dependencies(objects.size());
    for (size_t i = 0; i < objects.size(); ++i)
    {
        const auto & create = getCreateTypeQuery(*objects[i].second);
        auto parameter_names = getParameterNames(create);
        forEachTypeReference(create.base_type, [&](const TypeReference & ref) -> bool
        {
            if (ref.num_arguments == 0 && parameter_names.contains(ref.name))
                return false;
            if (auto it = index_by_name.find(ref.name); it != index_by_name.end() && it->second != i)
                dependencies[i].push_back(it->second);
            return true;
        });
    }

    enum class State : uint8_t { NotVisited, InProgress, Done };
    std::vector<State> states(objects.size(), State::NotVisited);
    std::vector<size_t> order;
    order.reserve(objects.size());

    /// Iterative depth-first search: a definition can be deeply nested in a backup made from a hand-edited storage.
    for (size_t root = 0; root < objects.size(); ++root)
    {
        if (states[root] != State::NotVisited)
            continue;

        std::vector<std::pair<size_t, size_t>> stack{{root, 0}};
        states[root] = State::InProgress;
        while (!stack.empty())
        {
            auto & [current, next_dependency] = stack.back();
            if (next_dependency < dependencies[current].size())
            {
                size_t dependency = dependencies[current][next_dependency++];
                if (states[dependency] == State::NotVisited)
                {
                    states[dependency] = State::InProgress;
                    stack.emplace_back(dependency, 0);
                }
                continue;
            }
            states[current] = State::Done;
            order.push_back(current);
            stack.pop_back();
        }
    }

    VectorWithMemoryTracking<std::pair<String, ASTPtr>> sorted;
    sorted.reserve(objects.size());
    for (size_t index : order)
        sorted.push_back(std::move(objects[index]));
    objects = std::move(sorted);
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


ASTPtr substituteTypeParameters(const ASTPtr & ast, const std::unordered_map<String, ASTPtr> & substitutions)
{
    if (!ast)
        return nullptr;

    if (const auto * identifier = ast->as<ASTIdentifier>())
    {
        if (auto it = substitutions.find(identifier->name()); it != substitutions.end())
            return it->second->clone();
    }
    else if (const auto * data_type = ast->as<ASTDataType>())
    {
        auto arguments = data_type->getArguments();
        if (!arguments || arguments->children.empty())
        {
            if (auto it = substitutions.find(data_type->name); it != substitutions.end())
                return it->second->clone();
        }
    }
    else if (const auto * object_argument = ast->as<ASTObjectTypeArgument>())
    {
        /// The skipped paths of `JSON(SKIP a.b, SKIP REGEXP '...')` are not types.
        if (object_argument->skip_path || object_argument->skip_path_regexp)
            return ast->clone();
    }

    auto result = ast->clone();
    for (auto & child : result->children)
    {
        auto old_child = child;
        child = substituteTypeParameters(old_child, substitutions);

        /// Some nodes also keep their children in named fields, and the data type constructors read these fields:
        /// e.g. `Nested(x T)` is built from the `type` of each `ASTNameTypePair`. They must point to the substituted
        /// children, not to the children of the clone.
        auto update = [&](ASTPtr & field)
        {
            if (field.get() == old_child.get())
                field = child;
        };
        if (auto * name_type_pair = result->as<ASTNameTypePair>())
        {
            update(name_type_pair->type);
        }
        else if (auto * typed_path = result->as<ASTObjectTypedPathArgument>())
        {
            update(typed_path->type);
        }
        else if (auto * object_argument = result->as<ASTObjectTypeArgument>())
        {
            update(object_argument->path_with_type);
            update(object_argument->parameter);
        }
        else if (auto * function = result->as<ASTFunction>())
        {
            update(function->arguments);
            update(function->parameters);
            update(function->window_definition);
        }
    }
    return result;
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
    std::lock_guard lock(ddl_mutex);
    syncWithStorage(storage);

    auto normalized_query = prepareType(storage, type_name, create_type_query, throw_if_exists, replace_if_exists);
    if (!normalized_query)
        return false;
    return storeType(current_context, storage, type_name, normalized_query, throw_if_exists, replace_if_exists);
}

void UserDefinedTypeFactory::syncWithStorage(IUserDefinedSQLObjectsStorage & storage)
{
    /// With a replicated storage, other servers change the types too, and the local copy of the definitions catches
    /// up with their changes asynchronously. Re-read the definitions, so the dependencies are checked against the
    /// latest state the coordination service has, not against a lagging local copy.
    if (storage.isReplicated())
        storage.reloadObjects();
}

ASTPtr UserDefinedTypeFactory::prepareType(
    const IUserDefinedSQLObjectsStorage & storage,
    const String & type_name,
    const ASTPtr & create_type_query,
    bool throw_if_exists,
    bool replace_if_exists) const
{
    /// `IF NOT EXISTS` for an existing type is a no-op: the new definition is not even validated, like for
    /// `CREATE TABLE IF NOT EXISTS`. The storage re-checks the existence under its lock.
    if (!throw_if_exists && !replace_if_exists && storage.has(type_name))
        return nullptr;

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
    auto normalized_query = normalizeCreateTypeQuery(create);
    if (parameter_names.empty())
        data_type_factory.get(create.base_type);
    else
        checkNumbersOfArguments(create, parameter_names);

    /// Other types keep using the replaced type with the number of arguments they were defined with, and the new
    /// definition must stay valid for the actual arguments they use it with.
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

        checkDependentsAfterReplacement(storage, type_name, normalized_query);
    }

    return normalized_query;
}

bool UserDefinedTypeFactory::storeType(
    const ContextMutablePtr & current_context,
    IUserDefinedSQLObjectsStorage & storage,
    const String & type_name,
    const ASTPtr & normalized_query,
    bool throw_if_exists,
    bool replace_if_exists)
{
    try
    {
        return storage.storeObject(
            current_context,
            UserDefinedSQLObjectType::Type,
            type_name,
            normalized_query,
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
    std::lock_guard lock(ddl_mutex);
    syncWithStorage(storage);

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
    if (pending_definitions)
    {
        if (auto it = pending_definitions->find(type_name); it != pending_definitions->end())
            return it->second;
    }

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

void UserDefinedTypeFactory::backup(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup) const
{
    backupUserDefinedSQLObjects(
        backup_entries_collector,
        data_path_in_backup,
        UserDefinedSQLObjectType::Type,
        backup_entries_collector.getContext()->getUserDefinedTypesStorage().getAllObjects());
}

void UserDefinedTypeFactory::restore(RestorerFromBackup & restorer, const String & data_path_in_backup) const
{
    auto restored_types = restoreUserDefinedSQLObjects(restorer, data_path_in_backup, UserDefinedSQLObjectType::Type);
    sortByDependencies(restored_types);

    const auto & restore_settings = restorer.getRestoreSettings();
    bool throw_if_exists = (restore_settings.create_function == RestoreUDFCreationMode::kCreate);
    bool replace_if_exists = (restore_settings.create_function == RestoreUDFCreationMode::kReplace);
    auto restore_context = restorer.getContext();
    auto & storage = restore_context->getUserDefinedTypesStorage();

    std::lock_guard lock(ddl_mutex);
    syncWithStorage(storage);

    /// The backup is validated as the set of types it will become, not one definition at a time against the types it
    /// replaces: if the server has `A(T) AS Array(T)` and `B AS A(UInt8)`, and the backup has `A(K, V) AS Map(K, V)`
    /// and `B AS A(String, UInt8)`, replacing `A` must be checked against the restored `B`. The types that are kept
    /// (they exist and are not replaced) keep their current definitions.
    struct PreparedType
    {
        String name;
        ASTPtr normalized_query;
        ASTPtr previous_query;
    };
    std::vector<PreparedType> prepared_types;
    {
        PendingDefinitions definitions;
        for (const auto & [type_name, create_type_query] : restored_types)
        {
            if (replace_if_exists || !storage.has(type_name))
                definitions.emplace(type_name, normalizeCreateTypeQuery(getCreateTypeQuery(*create_type_query)));
        }
        PendingDefinitionsGuard guard(std::move(definitions));

        for (const auto & [type_name, create_type_query] : restored_types)
        {
            if (auto normalized_query = prepareType(storage, type_name, create_type_query, throw_if_exists, replace_if_exists))
                prepared_types.push_back({type_name, std::move(normalized_query), storage.tryGet(type_name)});
        }
    }

    /// The storage stores the types one by one. If storing one of them fails, the already stored ones are reverted,
    /// so the server is not left with a part of the backup that may be inconsistent with the types it replaced.
    size_t num_stored = 0;
    try
    {
        for (const auto & type : prepared_types)
        {
            storeType(restore_context, storage, type.name, type.normalized_query, throw_if_exists, replace_if_exists);
            ++num_stored;
        }
    }
    catch (...)
    {
        for (size_t i = num_stored; i > 0; --i)
        {
            const auto & type = prepared_types[i - 1];
            try
            {
                if (type.previous_query)
                    storeType(restore_context, storage, type.name, type.previous_query, /* throw_if_exists = */ false, /* replace_if_exists = */ true);
                else
                    storage.removeObject(restore_context, UserDefinedSQLObjectType::Type, type.name, /* throw_if_not_exists = */ false);
            }
            catch (...)
            {
                tryLogCurrentException(
                    getLogger("UserDefinedTypeFactory"),
                    fmt::format("Failed to revert user-defined type {} after a failed restore", backQuote(type.name)));
            }
        }
        throw;
    }
}
}
