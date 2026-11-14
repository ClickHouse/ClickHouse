#include <Interpreters/InterpreterCreateHandlerQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Parsers/ASTCreateHandlerQuery.h>
#include <Access/ContextAccess.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionVisitor.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/ApplyWithSubqueryVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Interpreters/misc.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Core/QualifiedTableName.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSQLSecurity.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/Access/ASTExecuteAsQuery.h>
#include <Parsers/QueryParameterVisitor.h>
#include <Common/SQLDefinedHandlers/SQLDefinedHandlerFromAST.h>
#include <Common/SQLDefinedHandlers/SQLDefinedHandlersFactory.h>
#include <Common/SQLDefinedHandlers/SQLDefinedHandlersMetadataStorage.h>
#include <base/EnumReflection.h>
#include <Core/Settings.h>
#include <Poco/String.h>

#include <unordered_set>


namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 max_expanded_ast_elements;
}

namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
    extern const int HANDLER_ALREADY_EXISTS;
    extern const int KEEPER_EXCEPTION;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

/// The named collection privileges are not required: `users.xml` administrators do not get them by default.
/// Nor are the deprecated source privileges: only `ALL` grants them, a stored grant list is reloaded as `READ, WRITE ON <source>`.
bool hasFullAccess(const ContextPtr & context)
{
    static const AccessFlags not_required = []
    {
        AccessFlags flags = AccessType::NAMED_COLLECTION_ADMIN | AccessType::SOURCES;
#define ADD_DEPRECATED_SOURCE(name, alias) flags |= AccessType::name;
        APPLY_FOR_SOURCE(ADD_DEPRECATED_SOURCE)
#undef ADD_DEPRECATED_SOURCE
        return flags;
    }();
    return context->getAccess()->hasGrantOption(AccessFlags::allFlags() - not_required);
}

/// A stored `<user>:definer`, the clone `processSQLSecurityOption` makes of an ephemeral `<user>`, is not `<user>`:
/// a real user can have that name, and a clone keeps grants `<user>` can lose.
bool isDefinerTheCurrentUser(const ASTSQLSecurity & sql_security, const ContextPtr & context)
{
    if (sql_security.is_definer_current_user)
        return true;
    return sql_security.definer && sql_security.definer->toString() == context->getUserName();
}

SQLSecurityType getSQLSecurityType(const ASTCreateHandlerQuery & handler)
{
    if (handler.sql_security)
        if (const auto & type = handler.sql_security->as<const ASTSQLSecurity &>().type)
            return *type;
    return SQLSecurityType::INVOKER;
}

/// Checks the grants `processSQLSecurityOption` demands for the SQL SECURITY a handler already has.
void checkAccessForStoredSQLSecurity(const ASTCreateHandlerQuery & handler, const ContextPtr & context)
{
    const auto type = getSQLSecurityType(handler);
    if (type == SQLSecurityType::NONE)
    {
        context->checkAccess(AccessType::ALLOW_SQL_SECURITY_NONE);
        return;
    }

    if (type != SQLSecurityType::DEFINER)
        return;

    const auto & sql_security = handler.sql_security->as<const ASTSQLSecurity &>();
    if (!sql_security.definer)
        return;

    if (!isDefinerTheCurrentUser(sql_security, context))
        context->checkAccess(AccessType::SET_DEFINER, sql_security.definer->toString());
}

bool hasSettingsClause(const IAST & ast)
{
    if (ast.as<ASTSetQuery>())
        return true;
    for (const auto & child : ast.children)
        if (child && hasSettingsClause(*child))
            return true;
    return false;
}

/// A settings profile can set `readonly` too.
bool setsReadonly(const IAST & ast)
{
    auto is_readonly_or_profile = [](std::string_view name)
    {
        const auto resolved = Settings::resolveName(name);
        return resolved == "readonly" || resolved == "profile";
    };
    if (const auto * set = ast.as<ASTSetQuery>())
    {
        if (std::ranges::any_of(set->changes, [&](const auto & change) { return is_readonly_or_profile(change.name); })
            || std::ranges::any_of(set->default_settings, is_readonly_or_profile))
            return true;
    }
    for (const auto & child : ast.children)
        if (child && setsReadonly(*child))
            return true;
    return false;
}

String describeStatementKind(const IAST & statement)
{
    if (statement.as<ASTExecuteAsQuery>())
        return "EXECUTE AS";
    return Poco::toUpper(String(magic_enum::enum_name(statement.getQueryKind())));
}

void collectQueryAliases(const IAST & ast, std::unordered_set<String> & aliases)
{
    if (String alias = ast.tryGetAlias(); !alias.empty())
        aliases.insert(std::move(alias));
    for (const auto & child : ast.children)
        if (child)
            collectQueryAliases(*child, aliases);
}

/// `AddDefaultDatabaseVisitor` leaves the table name of `joinGet` alone, and `joinGet` resolves a bare name in the current
/// database of the request. Qualify it, so the handler reads the table its query was checked against.
void qualifyJoinGetTableNames(IAST & ast, const String & database, const std::unordered_set<String> & aliases)
{
    if (auto * function = ast.as<ASTFunction>(); function && function->arguments && functionIsJoinGet(function->name)
        && !function->arguments->children.empty())
    {
        auto & table_argument = function->arguments->children[0];
        if (const auto * identifier = table_argument->as<ASTIdentifier>())
        {
            /// A compound identifier is already qualified, a parameterized name is only known when the handler is invoked,
            /// and an alias of an expression is not a table name.
            if (!identifier->compound() && !identifier->isParam() && !aliases.contains(identifier->name()))
                table_argument = make_intrusive<ASTIdentifier>(std::vector<String>{database, identifier->name()});
        }
        else if (auto * literal = table_argument->as<ASTLiteral>(); literal && literal->value.getType() == Field::Types::String)
        {
            auto qualified_table_name = QualifiedTableName::tryParseFromString(literal->value.safeGet<String>());
            if (qualified_table_name && qualified_table_name->database.empty())
            {
                qualified_table_name->database = database;
                literal->value = qualified_table_name->getFullName();
            }
        }
    }

    for (const auto & child : ast.children)
        if (child)
            qualifyJoinGetTableNames(*child, database, aliases);
}

/// Qualifies the names with the current database of the user storing the handler, as `CREATE VIEW` does.
void qualifyStatement(ASTPtr & statement, const AddDefaultDatabaseVisitor & visitor, const String & database, const ContextPtr & context)
{
    const size_t max_expanded_ast_elements = context->getSettingsRef()[Setting::max_expanded_ast_elements];
    auto qualify_select = [&](ASTPtr & select)
    {
        /// The full traversal of the visitor expects the common table expressions to be expanded.
        ApplyWithSubqueryVisitor::visit(select, max_expanded_ast_elements);
        visitor.visit(select);
        std::unordered_set<String> aliases;
        collectQueryAliases(*select, aliases);
        qualifyJoinGetTableNames(*select, database, aliases);
    };

    if (auto * insert = statement->as<ASTInsertQuery>())
    {
        if (!insert->database)
            insert->setDatabase(database);
        if (insert->select)
            qualify_select(insert->select);
        if (insert->returning)
            qualify_select(insert->returning);
        return;
    }
    qualify_select(statement);
}

void checkSelectAccess(const ASTPtr & statement, const ContextPtr & context)
{
    InterpreterSelectQueryAnalyzer::getSampleBlock(statement->clone(), context, SelectQueryOptions{}.analyze().checkSubqueryTableAccess());
}

void replaceHandlerQuery(ASTCreateHandlerQuery & handler, ASTPtr query)
{
    for (auto & child : handler.children)
    {
        if (child.get() == handler.query.get())
        {
            child = query;
            break;
        }
    }
    handler.query = std::move(query);
}

void replaceHandlerSQLSecurity(ASTCreateHandlerQuery & handler, ASTPtr sql_security)
{
    if (handler.sql_security)
        handler.replace(handler.sql_security, std::move(sql_security));
    else
        handler.set(handler.sql_security, std::move(sql_security));
}

/// Checks that the user storing `handler` may run its query, and rewrites the query into the stored form. This is proven
/// when the query runs (full access, or it runs as this user), or here by analysis of a SELECT without parameters or SETTINGS.
void validateHandlerQuery(ASTCreateHandlerQuery & handler, const ContextPtr & context)
{
    if (!handler.query)
        return;

    /// Like a view, a handler keeps the definitions of the SQL UDFs it was stored with.
    if (!UserDefinedSQLFunctionFactory::instance().empty())
    {
        ASTPtr query = handler.query;
        UserDefinedSQLFunctionVisitor::visit(query, context);
        if (query != handler.query)
            replaceHandlerQuery(handler, std::move(query));
    }

    const auto type = getSQLSecurityType(handler);
    const bool runs_as_definer = type != SQLSecurityType::INVOKER;
    /// A NONE query runs with full access, named collections included.
    const bool full_access = hasFullAccess(context)
        && (type != SQLSecurityType::NONE || context->getAccess()->isGranted(AccessType::NAMED_COLLECTION));
    const bool self_definer = type == SQLSecurityType::DEFINER
        && isDefinerTheCurrentUser(handler.sql_security->as<const ASTSQLSecurity &>(), context);

    const String database = context->getCurrentDatabase();
    const AddDefaultDatabaseVisitor visitor(context, database);

    for (ASTPtr * statement_ptr : getExecutedStatements(handler.query))
    {
        ASTPtr & statement = *statement_ptr;
        const bool is_select = isSelectShapedStatement(*statement);

        /// A query running as the definer must not act on the requester's session or reach objects chosen by the request.
        if (runs_as_definer && !is_select && !isInsertIntoTableStatement(*statement))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Handler `{}` with SQL SECURITY {} runs a query of type {}. Only SELECT queries and INSERT queries into a table "
                "are supported for handlers with SQL SECURITY DEFINER or NONE",
                handler.handler_name, magic_enum::enum_name(type), describeStatementKind(*statement));

        if (runs_as_definer && setsReadonly(*statement))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Handler `{}` with SQL SECURITY {} changes `readonly` or the settings profile in its query. A handler with SQL SECURITY "
                "DEFINER or NONE runs in the stricter `readonly` mode of the request and of its definer, or of the server's `system_profile` for NONE",
                handler.handler_name, magic_enum::enum_name(type));

        if (full_access || self_definer)
        {
            if (runs_as_definer)
            {
                if (!context->getExternalTables().empty())
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Handler `{}` with SQL SECURITY {} cannot be stored in a session with temporary tables: its query runs "
                        "without them, and a temporary table can have the name of a table it reads. Store the handler from a "
                        "session without temporary tables",
                        handler.handler_name, magic_enum::enum_name(type));
                qualifyStatement(statement, visitor, database, context);
            }
            /// The privileges for a parameterized SELECT and for an INSERT are checked when the query runs as the definer.
            if (!full_access && is_select && analyzeReceiveQueryParams(statement).empty())
                checkSelectAccess(statement, context);
            continue;
        }

        const std::string_view way_out = type == SQLSecurityType::INVOKER
            ? "Use DEFINER = CURRENT_USER SQL SECURITY DEFINER to store a query that is checked against your privileges "
              "when it runs, or create the handler as a user with full access"
            : "Use DEFINER = CURRENT_USER, or create the handler as a user with full access";

        if (!context->getExternalTables().empty())
            throw Exception(ErrorCodes::ACCESS_DENIED,
                "Handler `{}` cannot be checked against the privileges of the user `{}` in a session with temporary tables: "
                "a temporary table can have the name of a table the handler reads. Store the handler from a session "
                "without temporary tables",
                handler.handler_name, context->getUserName());

        if (!is_select)
            throw Exception(ErrorCodes::ACCESS_DENIED,
                "Handler `{}` runs a query of type {}, but only a SELECT query can be checked against the privileges of the user `{}` "
                "who stores the handler. {}",
                handler.handler_name, describeStatementKind(*statement), context->getUserName(), way_out);

        if (!analyzeReceiveQueryParams(statement).empty())
            throw Exception(ErrorCodes::ACCESS_DENIED,
                "Handler `{}` runs a query with query parameters, which cannot be checked against the privileges of the user `{}` "
                "who stores the handler: the parameters can change what the query reads. {}",
                handler.handler_name, context->getUserName(), way_out);

        if (hasSettingsClause(*statement))
            throw Exception(ErrorCodes::ACCESS_DENIED,
                "Handler `{}` runs a query with a SETTINGS clause, which cannot be checked against the privileges of the user `{}` "
                "who stores the handler: settings can change what the query reads. {}",
                handler.handler_name, context->getUserName(), way_out);

        qualifyStatement(statement, visitor, database, context);
        checkSelectAccess(statement, context);
    }
}

}

BlockIO InterpreterCreateHandlerQuery::execute()
{
    auto current_context = getContext();

    const auto updated_query = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    const auto & query = updated_query->as<const ASTCreateHandlerQuery &>();

    current_context->checkAccess(query.is_alter ? AccessType::ALTER_HANDLER : AccessType::CREATE_HANDLER);

    if (!query.cluster.empty())
    {
        /// The other hosts run the query as the initiating user only with `distributed_ddl_use_initial_user_and_roles`,
        /// and the definer is resolved and authorized on the initiator only.
        if (query.sql_security)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "DEFINER and SQL SECURITY are not supported for a handler together with ON CLUSTER. "
                "Run the query on each host instead");
        if (!hasFullAccess(current_context))
            throw Exception(ErrorCodes::ACCESS_DENIED,
                "{} HANDLER ... ON CLUSTER requires a user with full access: the handler query cannot be checked against "
                "the privileges of the user `{}` on the other hosts. Run the query on each host instead",
                query.is_alter ? "ALTER" : "CREATE", current_context->getUserName());

        DDLQueryOnClusterParams params;
        return executeDDLQueryOnCluster(updated_query, current_context, params);
    }

    auto & factory = SQLDefinedHandlersFactory::instance();

    if (!query.is_alter)
    {
        if (factory.prepareCreate(query.handler_name))
        {
            if (query.if_not_exists)
                return {};
            throw Exception(ErrorCodes::HANDLER_ALREADY_EXISTS, "A handler `{}` already exists", query.handler_name);
        }

        auto create_ast = query.clone();
        auto & create = create_ast->as<ASTCreateHandlerQuery &>();
        validateHandlerQuery(create, current_context);
        /// After the validation, so a rejected handler creates no definer clone of an ephemeral user.
        if (create.sql_security)
            InterpreterCreateQuery::processSQLSecurityOption(current_context, create.sql_security->as<ASTSQLSecurity &>());
        factory.createFromSQL(create);
        return {};
    }

    /// The validation runs without the factory lock, so the result is stored only if the handler was not changed meanwhile.
    auto alter_ast = query.clone();
    auto & alter = alter_ast->as<ASTCreateHandlerQuery &>();
    /// Made once, and kept apart from `alter`, so that every attempt validates the clause as written.
    ASTPtr processed_sql_security;
    for (size_t attempt = 0; attempt < SQLDefinedHandlersFactory::max_replicated_write_attempts; ++attempt)
    {
        const String base = factory.prepareAlter(alter.handler_name);
        auto merged_ast = parseStoredHandlerStatement(base, alter.handler_name);
        auto & merged = merged_ast->as<ASTCreateHandlerQuery &>();
        mergeAlterIntoCreateHandler(merged, alter);

        if (!alter.sql_security)
            checkAccessForStoredSQLSecurity(merged, current_context);
        validateHandlerQuery(merged, current_context);

        if (alter.sql_security)
        {
            if (!processed_sql_security)
            {
                processed_sql_security = alter.sql_security->clone();
                InterpreterCreateQuery::processSQLSecurityOption(current_context, processed_sql_security->as<ASTSQLSecurity &>());
            }
            replaceHandlerSQLSecurity(merged, processed_sql_security->clone());
        }

        if (factory.updateFromSQL(alter, base, merged))
            return {};
    }

    throw Exception(ErrorCodes::KEEPER_EXCEPTION,
        "Could not alter handler `{}`: the set of handlers kept changing concurrently", alter.handler_name);
}

void registerInterpreterCreateHandlerQuery(InterpreterFactory & factory);
void registerInterpreterCreateHandlerQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterCreateHandlerQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterCreateHandlerQuery", create_fn);
}

}
