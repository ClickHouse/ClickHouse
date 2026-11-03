#include <Interpreters/InterpreterCreateHandlerQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Parsers/ASTCreateHandlerQuery.h>
#include <Access/AccessControl.h>
#include <Access/ContextAccess.h>
#include <Access/User.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionVisitor.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/ApplyWithSubqueryVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
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

/// A user holding every privilege with the grant option can run any query a handler can run. The named collection
/// privileges are not required: `users.xml` administrators do not get them by default.
bool hasFullAccess(const ContextPtr & context)
{
    return context->getAccess()->hasGrantOption(AccessType::ALL - AccessType::NAMED_COLLECTION_ADMIN);
}

/// `processSQLSecurityOption` stores an ephemeral definer as a clone named `<user>:definer`. A real user can have the
/// same suffix, so the suffix belongs to `<user>` only when `<user>` is ephemeral.
String getDefinerOwnerName(const String & definer, const ContextPtr & context)
{
    static constexpr std::string_view ephemeral_suffix = ":definer";
    if (definer.ends_with(ephemeral_suffix))
    {
        String base_name = definer.substr(0, definer.size() - ephemeral_suffix.size());
        const auto & access_control = context->getAccessControl();
        if (auto base_id = access_control.find<User>(base_name); base_id && access_control.isEphemeral(*base_id))
            return base_name;
    }
    return definer;
}

bool isDefinerTheCurrentUser(const ASTSQLSecurity & sql_security, const ContextPtr & context)
{
    if (sql_security.is_definer_current_user)
        return true;
    return sql_security.definer && getDefinerOwnerName(sql_security.definer->toString(), context) == context->getUserName();
}

SQLSecurityType getSQLSecurityType(const ASTCreateHandlerQuery & handler)
{
    if (handler.sql_security)
        if (const auto & type = handler.sql_security->as<const ASTSQLSecurity &>().type)
            return *type;
    return SQLSecurityType::INVOKER;
}

/// The grants `processSQLSecurityOption` demands for the stored SQL SECURITY of a handler: changing a handler that
/// runs as its definer takes the same authority as declaring that SQL SECURITY.
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

    const String definer = getDefinerOwnerName(sql_security.definer->toString(), context);
    if (definer != context->getUserName())
        context->checkAccess(AccessType::SET_DEFINER, definer);
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

String describeStatementKind(const IAST & statement)
{
    if (statement.as<ASTExecuteAsQuery>())
        return "EXECUTE AS";
    return Poco::toUpper(String(magic_enum::enum_name(statement.getQueryKind())));
}

/// Qualifies the names with the database of the user storing the handler, as `CREATE VIEW` does: otherwise they
/// would resolve in the current database of the request.
void qualifyStatement(ASTPtr & statement, const AddDefaultDatabaseVisitor & visitor, const String & database, const ContextPtr & context)
{
    const size_t max_expanded_ast_elements = context->getSettingsRef()[Setting::max_expanded_ast_elements];
    auto qualify_select = [&](ASTPtr & select)
    {
        /// The full traversal of the visitor expects the common table expressions to be expanded.
        ApplyWithSubqueryVisitor::visit(select, max_expanded_ast_elements);
        visitor.visit(select);
        visitor.visitTableExpressions(*select);
    };

    if (auto * insert = statement->as<ASTInsertQuery>())
    {
        if (insert->getDatabase().empty())
            insert->setDatabase(database);
        if (insert->select)
            qualify_select(insert->select);
        return;
    }
    qualify_select(statement);
}

/// The check `CREATE VIEW` does for its `SELECT`.
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

/// Validates the query of `handler` (a CREATE HANDLER statement, or a stored one with an ALTER HANDLER merged onto
/// it) against the user storing it, and rewrites the query into the form that is stored.
///
/// The query runs later as another user: the requesting one (SQL SECURITY INVOKER, the default), the definer, or
/// nobody (NONE). The user storing it must be allowed to run it, which is proven either when it runs (the user has
/// full access, or the handler runs as this user), or here by analysis, as for the SELECT of a view. An analysis
/// proves only the query it analyzed, so it is accepted only for a SELECT without query parameters and SETTINGS.
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
    const bool full_access = hasFullAccess(context);
    const bool self_definer = type == SQLSecurityType::DEFINER
        && isDefinerTheCurrentUser(handler.sql_security->as<const ASTSQLSecurity &>(), context);

    const String database = context->getCurrentDatabase();
    const AddDefaultDatabaseVisitor visitor(context, database);

    for (ASTPtr * statement_ptr : getExecutedStatements(handler.query))
    {
        ASTPtr & statement = *statement_ptr;
        const bool is_select = isSelectShapedStatement(*statement);

        /// A query running as the definer must not act on the session of the requesting user, and the objects it
        /// reaches must not depend on the request.
        if (runs_as_definer && !is_select && !isInsertIntoTableStatement(*statement))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Handler `{}` with SQL SECURITY {} runs a query of type {}. Only SELECT queries and INSERT queries into a table "
                "are supported for handlers with SQL SECURITY DEFINER or NONE",
                handler.handler_name, magic_enum::enum_name(type), describeStatementKind(*statement));

        if (full_access || self_definer)
        {
            if (runs_as_definer)
                qualifyStatement(statement, visitor, database, context);
            /// The privileges for a parameterized SELECT and for an INSERT are checked when the query runs as the definer.
            if (!full_access && is_select && analyzeReceiveQueryParams(statement).empty())
                checkSelectAccess(statement, context);
            continue;
        }

        const std::string_view way_out = type == SQLSecurityType::INVOKER
            ? "Use DEFINER = CURRENT_USER SQL SECURITY DEFINER to store a query that is checked against your privileges "
              "when it runs, or create the handler as a user with full access"
            : "Use DEFINER = CURRENT_USER, or create the handler as a user with full access";

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
        /// `CREATE HANDLER IF NOT EXISTS` of an existing handler is a no-op and does not validate the query.
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

    /// The validation can read tables, so it runs without the lock of the factory, and the result is stored only if
    /// the handler was not changed meanwhile.
    auto alter_ast = query.clone();
    auto & alter = alter_ast->as<ASTCreateHandlerQuery &>();
    bool alter_sql_security_processed = false;
    for (size_t attempt = 0; attempt < SQLDefinedHandlersFactory::max_replicated_write_attempts; ++attempt)
    {
        const String base = factory.prepareAlter(alter.handler_name);
        auto merged_ast = parseStoredHandlerStatement(base, alter.handler_name);
        auto & merged = merged_ast->as<ASTCreateHandlerQuery &>();
        mergeAlterIntoCreateHandler(merged, alter);

        /// Every clause of a handler decides what its query does or who can run it.
        if (!alter.sql_security)
            checkAccessForStoredSQLSecurity(merged, current_context);
        validateHandlerQuery(merged, current_context);

        if (alter.sql_security && !alter_sql_security_processed)
        {
            InterpreterCreateQuery::processSQLSecurityOption(current_context, alter.sql_security->as<ASTSQLSecurity &>());
            alter_sql_security_processed = true;
            replaceHandlerSQLSecurity(merged, alter.sql_security->clone());
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
