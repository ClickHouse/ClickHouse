#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/NamedScalars/InterpreterNamedScalarDDLQuery.h>

#include <Access/Common/SQLSecurityDefs.h>
#include <Common/Exception.h>
#include <Core/Settings.h>
#include <Core/UUID.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/NamedScalars/NamedScalar.h>
#include <Interpreters/NamedScalars/NamedScalarsManager.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTNamedScalarDDLQuery.h>
#include <Parsers/ASTSQLSecurity.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_experimental_named_scalars;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NAMED_SCALAR_ALREADY_EXISTS;
    extern const int SUPPORT_IS_DISABLED;
    extern const int SYNTAX_ERROR;
}

namespace
{
String getNamedScalarName(const ASTPtr & ast)
{
    const auto * identifier = ast ? ast->as<ASTIdentifier>() : nullptr;
    if (!identifier)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Named scalar name is not an identifier");

    String name;
    if (!tryGetIdentifierNameInto(identifier, name))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Named scalar name must be an identifier");

    NamedScalarsManager::checkName(name);
    return name;
}

NamedScalarCacheKind resolveCacheKind(
    ASTNamedScalarDDLQuery::CacheKind ast_cache_kind,
    const NamedScalarsManager & manager)
{
    switch (ast_cache_kind)
    {
        case ASTNamedScalarDDLQuery::CacheKind::Default:
            return manager.getDefaultCacheKind();
        case ASTNamedScalarDDLQuery::CacheKind::Local:
            return NamedScalarCacheKind::Local;
        case ASTNamedScalarDDLQuery::CacheKind::Shared:
            return NamedScalarCacheKind::Shared;
    }
    UNREACHABLE();
}

ASTNamedScalarDDLQuery::CacheKind toASTCacheKind(NamedScalarCacheKind cache_kind)
{
    return cache_kind == NamedScalarCacheKind::Shared
        ? ASTNamedScalarDDLQuery::CacheKind::Shared
        : ASTNamedScalarDDLQuery::CacheKind::Local;
}

String resolveDefiner(ASTNamedScalarDDLQuery & create_query, const ContextMutablePtr & current_context)
{
    if (!create_query.sql_security)
    {
        auto sql_security = make_intrusive<ASTSQLSecurity>();
        sql_security->type = SQLSecurityType::DEFINER;
        sql_security->is_definer_current_user = true;
        create_query.sql_security = sql_security;
        create_query.children.push_back(create_query.sql_security);
    }

    auto & sql_security = create_query.sql_security->as<ASTSQLSecurity &>();
    InterpreterCreateQuery::processSQLSecurityOption(
        current_context,
        sql_security,
        /* is_materialized_view */ true);

    if (sql_security.type != SQLSecurityType::DEFINER)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Only SQL SECURITY DEFINER is supported for NAMED SCALAR");

    if (!sql_security.definer)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "CREATE NAMED SCALAR requires a user identity (SQL SECURITY DEFINER)");

    sql_security.type = SQLSecurityType::DEFINER;
    sql_security.is_definer_current_user = false;

    return sql_security.definer->toString();
}

BlockIO executeCreate(const ASTPtr & query_ptr, ContextMutablePtr current_context)
{
    /// Experimental gate. Operators must opt in explicitly to create
    /// named scalars on this server. Reads via getNamedScalar are NOT
    /// gated — once a scalar exists, dependent queries should not need
    /// to re-assert the experimental flag on every access.
    if (!current_context->getSettingsRef()[Setting::allow_experimental_named_scalars])
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "Named scalars are an experimental feature. "
            "Enable them by setting `allow_experimental_named_scalars = 1` "
            "before issuing CREATE NAMED SCALAR.");

    auto & create_query = query_ptr->as<ASTNamedScalarDDLQuery &>();
    auto scalar_name = getNamedScalarName(create_query.named_scalar_name);
    auto & manager = current_context->getNamedScalarsManager();
    const auto cache_kind = resolveCacheKind(create_query.cache_kind, manager);
    create_query.cache_kind = toASTCacheKind(cache_kind);

    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::CREATE_NAMED_SCALAR);
    if (create_query.or_replace)
        access_rights_elements.emplace_back(AccessType::DROP_NAMED_SCALAR);

    current_context->checkAccess(access_rights_elements);

    if (!create_query.cluster.empty() && cache_kind == NamedScalarCacheKind::Shared)
        throw Exception(ErrorCodes::SYNTAX_ERROR,
            "ON CLUSTER is not supported for SHARED NAMED SCALAR");

    const String definer_name = resolveDefiner(create_query, current_context);

    AddDefaultDatabaseVisitor add_default_database(current_context, current_context->getCurrentDatabase());
    add_default_database.visit(create_query.expression);

    if (create_query.uuid == UUIDHelpers::Nil)
        create_query.uuid = UUIDHelpers::generateV4();

    if (!create_query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        params.access_to_check = access_rights_elements;
        return executeDDLQueryOnCluster(query_ptr, current_context, params);
    }

    /// Reject CREATE OR REPLACE that changes the cache kind — mixing LOCAL
    /// and SHARED backends for the same scalar corrupts state. Consult
    /// the persisted definition (Keeper RTT for SHARED) so the guard
    /// works even when the watcher hasn't reconciled the local map yet
    /// or the scalar exists only on peers.
    if (create_query.or_replace)
    {
        auto existing_kind = manager.getCacheKind(
            scalar_name, current_context, getLogger("InterpreterNamedScalarDDLQuery"));
        if (existing_kind && *existing_kind != cache_kind)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Cannot change cache kind of named scalar '{}' via CREATE OR REPLACE "
                "(was {}, requested {}). DROP and CREATE it instead.",
                scalar_name,
                toString(*existing_kind),
                toString(cache_kind));
    }

    /// Fail fast for SHARED on an unconfigured node, before evaluating
    /// the source SELECT.
    manager.ensureCreatable(cache_kind);

    /// Avoid evaluating on duplicate / IF NOT EXISTS hits. Catalogs do
    /// their own atomic check for crash-safety.
    if (!create_query.or_replace && manager.definitionExists(scalar_name))
    {
        if (create_query.if_not_exists)
            return {};
        throw Exception(
            ErrorCodes::NAMED_SCALAR_ALREADY_EXISTS,
            "Named scalar '{}' already exists",
            scalar_name);
    }

    WriteBufferFromOwnString ddl_buf;
    IAST::FormatSettings format_settings(/*one_line=*/false);
    query_ptr->format(ddl_buf, format_settings);

    manager.create(NamedScalarCreateRequest{
        .cache_kind = cache_kind,
        .name = std::move(scalar_name),
        .formatted_create_query = ddl_buf.str(),
        .if_not_exists = create_query.if_not_exists,
        .or_replace = create_query.or_replace,
    }, current_context);
    return {};
}

BlockIO executeDrop(const ASTPtr & query_ptr, ContextMutablePtr current_context)
{
    const auto & drop_query = query_ptr->as<ASTNamedScalarDDLQuery &>();
    auto scalar_name = getNamedScalarName(drop_query.named_scalar_name);

    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::DROP_NAMED_SCALAR);

    /// Check access before any catalog probe so users without
    /// DROP_NAMED_SCALAR can't side-channel the existence/kind of a
    /// scalar via getCacheKind's Keeper RTT. Mirrors executeCreate.
    current_context->checkAccess(access_rights_elements);

    if (!drop_query.cluster.empty())
    {
        /// Determine cache kind from the persisted definition (queries Keeper
        /// for SHARED). The previous local-map-only check would silently fall
        /// through to a single-node DROP when the scalar wasn't yet in the
        /// coordinator's map (just-restarted node, lagging watcher, or a
        /// LOCAL scalar that exists only on peers) — leaving cluster state
        /// inconsistent.
        auto & manager = current_context->getNamedScalarsManager();
        auto kind = manager.getCacheKind(
            scalar_name, current_context, getLogger("InterpreterNamedScalarDDLQuery"));

        if (kind == NamedScalarCacheKind::Shared)
            throw Exception(ErrorCodes::SYNTAX_ERROR,
                "ON CLUSTER is not supported for SHARED NAMED SCALAR");

        /// LOCAL or non-existent: broadcast. Per-node DROP IF EXISTS no-ops
        /// if the scalar isn't present on that node.
        DDLQueryOnClusterParams params;
        params.access_to_check = std::move(access_rights_elements);
        return executeDDLQueryOnCluster(query_ptr, current_context, params);
    }

    current_context->getNamedScalarsManager().drop(
        scalar_name, !drop_query.if_exists);
    return {};
}
}

BlockIO InterpreterNamedScalarDDLQuery::execute()
{
    /// Strip the ON CLUSTER clause if it targets our own local cluster,
    /// so fan-out targets dispatch on cluster.empty() correctly. Same
    /// pattern as InterpreterCreateFunctionQuery /
    /// InterpreterCreateNamedCollectionQuery.
    const auto updated_query_ptr = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    const auto & query = updated_query_ptr->as<ASTNamedScalarDDLQuery &>();
    switch (query.action)
    {
        case ASTNamedScalarDDLQuery::Action::Create:
            return executeCreate(updated_query_ptr, getContext());
        case ASTNamedScalarDDLQuery::Action::Drop:
            return executeDrop(updated_query_ptr, getContext());
    }
    UNREACHABLE();
}

void registerInterpreterNamedScalarDDLQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterNamedScalarDDLQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterNamedScalarDDLQuery", create_fn);
}

}
