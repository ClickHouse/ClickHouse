#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/Access/InterpreterShowGrantsQuery.h>
#include <Parsers/Access/ASTGrantQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/Access/ASTShowGrantsQuery.h>
#include <Parsers/Access/ASTUserNameWithHost.h>
#include <Access/AccessControl.h>
#include <Access/AccessRights.h>
#include <Access/CachedAccessChecking.h>
#include <Access/ContextAccess.h>
#include <Access/EnabledRolesInfo.h>
#include <Access/Role.h>
#include <Access/RolesOrUsersSet.h>
#include <Access/User.h>
#include <Columns/ColumnString.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <boost/range/algorithm_ext/push_back.hpp>
#include <base/sort.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_show_grants_for_query;
    extern const SettingsUInt64 query_plan_max_step_description_length;
    extern const SettingsBool use_skip_indexes_on_data_read;
}

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{
    void getGrantsFromAccess(
        ASTs & res,
        const AccessRights & access,
        const boost::intrusive_ptr<ASTRolesOrUsersSet> grantees,
        const AccessControl * access_control,
        bool attach_mode = false,
        bool with_implicit = false)
    {
        boost::intrusive_ptr<ASTGrantQuery> current_query = nullptr;

        AccessRightsElements elements;
        if (with_implicit)
            elements = ContextAccess::addImplicitAccessRights(access, *access_control).getElements();
        else
            elements = access.getElements();

        for (auto & element : elements)
        {
            if (element.empty())
                continue;

            if (current_query)
            {
                const auto & prev_element = current_query->access_rights_elements.back();
                bool continue_with_current_query = element.sameDatabaseAndTableAndParameter(prev_element) && element.sameOptions(prev_element);
                if (!continue_with_current_query)
                    current_query = nullptr;
            }

            if (!current_query)
            {
                current_query = make_intrusive<ASTGrantQuery>();
                current_query->grantees = grantees;
                current_query->attach_mode = attach_mode;
                if (element.is_partial_revoke)
                    current_query->is_revoke = true;
                res.push_back(current_query);
            }

            current_query->access_rights_elements.emplace_back(std::move(element));
        }
    }

    template <typename T>
    void unionAccessFromRoles(AccessRights & res, const T & entity, const AccessControl * access_control)
    {
        std::vector granted_roles(entity.granted_roles.getGranted().begin(), entity.granted_roles.getGranted().end());
        std::vector granted_roles_with_admin_option(entity.granted_roles.getGrantedWithAdminOption().begin(), entity.granted_roles.getGrantedWithAdminOption().end());
        auto roles_info = access_control->getEnabledRolesInfo(granted_roles, granted_roles_with_admin_option);

        res.makeUnion(roles_info->access);
    }

    template <typename T>
    ASTs getGrantQueriesImpl(
        const T & grantee,
        const AccessControl * access_control /* not used if attach_mode == true */,
        bool attach_mode = false,
        bool with_implicit = false,
        bool final = false,
        const AccessRights * for_query_access = nullptr)
    {
        ASTs res;

        boost::intrusive_ptr<ASTRolesOrUsersSet> grantees = make_intrusive<ASTRolesOrUsersSet>();
        grantees->names = make_intrusive<ASTUserNamesWithHost>(grantee.getName());

        if (for_query_access)
        {
            /// `FOR QUERY` implies `FINAL`, and the implicit grants are added before they are filtered.
            AccessRights access = grantee.access;
            unionAccessFromRoles(access, grantee, access_control);
            if (with_implicit)
                access = ContextAccess::addImplicitAccessRights(access, *access_control);
            access.makeIntersection(*for_query_access);
            getGrantsFromAccess(res, access, grantees, access_control);
            return res;
        }

        AccessRights access_copy;

        if (final)
        {
            access_copy = grantee.access;
            unionAccessFromRoles(access_copy, grantee, access_control);
        }

        const AccessRights & access = final ? access_copy : grantee.access;

        getGrantsFromAccess(
            res,
            access,
            grantees,
            access_control,
            attach_mode,
            with_implicit);

        if (!final)
        {
            for (const auto & element : grantee.granted_roles.getElements())
            {
                if (element.empty())
                    continue;

                auto grant_query = make_intrusive<ASTGrantQuery>();
                grant_query->grantees = grantees;
                grant_query->admin_option = element.admin_option;
                grant_query->attach_mode = attach_mode;
                if (attach_mode)
                    grant_query->roles = RolesOrUsersSet{element.ids}.toAST();
                else
                    grant_query->roles = RolesOrUsersSet{element.ids}.toASTWithNames(*access_control);
                res.push_back(std::move(grant_query));
            }
        }

        return res;
    }

    ASTs getGrantQueriesImpl(
        const IAccessEntity & entity,
        const AccessControl * access_control /* not used if attach_mode == true */,
        bool attach_mode = false,
        bool with_implicit = false,
        bool final = false,
        const AccessRights * for_query_access = nullptr)
    {
        if (const User * user = typeid_cast<const User *>(&entity))
            return getGrantQueriesImpl(*user, access_control, attach_mode, with_implicit, final, for_query_access);
        if (const Role * role = typeid_cast<const Role *>(&entity))
            return getGrantQueriesImpl(*role, access_control, attach_mode, with_implicit, final, for_query_access);
        throw Exception(ErrorCodes::LOGICAL_ERROR, "{} is expected to be user or role", entity.formatTypeWithName());
    }

}


BlockIO InterpreterShowGrantsQuery::execute()
{
    BlockIO res;
    res.pipeline = executeImpl();
    return res;
}


QueryPipeline InterpreterShowGrantsQuery::executeImpl()
{
    /// Build a create query.
    ASTs grant_queries = getGrantQueries();

    /// Build the result column.
    MutableColumnPtr column = ColumnString::create();
    for (const auto & grant_query : grant_queries)
    {
        column->insert(grant_query->formatWithSecretsOneLine());
    }

    /// Prepare description of the result column.
    const auto & show_query = query_ptr->as<const ASTShowGrantsQuery &>();
    String desc = show_query.for_query ? show_query.formatForLogging() : show_query.formatWithSecretsOneLine();
    String prefix = "SHOW ";
    if (desc.starts_with(prefix))
        desc = desc.substr(prefix.length()); /// `desc` always starts with "SHOW ", so we can trim this prefix.

    return QueryPipeline(std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(Block{{std::move(column), std::make_shared<DataTypeString>(), desc}})));
}


std::vector<AccessEntityPtr> InterpreterShowGrantsQuery::getEntities() const
{
    const auto & access = getContext()->getAccess();
    const auto & access_control = getContext()->getAccessControl();

    const auto & show_query = query_ptr->as<ASTShowGrantsQuery &>();
    auto ids = RolesOrUsersSet{*show_query.for_roles, access_control, getContext()->getUserID()}.getMatchingIDs(access_control);

    CachedAccessChecking show_users(access, AccessType::SHOW_USERS);
    CachedAccessChecking show_roles(access, AccessType::SHOW_ROLES);
    bool throw_if_access_denied = !show_query.for_roles->all;

    auto current_user = access->getUser();
    auto roles_info = access->getRolesInfo();

    std::vector<AccessEntityPtr> entities;
    for (const auto & id : ids)
    {
        auto entity = access_control.tryRead(id);
        if (!entity)
            continue;

        bool is_current_user = (id == access->getUserID());
        bool is_enabled_or_granted_role = entity->isTypeOf<Role>()
            && (current_user->granted_roles.isGranted(id) || roles_info->enabled_roles.contains(id));

        if (is_current_user /* Any user can see his own grants */
            || is_enabled_or_granted_role /* and grants from the granted roles */
            || (entity->isTypeOf<User>() && show_users.checkAccess(throw_if_access_denied))
            || (entity->isTypeOf<Role>() && show_roles.checkAccess(throw_if_access_denied)))
            entities.push_back(entity);
    }

    ::sort(entities.begin(), entities.end(), IAccessEntity::LessByTypeAndName{});
    return entities;
}


AccessRights InterpreterShowGrantsQuery::getAccessCheckedByQuery() const
{
    const auto & show_query = query_ptr->as<const ASTShowGrantsQuery &>();

    /// Analyzed like `EXPLAIN PIPELINE`, in its own query context so that only its checks are recorded.
    auto analysis_context = Context::createCopy(getContext());
    analysis_context->makeQueryContext();
    analysis_context->getQueryPrivilegesInfoPtr()->startRecordingAccess();

    QueryPlan plan;
    ContextPtr context;
    {
        InterpreterSelectQueryAnalyzer interpreter(show_query.for_query, analysis_context, SelectQueryOptions{});
        interpreter.applyDistributedPlanFallbackIfNeeded();
        context = interpreter.getContext();
        plan = std::move(interpreter).extractQueryPlan();
    }

    /// Some storages check access only when their pipeline is built. The pipeline is never executed.
    auto optimization_settings = QueryPlanOptimizationSettings(context);
    optimization_settings.is_explain = true;
    optimization_settings.use_skip_indexes_on_data_read_at_execution = getContext()->getSettingsRef()[Setting::use_skip_indexes_on_data_read];
    optimization_settings.max_step_description_length = getContext()->getSettingsRef()[Setting::query_plan_max_step_description_length];
    auto pipeline = plan.buildQueryPipeline(optimization_settings, BuildQueryPipelineSettings(context));

    AccessRights res;
    for (const auto & element : analysis_context->getQueryPrivilegesInfo().getRecordedAccess())
        res.grantWithGrantOption(element);
    return res;
}


ASTs InterpreterShowGrantsQuery::getGrantQueries() const
{
    const auto & show_query = query_ptr->as<const ASTShowGrantsQuery &>();
    if (show_query.for_query)
    {
        if (!getContext()->getSettingsRef()[Setting::allow_experimental_show_grants_for_query])
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "SHOW GRANTS ... FOR QUERY is experimental. Set `allow_experimental_show_grants_for_query` = 1 to enable it");
        if (!dynamic_cast<const ASTSelectWithUnionQuery *>(show_query.for_query.get()))
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Only SELECT is supported in SHOW GRANTS ... FOR QUERY");
    }

    auto entities = getEntities();
    const auto & access_control = getContext()->getAccessControl();

    std::optional<AccessRights> for_query_access;
    if (show_query.for_query)
        for_query_access = getAccessCheckedByQuery();

    ASTs grant_queries;
    for (const auto & entity : entities)
    {
        boost::range::push_back(grant_queries, getGrantQueriesImpl(*entity, &access_control, /* attach_mode= */ false,
            show_query.with_implicit, show_query.final, for_query_access ? &*for_query_access : nullptr));
    }

    return grant_queries;
}


bool InterpreterShowGrantsQuery::ignoreQuota() const
{
    /// Analyzing the query of `FOR QUERY` costs as much as `EXPLAIN PIPELINE`.
    return !query_ptr->as<const ASTShowGrantsQuery &>().for_query;
}


bool InterpreterShowGrantsQuery::ignoreLimits() const
{
    return !query_ptr->as<const ASTShowGrantsQuery &>().for_query;
}


ASTs InterpreterShowGrantsQuery::getGrantQueries(const IAccessEntity & user_or_role, const AccessControl & access_control, bool with_implicit, bool final)
{
    return getGrantQueriesImpl(user_or_role, &access_control, false, with_implicit, final);
}


ASTs InterpreterShowGrantsQuery::getAttachGrantQueries(const IAccessEntity & user_or_role)
{
    return getGrantQueriesImpl(user_or_role, nullptr, true, false);
}

void registerInterpreterShowGrantsQuery(InterpreterFactory & factory);
void registerInterpreterShowGrantsQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterShowGrantsQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterShowGrantsQuery", create_fn);
}

}
