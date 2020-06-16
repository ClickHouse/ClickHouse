#include <Interpreters/InterpreterCreateQuotaQuery.h>
#include <Parsers/ASTCreateQuotaQuery.h>
#include <Parsers/ASTExtendedRoleSet.h>
#include <Interpreters/Context.h>
#include <Interpreters/DDLWorker.h>
#include <Access/AccessControlManager.h>
#include <Access/AccessFlags.h>
#include <ext/range.h>
#include <boost/range/algorithm/find_if.hpp>
#include <boost/range/algorithm/upper_bound.hpp>
#include <boost/range/algorithm/sort.hpp>


namespace DB
{
namespace
{
void updateQuotaFromQueryImpl(Quota & quota, const ASTCreateQuotaQuery & query, const std::optional<ExtendedRoleSet> & roles_from_query = {})
    {
        if (query.alter)
        {
            if (!query.new_name.empty())
                quota.setName(query.new_name);
        }
        else
            quota.setName(query.name);

        if (query.key_type)
            quota.key_type = *query.key_type;

        auto & quota_all_limits = quota.all_limits;
        for (const auto & query_limits : query.all_limits)
        {
            auto duration = query_limits.duration;

            auto it = boost::range::find_if(quota_all_limits, [&](const Quota::Limits & x) { return x.duration == duration; });
            if (query_limits.drop)
            {
                if (it != quota_all_limits.end())
                    quota_all_limits.erase(it);
                continue;
            }

            if (it == quota_all_limits.end())
            {
                /// We keep `all_limits` sorted by duration.
                it = quota_all_limits.insert(
                    boost::range::upper_bound(
                        quota_all_limits,
                        duration,
                        [](const std::chrono::seconds & lhs, const Quota::Limits & rhs) { return lhs < rhs.duration; }),
                    Quota::Limits{});
                it->duration = duration;
            }

            auto & quota_limits = *it;
            quota_limits.randomize_interval = query_limits.randomize_interval;
            for (auto resource_type : ext::range(Quota::MAX_RESOURCE_TYPE))
                quota_limits.max[resource_type] = query_limits.max[resource_type];
        }

        const ExtendedRoleSet * roles = nullptr;
        std::optional<ExtendedRoleSet> temp_role_set;
        if (roles_from_query)
            roles = &*roles_from_query;
        else if (query.roles)
            roles = &temp_role_set.emplace(*query.roles);

        if (roles)
            quota.to_roles = *roles;
    }
}


BlockIO InterpreterCreateQuotaQuery::execute()
{
    auto & query = query_ptr->as<ASTCreateQuotaQuery &>();
    auto & access_control = context.getAccessControlManager();
    context.checkAccess(query.alter ? AccessType::ALTER_QUOTA : AccessType::CREATE_QUOTA);

    if (!query.cluster.empty())
    {
        query.replaceCurrentUserTagWithName(context.getUserName());
        return executeDDLQueryOnCluster(query_ptr, context);
    }

    std::optional<ExtendedRoleSet> roles_from_query;
    if (query.roles)
        roles_from_query = ExtendedRoleSet{*query.roles, access_control, context.getUserID()};

    if (query.alter)
    {
        auto update_func = [&](const AccessEntityPtr & entity) -> AccessEntityPtr
        {
            auto updated_quota = typeid_cast<std::shared_ptr<Quota>>(entity->clone());
            updateQuotaFromQueryImpl(*updated_quota, query, roles_from_query);
            return updated_quota;
        };
        if (query.if_exists)
        {
            if (auto id = access_control.find<Quota>(query.name))
                access_control.tryUpdate(*id, update_func);
        }
        else
            access_control.update(access_control.getID<Quota>(query.name), update_func);
    }
    else
    {
        auto new_quota = std::make_shared<Quota>();
        updateQuotaFromQueryImpl(*new_quota, query, roles_from_query);

        if (query.if_not_exists)
            access_control.tryInsert(new_quota);
        else if (query.or_replace)
            access_control.insertOrReplace(new_quota);
        else
            access_control.insert(new_quota);
    }

    return {};
}


void InterpreterCreateQuotaQuery::updateQuotaFromQuery(Quota & quota, const ASTCreateQuotaQuery & query)
{
    updateQuotaFromQueryImpl(quota, query);
}

}
