#include <Interpreters/InterpreterCreateQuotaQuery.h>
#include <Parsers/ASTCreateQuotaQuery.h>
#include <Parsers/ASTRolesOrUsersSet.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Access/AccessControlManager.h>
#include <Access/AccessFlags.h>
#include <common/range.h>
#include <boost/range/algorithm/find_if.hpp>
#include <boost/range/algorithm/upper_bound.hpp>
#include <boost/range/algorithm/sort.hpp>


namespace DB
{
namespace
{
    void updateQuotaFromQueryImpl(
        Quota & quota,
        const ASTCreateQuotaQuery & query,
        const String & override_name,
        const std::optional<RolesOrUsersSet> & override_to_roles)
    {
        if (!override_name.empty())
            quota.setName(override_name);
        else if (!query.new_name.empty())
            quota.setName(query.new_name);
        else if (query.names.size() == 1)
            quota.setName(query.names.front());

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
            for (auto resource_type : collections::range(Quota::MAX_RESOURCE_TYPE))
                quota_limits.max[resource_type] = query_limits.max[resource_type];
        }

        if (override_to_roles)
            quota.to_roles = *override_to_roles;
        else if (query.roles)
            quota.to_roles = *query.roles;
    }
}


BlockIO InterpreterCreateQuotaQuery::execute()
{
    auto & query = query_ptr->as<ASTCreateQuotaQuery &>();
    auto & access_control = getContext()->getAccessControlManager();
    getContext()->checkAccess(query.alter ? AccessType::ALTER_QUOTA : AccessType::CREATE_QUOTA);

    if (!query.cluster.empty())
    {
        query.replaceCurrentUserTag(getContext()->getUserName());
        return executeDDLQueryOnCluster(query_ptr, getContext());
    }

    std::optional<RolesOrUsersSet> roles_from_query;
    if (query.roles)
        roles_from_query = RolesOrUsersSet{*query.roles, access_control, getContext()->getUserID()};

    if (query.alter)
    {
        auto update_func = [&](const AccessEntityPtr & entity) -> AccessEntityPtr
        {
            auto updated_quota = typeid_cast<std::shared_ptr<Quota>>(entity->clone());
            updateQuotaFromQueryImpl(*updated_quota, query, {}, roles_from_query);
            return updated_quota;
        };
        if (query.if_exists)
        {
            auto ids = access_control.find<Quota>(query.names);
            access_control.tryUpdate(ids, update_func);
        }
        else
            access_control.update(access_control.getIDs<Quota>(query.names), update_func);
    }
    else
    {
        std::vector<AccessEntityPtr> new_quotas;
        for (const String & name : query.names)
        {
            auto new_quota = std::make_shared<Quota>();
            updateQuotaFromQueryImpl(*new_quota, query, name, roles_from_query);
            new_quotas.emplace_back(std::move(new_quota));
        }

        if (query.if_not_exists)
            access_control.tryInsert(new_quotas);
        else if (query.or_replace)
            access_control.insertOrReplace(new_quotas);
        else
            access_control.insert(new_quotas);
    }

    return {};
}


void InterpreterCreateQuotaQuery::updateQuotaFromQuery(Quota & quota, const ASTCreateQuotaQuery & query)
{
    updateQuotaFromQueryImpl(quota, query, {}, {});
}

}
