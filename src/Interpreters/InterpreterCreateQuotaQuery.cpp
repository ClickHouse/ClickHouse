#include <Interpreters/InterpreterCreateQuotaQuery.h>
#include <Parsers/ASTCreateQuotaQuery.h>
#include <Parsers/ASTRoleList.h>
#include <Interpreters/Context.h>
#include <Access/AccessControlManager.h>
#include <ext/range.h>
#include <boost/range/algorithm/find_if.hpp>
#include <boost/range/algorithm/upper_bound.hpp>
#include <boost/range/algorithm/sort.hpp>


namespace DB
{
BlockIO InterpreterCreateQuotaQuery::execute()
{
    context.checkQuotaManagementIsAllowed();
    const auto & query = query_ptr->as<const ASTCreateQuotaQuery &>();
    auto & access_control = context.getAccessControlManager();

    if (query.alter)
    {
        auto update_func = [&](const AccessEntityPtr & entity) -> AccessEntityPtr
        {
            auto updated_quota = typeid_cast<std::shared_ptr<Quota>>(entity->clone());
            updateQuotaFromQuery(*updated_quota, query);
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
        updateQuotaFromQuery(*new_quota, query);

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
        if (query_limits.unset_tracking)
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
        {
            if (query_limits.max[resource_type])
                quota_limits.max[resource_type] = *query_limits.max[resource_type];
        }
    }

    if (query.roles)
    {
        const auto & query_roles = *query.roles;

        /// We keep `roles` sorted.
        quota.roles = query_roles.roles;
        if (query_roles.current_user)
            quota.roles.push_back(context.getClientInfo().current_user);
        boost::range::sort(quota.roles);
        quota.roles.erase(std::unique(quota.roles.begin(), quota.roles.end()), quota.roles.end());

        quota.all_roles = query_roles.all_roles;

        /// We keep `except_roles` sorted.
        quota.except_roles = query_roles.except_roles;
        if (query_roles.except_current_user)
            quota.except_roles.push_back(context.getClientInfo().current_user);
        boost::range::sort(quota.except_roles);
        quota.except_roles.erase(std::unique(quota.except_roles.begin(), quota.except_roles.end()), quota.except_roles.end());
    }
}
}
