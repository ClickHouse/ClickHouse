#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/Access/InterpreterCreateQuotaQuery.h>

#include <Access/AccessControl.h>
#include <Access/Common/AccessFlags.h>
#include <Access/Quota.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Parsers/Access/ASTCreateQuotaQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <base/range.h>
#include <boost/range/algorithm/find_if.hpp>
#include <boost/range/algorithm/sort.hpp>
#include <boost/range/algorithm/upper_bound.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int ACCESS_ENTITY_ALREADY_EXISTS;
}

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
            for (auto quota_type : collections::range(QuotaType::MAX))
            {
                auto quota_type_i = static_cast<size_t>(quota_type);
                quota_limits.max[quota_type_i] = query_limits.max[quota_type_i];
            }
        }

        if (override_to_roles)
            quota.to_roles = *override_to_roles;
        else if (query.roles)
            quota.to_roles = *query.roles;
    }
}


BlockIO InterpreterCreateQuotaQuery::execute()
{
    const auto updated_query_ptr = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    auto & query = updated_query_ptr->as<ASTCreateQuotaQuery &>();

    auto & access_control = getContext()->getAccessControl();
    getContext()->checkAccess(query.alter ? AccessType::ALTER_QUOTA : AccessType::CREATE_QUOTA);

    if (!query.cluster.empty())
    {
        query.replaceCurrentUserTag(getContext()->getUserName());
        return executeDDLQueryOnCluster(updated_query_ptr, getContext());
    }

    std::optional<RolesOrUsersSet> roles_from_query;
    if (query.roles)
        roles_from_query = RolesOrUsersSet{*query.roles, access_control, getContext()->getUserID()};

    IAccessStorage * storage = &access_control;
    MultipleAccessStorage::StoragePtr storage_ptr;

    if (!query.storage_name.empty())
    {
        storage_ptr = access_control.getStorageByName(query.storage_name);
        storage = storage_ptr.get();
    }

    if (query.alter)
    {
        auto update_func = [&](const AccessEntityPtr & entity, const UUID &) -> AccessEntityPtr
        {
            auto updated_quota = typeid_cast<std::shared_ptr<Quota>>(entity->clone());
            updateQuotaFromQueryImpl(*updated_quota, query, {}, roles_from_query);
            return updated_quota;
        };
        if (query.if_exists)
        {
            auto ids = storage->find<Quota>(query.names);
            storage->tryUpdate(ids, update_func);
        }
        else
            storage->update(storage->getIDs<Quota>(query.names), update_func);
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

        if (!query.storage_name.empty())
        {
            for (const auto & name : query.names)
            {
                if (auto another_storage_ptr = access_control.findExcludingStorage(AccessEntityType::QUOTA, name, storage_ptr))
                    throw Exception(ErrorCodes::ACCESS_ENTITY_ALREADY_EXISTS, "Quota {} already exists in storage {}", name, another_storage_ptr->getStorageName());
            }
        }

        if (query.if_not_exists)
            storage->tryInsert(new_quotas);
        else if (query.or_replace)
            storage->insertOrReplace(new_quotas);
        else
            storage->insert(new_quotas);
    }

    return {};
}


void InterpreterCreateQuotaQuery::updateQuotaFromQuery(Quota & quota, const ASTCreateQuotaQuery & query)
{
    updateQuotaFromQueryImpl(quota, query, {}, {});
}

void registerInterpreterCreateQuotaQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterCreateQuotaQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterCreateQuotaQuery", create_fn);
}

}
