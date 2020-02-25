#include <Access/RoleContext.h>
#include <Access/Role.h>
#include <Access/CurrentRolesInfo.h>
#include <Access/AccessControlManager.h>
#include <boost/range/algorithm/copy.hpp>
#include <boost/range/algorithm/find.hpp>
#include <boost/range/algorithm/sort.hpp>


namespace DB
{
namespace
{
    void makeUnique(std::vector<UUID> & vec)
    {
        boost::range::sort(vec);
        vec.erase(std::unique(vec.begin(), vec.end()), vec.end());
    }
}


RoleContext::RoleContext(const AccessControlManager & manager_, const UUID & current_role_, bool with_admin_option_)
    : manager(&manager_), current_role(current_role_), with_admin_option(with_admin_option_)
{
    update();
}


RoleContext::RoleContext(std::vector<RoleContextPtr> && children_)
    : children(std::move(children_))
{
    update();
}


RoleContext::~RoleContext() = default;


void RoleContext::update()
{
    std::vector<OnChangeHandler> handlers_to_notify;
    CurrentRolesInfoPtr info_to_notify;

    {
        std::lock_guard lock{mutex};
        auto old_info = info;

        updateImpl();

        if (!handlers.empty() && (!old_info || (*old_info != *info)))
        {
            boost::range::copy(handlers, std::back_inserter(handlers_to_notify));
            info_to_notify = info;
        }
    }

    for (const auto & handler : handlers_to_notify)
        handler(info_to_notify);
}


void RoleContext::updateImpl()
{
    if (!current_role && children.empty())
    {
        info = std::make_shared<CurrentRolesInfo>();
        return;
    }

    if (!children.empty())
    {
        if (subscriptions_for_change_children.empty())
        {
            for (const auto & child : children)
                subscriptions_for_change_children.emplace_back(
                    child->subscribeForChanges([this](const CurrentRolesInfoPtr &) { update(); }));
        }

        auto new_info = std::make_shared<CurrentRolesInfo>();
        auto & new_info_ref = *new_info;

        for (const auto & child : children)
        {
            auto child_info = child->getInfo();
            new_info_ref.access.merge(child_info->access);
            new_info_ref.access_with_grant_option.merge(child_info->access_with_grant_option);
            boost::range::copy(child_info->current_roles, std::back_inserter(new_info_ref.current_roles));
            boost::range::copy(child_info->enabled_roles, std::back_inserter(new_info_ref.enabled_roles));
            boost::range::copy(child_info->enabled_roles_with_admin_option, std::back_inserter(new_info_ref.enabled_roles_with_admin_option));
            boost::range::copy(child_info->names_of_roles, std::inserter(new_info_ref.names_of_roles, new_info_ref.names_of_roles.end()));
        }
        makeUnique(new_info_ref.current_roles);
        makeUnique(new_info_ref.enabled_roles);
        makeUnique(new_info_ref.enabled_roles_with_admin_option);
        info = new_info;
        return;
    }

    assert(current_role);
    traverseRoles(*current_role, with_admin_option);

    auto new_info = std::make_shared<CurrentRolesInfo>();
    auto & new_info_ref = *new_info;

    for (auto it = roles_map.begin(); it != roles_map.end();)
    {
        const auto & id = it->first;
        auto & entry = it->second;
        if (!entry.in_use)
        {
            it = roles_map.erase(it);
            continue;
        }

        if (id == *current_role)
            new_info_ref.current_roles.push_back(id);

        new_info_ref.enabled_roles.push_back(id);

        if (entry.with_admin_option)
            new_info_ref.enabled_roles_with_admin_option.push_back(id);

        new_info_ref.access.merge(entry.role->access);
        new_info_ref.access_with_grant_option.merge(entry.role->access_with_grant_option);
        new_info_ref.names_of_roles[id] = entry.role->getName();

        entry.in_use = false;
        entry.with_admin_option = false;
        ++it;
    }

    info = new_info;
}


void RoleContext::traverseRoles(const UUID & id_, bool with_admin_option_)
{
    auto it = roles_map.find(id_);
    if (it == roles_map.end())
    {
        assert(manager);
        auto subscription = manager->subscribeForChanges(id_, [this, id_](const UUID &, const AccessEntityPtr & entity)
        {
            {
                std::lock_guard lock{mutex};
                auto it2 = roles_map.find(id_);
                if (it2 == roles_map.end())
                    return;
                if (entity)
                    it2->second.role = typeid_cast<RolePtr>(entity);
                else
                    roles_map.erase(it2);
            }
            update();
        });

        auto role = manager->tryRead<Role>(id_);
        if (!role)
            return;

        RoleEntry new_entry;
        new_entry.role = role;
        new_entry.subscription_for_change_role = std::move(subscription);
        it = roles_map.emplace(id_, std::move(new_entry)).first;
    }

    RoleEntry & entry = it->second;
    entry.with_admin_option |= with_admin_option_;
    if (entry.in_use)
        return;

    entry.in_use = true;
    for (const auto & granted_role : entry.role->granted_roles)
        traverseRoles(granted_role, false);

    for (const auto & granted_role : entry.role->granted_roles_with_admin_option)
        traverseRoles(granted_role, true);
}


CurrentRolesInfoPtr RoleContext::getInfo() const
{
    std::lock_guard lock{mutex};
    return info;
}


ext::scope_guard RoleContext::subscribeForChanges(const OnChangeHandler & handler) const
{
    std::lock_guard lock{mutex};
    handlers.push_back(handler);
    auto it = std::prev(handlers.end());

    return [this, it]
    {
        std::lock_guard lock2{mutex};
        handlers.erase(it);
    };
}
}
