#pragma once

#include <Core/UUID.h>
#include <ext/scope_guard.h>
#include <ext/shared_ptr_helper.h>
#include <list>
#include <mutex>
#include <unordered_map>
#include <vector>


namespace DB
{
struct Role;
using RolePtr = std::shared_ptr<const Role>;
struct CurrentRolesInfo;
using CurrentRolesInfoPtr = std::shared_ptr<const CurrentRolesInfo>;
class AccessControlManager;


class RoleContext
{
public:
    ~RoleContext();

    /// Returns all the roles specified in the constructor.
    CurrentRolesInfoPtr getInfo() const;

    using OnChangeHandler = std::function<void(const CurrentRolesInfoPtr & info)>;

    /// Called when either the specified roles or the roles granted to the specified roles are changed.
    ext::scope_guard subscribeForChanges(const OnChangeHandler & handler) const;

private:
    friend struct ext::shared_ptr_helper<RoleContext>;
    RoleContext(const AccessControlManager & manager_, const UUID & current_role_, bool with_admin_option_);
    RoleContext(std::vector<std::shared_ptr<const RoleContext>> && children_);

    void update();
    void updateImpl();

    void traverseRoles(const UUID & id_, bool with_admin_option_);

    const AccessControlManager * manager = nullptr;
    std::optional<UUID> current_role;
    bool with_admin_option = false;
    std::vector<std::shared_ptr<const RoleContext>> children;
    std::vector<ext::scope_guard> subscriptions_for_change_children;

    struct RoleEntry
    {
        RolePtr role;
        ext::scope_guard subscription_for_change_role;
        bool with_admin_option = false;
        bool in_use = false;
    };
    mutable std::unordered_map<UUID, RoleEntry> roles_map;
    mutable CurrentRolesInfoPtr info;
    mutable std::list<OnChangeHandler> handlers;
    mutable std::mutex mutex;
};

using RoleContextPtr = std::shared_ptr<const RoleContext>;
}
