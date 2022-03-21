#pragma once

#include <Core/UUID.h>
#include <base/scope_guard.h>
#include <boost/container/flat_set.hpp>
#include <list>
#include <mutex>
#include <vector>


namespace DB
{
struct EnabledRolesInfo;

class EnabledRoles
{
public:
    struct Params
    {
        boost::container::flat_set<UUID> current_roles;
        boost::container::flat_set<UUID> current_roles_with_admin_option;

        auto toTuple() const { return std::tie(current_roles, current_roles_with_admin_option); }
        friend bool operator ==(const Params & lhs, const Params & rhs) { return lhs.toTuple() == rhs.toTuple(); }
        friend bool operator !=(const Params & lhs, const Params & rhs) { return !(lhs == rhs); }
        friend bool operator <(const Params & lhs, const Params & rhs) { return lhs.toTuple() < rhs.toTuple(); }
        friend bool operator >(const Params & lhs, const Params & rhs) { return rhs < lhs; }
        friend bool operator <=(const Params & lhs, const Params & rhs) { return !(rhs < lhs); }
        friend bool operator >=(const Params & lhs, const Params & rhs) { return !(lhs < rhs); }
    };

    ~EnabledRoles();

    /// Returns all the roles specified in the constructor.
    std::shared_ptr<const EnabledRolesInfo> getRolesInfo() const;

    using OnChangeHandler = std::function<void(const std::shared_ptr<const EnabledRolesInfo> & info)>;

    /// Called when either the specified roles or the roles granted to the specified roles are changed.
    scope_guard subscribeForChanges(const OnChangeHandler & handler) const;

private:
    friend class RoleCache;
    explicit EnabledRoles(const Params & params_);

    void setRolesInfo(const std::shared_ptr<const EnabledRolesInfo> & info_, scope_guard & notifications);

    const Params params;
    mutable std::shared_ptr<const EnabledRolesInfo> info;
    mutable std::list<OnChangeHandler> handlers;
    mutable std::mutex mutex;
};

}
