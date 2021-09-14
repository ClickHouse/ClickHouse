#include <Access/EnabledRoles.h>
#include <Access/Role.h>
#include <Access/EnabledRolesInfo.h>
#include <boost/range/algorithm/copy.hpp>


namespace DB
{
EnabledRoles::EnabledRoles(const Params & params_) : params(params_)
{
}

EnabledRoles::~EnabledRoles() = default;


std::shared_ptr<const EnabledRolesInfo> EnabledRoles::getRolesInfo() const
{
    std::lock_guard lock{mutex};
    return info;
}


ext::scope_guard EnabledRoles::subscribeForChanges(const OnChangeHandler & handler) const
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


void EnabledRoles::setRolesInfo(const std::shared_ptr<const EnabledRolesInfo> & info_, ext::scope_guard & notifications)
{
    std::lock_guard lock{mutex};

    if (info && info_ && *info == *info_)
        return;

    info = info_;

    std::vector<OnChangeHandler> handlers_to_notify;
    boost::range::copy(handlers, std::back_inserter(handlers_to_notify));

    notifications.join(ext::scope_guard([info = info, handlers_to_notify = std::move(handlers_to_notify)]
    {
        for (const auto & handler : handlers_to_notify)
            handler(info);
    }));
}

}
