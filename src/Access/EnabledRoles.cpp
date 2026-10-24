#include <Access/EnabledRoles.h>
#include <Access/Role.h>
#include <Access/EnabledRolesInfo.h>
#include <boost/range/algorithm/copy.hpp>


namespace DB
{
EnabledRoles::EnabledRoles(const Params & params_) : params(params_), handlers(std::make_shared<Handlers>())
{
}

EnabledRoles::~EnabledRoles() = default;


std::shared_ptr<const EnabledRolesInfo> EnabledRoles::getRolesInfo() const
{
    std::lock_guard lock{info_mutex};
    return info;
}


EnabledRoles::Subscription EnabledRoles::subscribeForChanges(
    const OnChangeHandler & handler, const std::optional<UUID> & cache_user_id) const
{
    std::lock_guard lock{handlers->access_cache.getMutex()};
    auto access_cache = cache_user_id ? handlers->access_cache.registerContext(*cache_user_id) : EffectiveAccessRightsCache::Handle{};
    handlers->list.push_back(handler);
    auto it = std::prev(handlers->list.end());

    scope_guard unsubscribe = [my_handlers = handlers, it]
    {
        std::lock_guard lock2{my_handlers->mutex};
        my_handlers->list.erase(it);
    };
    return {std::move(unsubscribe), std::move(access_cache)};
}


void EnabledRoles::setRolesInfo(const std::shared_ptr<const EnabledRolesInfo> & info_, scope_guard * notifications)
{
    {
        std::lock_guard lock{info_mutex};
        if (info && info_ && *info == *info_)
            return;

        info = info_;
    }

    if (notifications)
    {
        std::vector<OnChangeHandler> handlers_to_notify;
        {
            std::lock_guard lock{handlers->mutex};
            boost::range::copy(handlers->list, std::back_inserter(handlers_to_notify));
        }

        notifications->join(scope_guard(
            [my_info = info, my_handlers_to_notify = std::move(handlers_to_notify)]
            {
                for (const auto & handler : my_handlers_to_notify)
                    handler(my_info);
            }));
    }
}

}
