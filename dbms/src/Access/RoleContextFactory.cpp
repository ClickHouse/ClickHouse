#include <Access/RoleContextFactory.h>
#include <boost/container/flat_set.hpp>


namespace DB
{

RoleContextFactory::RoleContextFactory(const AccessControlManager & manager_)
    : manager(manager_), cache(600000 /* 10 minutes */) {}


RoleContextFactory::~RoleContextFactory() = default;


RoleContextPtr RoleContextFactory::createContext(
    const std::vector<UUID> & roles, const std::vector<UUID> & roles_with_admin_option)
{
    if (roles.size() == 1 && roles_with_admin_option.empty())
        return createContextImpl(roles[0], false);

    if (roles.size() == 1 && roles_with_admin_option == roles)
        return createContextImpl(roles[0], true);

    std::vector<RoleContextPtr> children;
    children.reserve(roles.size());
    for (const auto & role : roles_with_admin_option)
        children.push_back(createContextImpl(role, true));

    boost::container::flat_set<UUID> roles_with_admin_option_set{roles_with_admin_option.begin(), roles_with_admin_option.end()};
    for (const auto & role : roles)
    {
        if (!roles_with_admin_option_set.contains(role))
            children.push_back(createContextImpl(role, false));
    }

    return ext::shared_ptr_helper<RoleContext>::create(std::move(children));
}


RoleContextPtr RoleContextFactory::createContextImpl(const UUID & id, bool with_admin_option)
{
    std::lock_guard lock{mutex};
    auto key = std::make_pair(id, with_admin_option);
    auto x = cache.get(key);
    if (x)
        return *x;
    auto res = ext::shared_ptr_helper<RoleContext>::create(manager, id, with_admin_option);
    cache.add(key, res);
    return res;
}

}
