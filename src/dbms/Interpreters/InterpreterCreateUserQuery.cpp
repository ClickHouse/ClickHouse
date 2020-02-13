#include <Interpreters/InterpreterCreateUserQuery.h>
#include <Parsers/ASTCreateUserQuery.h>
#include <Interpreters/Context.h>
#include <Access/AccessControlManager.h>
#include <Access/User.h>


namespace DB
{
BlockIO InterpreterCreateUserQuery::execute()
{
    const auto & query = query_ptr->as<const ASTCreateUserQuery &>();
    auto & access_control = context.getAccessControlManager();
    context.checkAccess(query.alter ? AccessType::ALTER_USER : AccessType::CREATE_USER);

    if (query.alter)
    {
        auto update_func = [&](const AccessEntityPtr & entity) -> AccessEntityPtr
        {
            auto updated_user = typeid_cast<std::shared_ptr<User>>(entity->clone());
            updateUserFromQuery(*updated_user, query);
            return updated_user;
        };
        if (query.if_exists)
        {
            if (auto id = access_control.find<User>(query.name))
                access_control.tryUpdate(*id, update_func);
        }
        else
            access_control.update(access_control.getID<User>(query.name), update_func);
    }
    else
    {
        auto new_user = std::make_shared<User>();
        updateUserFromQuery(*new_user, query);

        if (query.if_not_exists)
            access_control.tryInsert(new_user);
        else if (query.or_replace)
            access_control.insertOrReplace(new_user);
        else
            access_control.insert(new_user);
    }

    return {};
}


void InterpreterCreateUserQuery::updateUserFromQuery(User & user, const ASTCreateUserQuery & query)
{
    if (query.alter)
    {
        if (!query.new_name.empty())
            user.setName(query.new_name);
    }
    else
        user.setName(query.name);

    if (query.authentication)
        user.authentication = *query.authentication;

    if (query.hosts)
        user.allowed_client_hosts = *query.hosts;
    if (query.remove_hosts)
        user.allowed_client_hosts.remove(*query.remove_hosts);
    if (query.add_hosts)
        user.allowed_client_hosts.add(*query.add_hosts);

    if (query.profile)
        user.profile = *query.profile;
}
}
