#include <Interpreters/InterpreterShowCreateUserQuery.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateUserQuery.h>
#include <Parsers/ASTShowCreateUserQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/formatAST.h>
#include <Access/AccessControlManager.h>
#include <Access/User2.h>
#include <Columns/ColumnString.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypeString.h>
#include <sstream>


namespace DB
{
BlockIO InterpreterShowCreateUserQuery::execute()
{
    BlockIO res;
    res.in = executeImpl();
    return res;
}


BlockInputStreamPtr InterpreterShowCreateUserQuery::executeImpl()
{
    auto create_query = getCreateUserQuery();
    std::stringstream stream;
    formatAST(*create_query, stream, false, true);

    MutableColumnPtr column = ColumnString::create();
    column->insert(stream.str());

    const auto & query = query_ptr->as<ASTShowCreateUserQuery &>();
    return std::make_shared<OneBlockInputStream>(
        Block{{std::move(column), std::make_shared<DataTypeString>(), "CREATE USER for " + query.user_name}});
}


ASTPtr InterpreterShowCreateUserQuery::getCreateUserQuery() const
{
    auto & manager = context.getAccessControlManager();
    auto user = manager.read<User2>(query_ptr->as<ASTShowCreateUserQuery &>().user_name);
    auto result = std::make_shared<ASTCreateUserQuery>();
    auto & query = *result;

    query.user_name = user->name;

    if (user->authentication.getType() != Authentication::NO_PASSWORD)
        query.authentication.emplace(user->authentication);

    query.allowed_hosts.emplace(user->allowed_hosts);

    {
        Strings default_roles;
        for (const auto & role_id : user->default_roles)
        {
            auto role_name = manager.tryReadName(role_id);
            if (role_name)
                default_roles.emplace_back(*role_name);
        }

        if (!default_roles.empty())
        {
            std::sort(default_roles.begin(), default_roles.end());
            query.default_roles.emplace();
            query.default_roles->role_names = std::move(default_roles);
        }
    }

    if (!user->settings.empty())
        query.settings.emplace(user->settings);
    if (!user->settings_constraints.empty())
        query.settings_constraints.emplace(user->settings_constraints);

    if (user->account_locked)
    {
        query.account_lock.emplace();
        query.account_lock->account_locked = user->account_locked;
    }

    return result;
}
}
