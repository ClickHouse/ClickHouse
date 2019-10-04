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
#include <common/StringRef.h>
#include <map>
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

    {
        query.authentication.emplace();
        auto & qauth = *query.authentication;
        switch (user->password.getType())
        {
            case EncryptedPassword::NONE:
                qauth.type = ASTCreateUserQuery::Authentication::NO_PASSWORD;
                break;
            case EncryptedPassword::PLAINTEXT:
                qauth.type = ASTCreateUserQuery::Authentication::PLAINTEXT_PASSWORD;
                qauth.password = std::make_shared<ASTLiteral>(user->password.getHash());
                break;
            case EncryptedPassword::SHA256:
                qauth.type = ASTCreateUserQuery::Authentication::SHA256_HASH;
                qauth.password = std::make_shared<ASTLiteral>(user->password.getHashHex());
                break;
            default:
                __builtin_unreachable();
        }
    }

    {
        query.allowed_hosts.emplace();
        auto & qah = *query.allowed_hosts;
        for (const auto & host_name : user->allowed_hosts.getHostNames())
            qah.host_names.emplace_back(std::make_shared<ASTLiteral>(host_name));
        for (const auto & host_regexp : user->allowed_hosts.getHostRegexps())
            qah.host_regexps.emplace_back(std::make_shared<ASTLiteral>(host_regexp));
        for (const auto & ip_address : user->allowed_hosts.getIPAddresses())
            qah.ip_addresses.emplace_back(std::make_shared<ASTLiteral>(ip_address.toString()));
        for (const auto & ip_subnet : user->allowed_hosts.getIPSubnets())
            qah.ip_addresses.emplace_back(std::make_shared<ASTLiteral>(ip_subnet.toString()));
    }

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

    {
        std::map<String, ASTCreateUserQuery::Setting> settings_map;
        for (const auto & entry : user->settings)
            settings_map[entry.name].value = std::make_shared<ASTLiteral>(entry.value);

        for (const auto & constraint : user->settings_constraints.getInfo())
        {
            auto & out = settings_map[constraint.name.toString()];
            if (!constraint.min.isNull())
                out.min = std::make_shared<ASTLiteral>(constraint.min);
            if (!constraint.max.isNull())
                out.max = std::make_shared<ASTLiteral>(constraint.max);
            out.read_only = constraint.read_only;
        }

        if (!settings_map.empty())
        {
            query.settings.emplace();
            for (auto & [name, setting] : settings_map)
            {
                setting.name = name;
                query.settings->emplace_back(std::move(setting));
            }
        }
    }

    {
        query.account_lock.emplace();
        query.account_lock->locked = user->account_locked;
    }

    return result;
}
}
