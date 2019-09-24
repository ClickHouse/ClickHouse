#include <ACL/Role.h>
#include <ACL/IControlAttributesStorage.h>
#include <Parsers/ASTGrantQuery.h>
#include <common/StringRef.h>
#include <map>


namespace DB
{
namespace ErrorCodes
{
    extern const int ROLE_NOT_FOUND;
    extern const int ROLE_ALREADY_EXISTS;
}


namespace AccessControlNames
{
    extern const size_t ROLE_NAMESPACE_IDX;
}


const ConstRole::Type ConstRole::Attributes::TYPE = {"Role",
                                                     nullptr,
                                                     ErrorCodes::ROLE_NOT_FOUND,
                                                     ErrorCodes::ROLE_ALREADY_EXISTS,
                                                     AccessControlNames::ROLE_NAMESPACE_IDX};

const ConstRole::Type & ConstRole::TYPE = Role::Attributes::TYPE;


bool ConstRole::Attributes::equal(const IControlAttributes & other) const
{
    if (!IControlAttributes::equal(other))
        return false;
    const auto & o = *other.cast<Attributes>();
    return (allowed_databases_by_grant_option[false] == o.allowed_databases_by_grant_option[false])
        && (allowed_databases_by_grant_option[true] == o.allowed_databases_by_grant_option[true])
        && (granted_roles_by_admin_option[false] == o.granted_roles_by_admin_option[false])
        && (granted_roles_by_admin_option[true] == o.granted_roles_by_admin_option[true]);
}


ConstRole::AttributesPtr ConstRole::getAttributes() const
{
    return storage.read<Attributes>(id);
}


ConstRole::AttributesPtr ConstRole::tryGetAttributes() const
{
    return storage.tryRead<Attributes>(id);
}


std::vector<ASTPtr> ConstRole::getGrantQueries() const
{
    auto attrs = getAttributes();
    std::vector<ASTPtr> result;
    using Kind = ASTGrantQuery::Kind;

    for (bool grant_option : {false, true})
    {
        std::map<std::pair<StringRef /* database */, StringRef /* table */>, std::shared_ptr<ASTGrantQuery>> grants;
        std::map<std::pair<StringRef /* database */, StringRef /* table */>, std::shared_ptr<ASTGrantQuery>> partial_revokes;

        for (const auto & info : attrs->allowed_databases_by_grant_option[grant_option].getInfo())
        {
            for (Kind kind : {Kind::GRANT, Kind::REVOKE})
            {
                auto access = (kind == Kind::GRANT) ? info.grants : info.partial_revokes;
                if (!access)
                    continue;

                auto & map = (kind == Kind::GRANT) ? grants : partial_revokes;
                auto it = map.find({info.database, info.table});
                if (it == map.end())
                {
                    auto new_query = std::make_shared<ASTGrantQuery>();
                    new_query->to_roles.emplace_back(attrs->name);
                    new_query->database = info.database;
                    new_query->table = info.table;
                    new_query->kind = kind;
                    new_query->grant_option = grant_option;
                    it = map.try_emplace({new_query->database, new_query->table}, new_query).first;
                }

                auto & query = *(it->second);
                if (info.column.empty())
                    query.access |= access;
                else
                    query.columns_access[info.column] |= access;
            }
        }

        for (Kind kind : {Kind::GRANT, Kind::REVOKE})
        {
            const auto & map = (kind == Kind::GRANT) ? grants : partial_revokes;
            for (const auto & key_with_query : map)
            {
                const auto query = key_with_query.second;
                result.push_back(query);
            }
        }
    }

    for (bool grant_option : {false, true})
    {
        std::vector<String> granted_roles;
        for (const UUID & granted_role_id : attrs->granted_roles_by_admin_option[grant_option])
        {
            auto granted_role_attrs = storage.tryRead(granted_role_id);
            if (!granted_role_attrs)
                continue;
            granted_roles.emplace_back(granted_role_attrs->name);
        }

        if (!granted_roles.empty())
        {
            auto new_query = std::make_shared<ASTGrantQuery>();
            new_query->to_roles.emplace_back(attrs->name);
            new_query->kind = Kind::GRANT;
            new_query->grant_option = grant_option;
            new_query->roles = std::move(granted_roles);
            result.push_back(std::move(new_query));
        }
    }
    return result;
}


void Role::update(const std::function<void(Attributes &)> & update_func)
{
    getStorage().update(id, update_func);
}


void Role::drop(bool if_exists)
{
    if (if_exists)
         getStorage().tryRemove(id);
    else
         getStorage().remove(id, TYPE);
}
}
