#include <Storages/System/StorageSystemUsers.h>
#include <Access/AccessControl.h>
#include <Access/Common/AccessFlags.h>
#include <Access/User.h>
#include <Backups/BackupEntriesCollector.h>
#include <Backups/RestorerFromBackup.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeEnum.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/Context.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Stringifier.h>
#include <Poco/JSONString.h>

#include <Access/Common/SSLCertificateSubjects.h>

#include <base/types.h>
#include <base/range.h>

#include <sstream>


namespace DB
{
namespace
{
    DataTypeEnum8::Values getAuthenticationTypeEnumValues()
    {
        DataTypeEnum8::Values enum_values;
        for (auto type : collections::range(AuthenticationType::MAX))
            enum_values.emplace_back(AuthenticationTypeInfo::get(type).name, static_cast<Int8>(type));
        return enum_values;
    }
}


ColumnsDescription StorageSystemUsers::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "User name."},
        {"id", std::make_shared<DataTypeUUID>(), "User ID."},
        {"storage", std::make_shared<DataTypeString>(), "Path to the storage of users. Configured in the access_control_path parameter."},
        {"auth_type", std::make_shared<DataTypeArray>(std::make_shared<DataTypeEnum8>(getAuthenticationTypeEnumValues())),
            "Shows the authentication types. "
            "There are multiple ways of user identification: "
            "with no password, with plain text password, with SHA256-encoded password, "
            "with double SHA-1-encoded password or with bcrypt-encoded password."
        },
        {"auth_params", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
            "Authentication parameters in the JSON format depending on the auth_type."
        },
        {"host_ip", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
            "IP addresses of hosts that are allowed to connect to the ClickHouse server."
        },
        {"host_names", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
            "Names of hosts that are allowed to connect to the ClickHouse server."
        },
        {"host_names_regexp", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
            "Regular expression for host names that are allowed to connect to the ClickHouse server."
        },
        {"host_names_like", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
            "Names of hosts that are allowed to connect to the ClickHouse server, set using the LIKE predicate."
        },
        {"default_roles_all", std::make_shared<DataTypeUInt8>(),
            "Shows that all granted roles set for user by default."
        },
        {"default_roles_list", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
            "List of granted roles provided by default."
        },
        {"default_roles_except", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
            "All the granted roles set as default excepting of the listed ones."
        },
        {"grantees_any", std::make_shared<DataTypeUInt8>(), "The flag that indicates whether a user with any grant option can grant it to anyone."},
        {"grantees_list", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "The list of users or roles to which this user is allowed to grant options to."},
        {"grantees_except", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "The list of users or roles to which this user is forbidden from grant options to."},
        {"default_database", std::make_shared<DataTypeString>(), "The name of the default database for this user."},
    };
}


void StorageSystemUsers::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    /// If "select_from_system_db_requires_grant" is enabled the access rights were already checked in InterpreterSelectQuery.
    const auto & access_control = context->getAccessControl();
    if (!access_control.doesSelectFromSystemDatabaseRequireGrant())
        context->checkAccess(AccessType::SHOW_USERS);

    std::vector<UUID> ids = access_control.findAll<User>();

    size_t column_index = 0;
    auto & column_name = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_id = assert_cast<ColumnUUID &>(*res_columns[column_index++]).getData();
    auto & column_storage = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_auth_type = assert_cast<ColumnInt8 &>(assert_cast<ColumnArray &>(*res_columns[column_index]).getData());
    auto & column_auth_type_offsets =  assert_cast<ColumnArray &>(*res_columns[column_index++]).getOffsets();
    auto & column_auth_params = assert_cast<ColumnString &>(assert_cast<ColumnArray &>(*res_columns[column_index]).getData());
    auto & column_auth_params_offsets = assert_cast<ColumnArray &>(*res_columns[column_index++]).getOffsets();
    auto & column_host_ip = assert_cast<ColumnString &>(assert_cast<ColumnArray &>(*res_columns[column_index]).getData());
    auto & column_host_ip_offsets = assert_cast<ColumnArray &>(*res_columns[column_index++]).getOffsets();
    auto & column_host_names = assert_cast<ColumnString &>(assert_cast<ColumnArray &>(*res_columns[column_index]).getData());
    auto & column_host_names_offsets = assert_cast<ColumnArray &>(*res_columns[column_index++]).getOffsets();
    auto & column_host_names_regexp = assert_cast<ColumnString &>(assert_cast<ColumnArray &>(*res_columns[column_index]).getData());
    auto & column_host_names_regexp_offsets = assert_cast<ColumnArray &>(*res_columns[column_index++]).getOffsets();
    auto & column_host_names_like = assert_cast<ColumnString &>(assert_cast<ColumnArray &>(*res_columns[column_index]).getData());
    auto & column_host_names_like_offsets = assert_cast<ColumnArray &>(*res_columns[column_index++]).getOffsets();
    auto & column_default_roles_all = assert_cast<ColumnUInt8 &>(*res_columns[column_index++]).getData();
    auto & column_default_roles_list = assert_cast<ColumnString &>(assert_cast<ColumnArray &>(*res_columns[column_index]).getData());
    auto & column_default_roles_list_offsets = assert_cast<ColumnArray &>(*res_columns[column_index++]).getOffsets();
    auto & column_default_roles_except = assert_cast<ColumnString &>(assert_cast<ColumnArray &>(*res_columns[column_index]).getData());
    auto & column_default_roles_except_offsets = assert_cast<ColumnArray &>(*res_columns[column_index++]).getOffsets();
    auto & column_grantees_any = assert_cast<ColumnUInt8 &>(*res_columns[column_index++]).getData();
    auto & column_grantees_list = assert_cast<ColumnString &>(assert_cast<ColumnArray &>(*res_columns[column_index]).getData());
    auto & column_grantees_list_offsets = assert_cast<ColumnArray &>(*res_columns[column_index++]).getOffsets();
    auto & column_grantees_except = assert_cast<ColumnString &>(assert_cast<ColumnArray &>(*res_columns[column_index]).getData());
    auto & column_grantees_except_offsets = assert_cast<ColumnArray &>(*res_columns[column_index++]).getOffsets();
    auto & column_default_database = assert_cast<ColumnString &>(*res_columns[column_index++]);

    auto add_row = [&](const String & name,
                       const UUID & id,
                       const String & storage_name,
                       const std::vector<AuthenticationData> & authentication_methods,
                       const AllowedClientHosts & allowed_hosts,
                       const RolesOrUsersSet & default_roles,
                       const RolesOrUsersSet & grantees,
                       const String default_database)
    {
        column_name.insertData(name.data(), name.length());
        column_id.push_back(id.toUnderType());
        column_storage.insertData(storage_name.data(), storage_name.length());

        for (const auto & auth_data : authentication_methods)
        {
            Poco::JSON::Object auth_params_json;

            if (auth_data.getType() == AuthenticationType::LDAP)
            {
                auth_params_json.set("server", auth_data.getLDAPServerName());
            }
            else if (auth_data.getType() == AuthenticationType::KERBEROS)
            {
                auth_params_json.set("realm", auth_data.getKerberosRealm());
            }
            else if (auth_data.getType() == AuthenticationType::SSL_CERTIFICATE)
            {
                Poco::JSON::Array::Ptr common_names = new Poco::JSON::Array();
                Poco::JSON::Array::Ptr subject_alt_names = new Poco::JSON::Array();

                const auto & subjects = auth_data.getSSLCertificateSubjects();
                for (const String & subject : subjects.at(SSLCertificateSubjects::Type::CN))
                    common_names->add(subject);
                for (const String & subject : subjects.at(SSLCertificateSubjects::Type::SAN))
                    subject_alt_names->add(subject);

                if (common_names->size() > 0)
                    auth_params_json.set("common_names", common_names);
                if (subject_alt_names->size() > 0)
                    auth_params_json.set("subject_alt_names", subject_alt_names);
            }

            std::ostringstream oss;         // STYLE_CHECK_ALLOW_STD_STRING_STREAM
            oss.exceptions(std::ios::failbit);
            Poco::JSON::Stringifier::stringify(auth_params_json, oss);
            const auto authentication_params_str = oss.str();

            column_auth_params.insertData(authentication_params_str.data(), authentication_params_str.size());
            column_auth_type.insertValue(static_cast<Int8>(auth_data.getType()));
        }

        column_auth_params_offsets.push_back(column_auth_params.size());
        column_auth_type_offsets.push_back(column_auth_type.size());

        if (allowed_hosts.containsAnyHost())
        {
            static constexpr std::string_view str{"::/0"};
            column_host_ip.insertData(str.data(), str.length());
        }
        else
        {
            if (allowed_hosts.containsLocalHost())
            {
                static constexpr std::string_view str{"localhost"};
                column_host_names.insertData(str.data(), str.length());
            }

            for (const auto & ip : allowed_hosts.getAddresses())
            {
                String str = ip.toString();
                column_host_ip.insertData(str.data(), str.length());
            }
            for (const auto & subnet : allowed_hosts.getSubnets())
            {
                String str = subnet.toString();
                column_host_ip.insertData(str.data(), str.length());
            }

            for (const auto & host_name : allowed_hosts.getNames())
                column_host_names.insertData(host_name.data(), host_name.length());

            for (const auto & name_regexp : allowed_hosts.getNameRegexps())
                column_host_names_regexp.insertData(name_regexp.data(), name_regexp.length());

            for (const auto & like_pattern : allowed_hosts.getLikePatterns())
                column_host_names_like.insertData(like_pattern.data(), like_pattern.length());
        }

        column_host_ip_offsets.push_back(column_host_ip.size());
        column_host_names_offsets.push_back(column_host_names.size());
        column_host_names_regexp_offsets.push_back(column_host_names_regexp.size());
        column_host_names_like_offsets.push_back(column_host_names_like.size());

        auto default_roles_ast = default_roles.toASTWithNames(access_control);
        column_default_roles_all.push_back(default_roles_ast->all);
        for (const auto & role_name : default_roles_ast->names)
            column_default_roles_list.insertData(role_name.data(), role_name.length());
        column_default_roles_list_offsets.push_back(column_default_roles_list.size());
        for (const auto & except_name : default_roles_ast->except_names)
            column_default_roles_except.insertData(except_name.data(), except_name.length());
        column_default_roles_except_offsets.push_back(column_default_roles_except.size());

        auto grantees_ast = grantees.toASTWithNames(access_control);
        column_grantees_any.push_back(grantees_ast->all);
        for (const auto & grantee_name : grantees_ast->names)
            column_grantees_list.insertData(grantee_name.data(), grantee_name.length());
        column_grantees_list_offsets.push_back(column_grantees_list.size());
        for (const auto & except_name : grantees_ast->except_names)
            column_grantees_except.insertData(except_name.data(), except_name.length());
        column_grantees_except_offsets.push_back(column_grantees_except.size());

        column_default_database.insertData(default_database.data(),default_database.length());
    };

    for (const auto & id : ids)
    {
        auto user = access_control.tryRead<User>(id);
        if (!user)
            continue;

        auto storage = access_control.findStorage(id);
        if (!storage)
            continue;

        add_row(user->getName(), id, storage->getStorageName(), user->authentication_methods, user->allowed_client_hosts,
                user->default_roles, user->grantees, user->default_database);
    }
}

void StorageSystemUsers::backupData(
    BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> & /* partitions */)
{
    const auto & access_control = backup_entries_collector.getContext()->getAccessControl();
    access_control.backup(backup_entries_collector, data_path_in_backup, AccessEntityType::USER);
}

void StorageSystemUsers::restoreDataFromBackup(
    RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> & /* partitions */)
{
    auto & access_control = restorer.getContext()->getAccessControl();
    access_control.restoreFromBackup(restorer, data_path_in_backup);
}

}
