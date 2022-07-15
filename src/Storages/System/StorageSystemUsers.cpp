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
#include <Poco/JSON/Stringifier.h>
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


NamesAndTypesList StorageSystemUsers::getNamesAndTypes()
{
    NamesAndTypesList names_and_types{
        {"name", std::make_shared<DataTypeString>()},
        {"id", std::make_shared<DataTypeUUID>()},
        {"storage", std::make_shared<DataTypeString>()},
        {"auth_type", std::make_shared<DataTypeEnum8>(getAuthenticationTypeEnumValues())},
        {"auth_params", std::make_shared<DataTypeString>()},
        {"host_ip", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"host_names", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"host_names_regexp", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"host_names_like", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"default_roles_all", std::make_shared<DataTypeUInt8>()},
        {"default_roles_list", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"default_roles_except", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"grantees_any", std::make_shared<DataTypeUInt8>()},
        {"grantees_list", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"grantees_except", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"default_database", std::make_shared<DataTypeString>()},
    };
    return names_and_types;
}


void StorageSystemUsers::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    context->checkAccess(AccessType::SHOW_USERS);
    const auto & access_control = context->getAccessControl();
    std::vector<UUID> ids = access_control.findAll<User>();

    size_t column_index = 0;
    auto & column_name = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_id = assert_cast<ColumnUUID &>(*res_columns[column_index++]).getData();
    auto & column_storage = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_auth_type = assert_cast<ColumnInt8 &>(*res_columns[column_index++]).getData();
    auto & column_auth_params = assert_cast<ColumnString &>(*res_columns[column_index++]);
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
                       const AuthenticationData & auth_data,
                       const AllowedClientHosts & allowed_hosts,
                       const RolesOrUsersSet & default_roles,
                       const RolesOrUsersSet & grantees,
                       const String default_database)
    {
        column_name.insertData(name.data(), name.length());
        column_id.push_back(id.toUnderType());
        column_storage.insertData(storage_name.data(), storage_name.length());
        column_auth_type.push_back(static_cast<Int8>(auth_data.getType()));

        if (auth_data.getType() == AuthenticationType::LDAP ||
            auth_data.getType() == AuthenticationType::KERBEROS ||
            auth_data.getType() == AuthenticationType::SSL_CERTIFICATE)
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
                Poco::JSON::Array::Ptr arr = new Poco::JSON::Array();
                for (const auto & common_name : auth_data.getSSLCertificateCommonNames())
                    arr->add(common_name);
                auth_params_json.set("common_names", arr);
            }

            std::ostringstream oss;         // STYLE_CHECK_ALLOW_STD_STRING_STREAM
            oss.exceptions(std::ios::failbit);
            Poco::JSON::Stringifier::stringify(auth_params_json, oss);
            const auto str = oss.str();

            column_auth_params.insertData(str.data(), str.size());
        }
        else
        {
            static constexpr std::string_view empty_json{"{}"};
            column_auth_params.insertData(empty_json.data(), empty_json.length());
        }

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

        add_row(user->getName(), id, storage->getStorageName(), user->auth_data, user->allowed_client_hosts,
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
    RestorerFromBackup & restorer, const String & /* data_path_in_backup */, const std::optional<ASTs> & /* partitions */)
{
    auto & access_control = restorer.getContext()->getAccessControl();
    access_control.restoreFromBackup(restorer);
}

}
