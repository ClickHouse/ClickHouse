#include <Access/IAccessStorage.h>
#include <Parsers/Access/ParserCreateUserQuery.h>
#include <Parsers/Access/ASTCreateUserQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/Access/ASTSettingsProfileElement.h>
#include <Parsers/Access/ASTUserNameWithHost.h>
#include <Parsers/Access/ASTAuthenticationData.h>
#include <Parsers/Access/ParserRolesOrUsersSet.h>
#include <Parsers/Access/ParserSettingsProfileElement.h>
#include <Parsers/Access/ParserUserNameWithHost.h>
#include <Parsers/Access/ParserPublicSSHKey.h>
#include <Parsers/Access/parseUserName.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserDatabaseOrNone.h>
#include <Parsers/ParserStringAndSubstitution.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Parsers/StatementFactory.h>
#include <Parsers/registerStatements.h>

#include <base/range.h>
#include <base/insertAtEnd.h>

#include "config.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{
    bool parseRenameTo(IParserBase::Pos & pos, Expected & expected, std::optional<String> & new_name)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{Keyword::RENAME_TO}.ignore(pos, expected))
                return false;

            String maybe_new_name;
            if (!parseUserName(pos, expected, maybe_new_name, /*allow_query_parameter=*/true))
                return false;

            new_name.emplace(std::move(maybe_new_name));
            return true;
        });
    }

    bool parseValidUntil(IParserBase::Pos & pos, Expected & expected, ASTPtr & valid_until)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{Keyword::VALID_UNTIL}.ignore(pos, expected))
                return false;

            ParserStringAndSubstitution until_p;

            return until_p.parse(pos, valid_until, expected);
        });
    }

    bool parseAuthenticationData(
        IParserBase::Pos & pos,
        Expected & expected,
        boost::intrusive_ptr<ASTAuthenticationData> & auth_data,
        bool is_type_specifier_mandatory,
        bool is_type_specifier_allowed,
        bool should_parse_no_password)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            std::optional<AuthenticationType> type;

            bool expect_password = false;
            bool expect_hash = false;
            bool expect_ldap_server_name = false;
            bool expect_kerberos_realm = false;
            bool expect_ssl_cert_subjects = false;
            bool expect_public_ssh_key = false;
            bool expect_http_auth_server = false;

            auto parse_non_password_based_type = [&](auto check_type)
            {
                if (ParserKeyword{AuthenticationTypeInfo::get(check_type).keyword}.ignore(pos, expected))
                {
                    type = check_type;

                    if (check_type == AuthenticationType::NO_AUTHENTICATION)
                        return true;

                    if (check_type == AuthenticationType::LDAP)
                        expect_ldap_server_name = true;
                    else if (check_type == AuthenticationType::KERBEROS)
                        expect_kerberos_realm = true;
                    else if (check_type == AuthenticationType::SSL_CERTIFICATE)
                        expect_ssl_cert_subjects = true;
                    else if (check_type == AuthenticationType::SSH_KEY)
                        expect_public_ssh_key = true;
                    else if (check_type == AuthenticationType::HTTP)
                        expect_http_auth_server = true;
                    else if (check_type == AuthenticationType::JWT)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "CREATE USER is not supported for JWT");
                    else if (check_type != AuthenticationType::NO_PASSWORD)
                        expect_password = true;

                    return true;
                }

                return false;
            };

            {
                const auto first_authentication_type_element_to_check
                    = should_parse_no_password ? AuthenticationType::NO_PASSWORD : AuthenticationType::PLAINTEXT_PASSWORD;

                for (auto check_type : collections::range(first_authentication_type_element_to_check, AuthenticationType::MAX))
                {
                    if (parse_non_password_based_type(check_type))
                        break;
                }
            }

            if (!type)
            {
                if (ParserKeyword{Keyword::SHA256_HASH}.ignore(pos, expected))
                {
                    type = AuthenticationType::SHA256_PASSWORD;
                    expect_hash = true;
                }
                else if (ParserKeyword{Keyword::SCRAM_SHA256_HASH}.ignore(pos, expected))
                {
                    type = AuthenticationType::SCRAM_SHA256_PASSWORD;
                    expect_hash = true;
                }
                else if (ParserKeyword{Keyword::DOUBLE_SHA1_HASH}.ignore(pos, expected))
                {
                    type = AuthenticationType::DOUBLE_SHA1_PASSWORD;
                    expect_hash = true;
                }
                else if (ParserKeyword{Keyword::BCRYPT_HASH}.ignore(pos, expected))
                {
                    type = AuthenticationType::BCRYPT_PASSWORD;
                    expect_hash = true;
                }
                else if (is_type_specifier_mandatory)
                    return false;
            }
            else if (!is_type_specifier_allowed)
            {
                return false;
            }

            /// If authentication type is not specified, then the default password type is used
            if (!type)
                expect_password = true;

            ASTPtr value;
            ASTPtr parsed_salt;
            ASTPtr public_ssh_keys;
            ASTPtr http_auth_scheme;
            ASTPtr ssl_cert_subjects;
            std::optional<String> ssl_cert_subject_type;

            if (expect_password || expect_hash)
            {
                if (!ParserKeyword{Keyword::BY}.ignore(pos, expected) || !ParserStringAndSubstitution{}.parse(pos, value, expected))
                    return false;

                if (expect_hash && (type == AuthenticationType::SHA256_PASSWORD || type == AuthenticationType::SCRAM_SHA256_PASSWORD))
                {
                    if (ParserKeyword{Keyword::SALT}.ignore(pos, expected))
                    {
                        if (!ParserStringAndSubstitution{}.parse(pos, parsed_salt, expected))
                            return false;
                    }
                }
            }
            else if (expect_ldap_server_name)
            {
                if (!ParserKeyword{Keyword::SERVER}.ignore(pos, expected) || !ParserStringAndSubstitution{}.parse(pos, value, expected))
                    return false;
            }
            else if (expect_kerberos_realm)
            {
                if (ParserKeyword{Keyword::REALM}.ignore(pos, expected))
                {
                    if (!ParserStringAndSubstitution{}.parse(pos, value, expected))
                        return false;
                }
            }
            else if (expect_ssl_cert_subjects)
            {
                for (const Keyword &keyword : {Keyword::CN, Keyword::SAN})
                    if (ParserKeyword{keyword}.ignore(pos, expected))
                    {
                        ssl_cert_subject_type = toStringView(keyword);
                        break;
                    }

                if (!ssl_cert_subject_type)
                    return false;

                if (!ParserList{std::make_unique<ParserStringAndSubstitution>(), std::make_unique<ParserToken>(TokenType::Comma), false}.parse(pos, ssl_cert_subjects, expected))
                    return false;
            }
            else if (expect_public_ssh_key)
            {
                if (!ParserKeyword{Keyword::BY}.ignore(pos, expected))
                    return false;

                if (!ParserList{std::make_unique<ParserPublicSSHKey>(), std::make_unique<ParserToken>(TokenType::Comma), false}.parse(pos, public_ssh_keys, expected))
                    return false;
            }
            else if (expect_http_auth_server)
            {
                if (!ParserKeyword{Keyword::SERVER}.ignore(pos, expected))
                    return false;
                if (!ParserStringAndSubstitution{}.parse(pos, value, expected))
                    return false;

                if (ParserKeyword{Keyword::SCHEME}.ignore(pos, expected))
                {
                    if (!ParserStringAndSubstitution{}.parse(pos, http_auth_scheme, expected))
                        return false;
                }
            }

            auth_data = make_intrusive<ASTAuthenticationData>();

            auth_data->type = type;
            auth_data->contains_password = expect_password;
            auth_data->contains_hash = expect_hash;

            if (value)
                auth_data->children.push_back(std::move(value));

            if (parsed_salt)
                auth_data->children.push_back(std::move(parsed_salt));

            if (ssl_cert_subjects)
            {
                auth_data->ssl_cert_subject_type = ssl_cert_subject_type.value();
                auth_data->children = std::move(ssl_cert_subjects->children);
            }

            if (public_ssh_keys)
                auth_data->children = std::move(public_ssh_keys->children);

            if (http_auth_scheme)
                auth_data->children.push_back(std::move(http_auth_scheme));

            parseValidUntil(pos, expected, auth_data->valid_until);

            return true;
        });
    }


    bool parseIdentifiedWith(
        IParserBase::Pos & pos,
        Expected & expected,
        std::vector<boost::intrusive_ptr<ASTAuthenticationData>> & authentication_methods,
        bool should_parse_no_password)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{Keyword::IDENTIFIED}.ignore(pos, expected))
                return false;

            // Parse first authentication method which doesn't come with a leading comma
            {
                bool is_type_specifier_mandatory = ParserKeyword{Keyword::WITH}.ignore(pos, expected);

                boost::intrusive_ptr<ASTAuthenticationData> ast_authentication_data;

                if (!parseAuthenticationData(pos, expected, ast_authentication_data, is_type_specifier_mandatory, is_type_specifier_mandatory, should_parse_no_password))
                {
                    return false;
                }

                authentication_methods.push_back(ast_authentication_data);
            }

            // Need to save current position, process comma and only update real position in case there is an authentication method after
            // the comma. Otherwise, position should not be changed as it needs to be processed by other parsers and possibly throw error
            // on trailing comma.
            IParserBase::Pos aux_pos = pos;
            while (ParserToken{TokenType::Comma}.ignore(aux_pos, expected))
            {
                boost::intrusive_ptr<ASTAuthenticationData> ast_authentication_data;

                if (!parseAuthenticationData(aux_pos, expected, ast_authentication_data, false, true, should_parse_no_password))
                {
                    break;
                }

                pos = aux_pos;
                authentication_methods.push_back(ast_authentication_data);
            }

            return !authentication_methods.empty();
        });
    }

    bool parseIdentifiedOrNotIdentified(IParserBase::Pos & pos, Expected & expected, std::vector<boost::intrusive_ptr<ASTAuthenticationData>> & authentication_methods)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (ParserKeyword{Keyword::NOT_IDENTIFIED}.ignore(pos, expected))
            {
                authentication_methods.emplace_back(make_intrusive<ASTAuthenticationData>());
                authentication_methods.back()->type = AuthenticationType::NO_PASSWORD;

                parseValidUntil(pos, expected, authentication_methods.back()->valid_until);

                return true;
            }

            return parseIdentifiedWith(pos, expected, authentication_methods, true);
        });
    }


    bool parseHostsWithoutPrefix(IParserBase::Pos & pos, Expected & expected, AllowedClientHosts & hosts)
    {
        AllowedClientHosts res_hosts;

        auto parse_host = [&]
        {
            if (ParserKeyword{Keyword::NONE}.ignore(pos, expected))
                return true;

            if (ParserKeyword{Keyword::ANY}.ignore(pos, expected))
            {
                res_hosts.addAnyHost();
                return true;
            }

            if (ParserKeyword{Keyword::LOCAL}.ignore(pos, expected))
            {
                res_hosts.addLocalHost();
                return true;
            }

            if (ParserKeyword{Keyword::REGEXP}.ignore(pos, expected))
            {
                ASTPtr ast;
                if (!ParserList{std::make_unique<ParserStringLiteral>(), std::make_unique<ParserToken>(TokenType::Comma), false}.parse(pos, ast, expected))
                    return false;

                for (const auto & name_regexp_ast : ast->children)
                    res_hosts.addNameRegexp(name_regexp_ast->as<const ASTLiteral &>().value.safeGet<String>());
                return true;
            }

            if (ParserKeyword{Keyword::NAME}.ignore(pos, expected))
            {
                ASTPtr ast;
                if (!ParserList{std::make_unique<ParserStringLiteral>(), std::make_unique<ParserToken>(TokenType::Comma), false}.parse(pos, ast, expected))
                    return false;

                for (const auto & name_ast : ast->children)
                    res_hosts.addName(name_ast->as<const ASTLiteral &>().value.safeGet<String>());

                return true;
            }

            if (ParserKeyword{Keyword::IP}.ignore(pos, expected))
            {
                ASTPtr ast;
                if (!ParserList{std::make_unique<ParserStringLiteral>(), std::make_unique<ParserToken>(TokenType::Comma), false}.parse(pos, ast, expected))
                    return false;

                for (const auto & subnet_ast : ast->children)
                    res_hosts.addSubnet(subnet_ast->as<const ASTLiteral &>().value.safeGet<String>());

                return true;
            }

            if (ParserKeyword{Keyword::LIKE}.ignore(pos, expected))
            {
                ASTPtr ast;
                if (!ParserList{std::make_unique<ParserStringLiteral>(), std::make_unique<ParserToken>(TokenType::Comma), false}.parse(pos, ast, expected))
                    return false;

                for (const auto & pattern_ast : ast->children)
                    res_hosts.addLikePattern(pattern_ast->as<const ASTLiteral &>().value.safeGet<String>());

                return true;
            }

            return false;
        };

        if (!ParserList::parseUtil(pos, expected, parse_host, false))
            return false;

        hosts = std::move(res_hosts);
        return true;
    }


    bool parseHosts(IParserBase::Pos & pos, Expected & expected, std::string_view prefix, AllowedClientHosts & hosts)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!prefix.empty() && !ParserKeyword::createDeprecated(prefix).ignore(pos, expected))
                return false;

            if (!ParserKeyword{Keyword::HOST}.ignore(pos, expected))
                return false;

            AllowedClientHosts res_hosts;
            if (!parseHostsWithoutPrefix(pos, expected, res_hosts))
                return false;

            hosts.add(res_hosts);
            return true;
        });
    }


    bool parseRoles(IParserBase::Pos & pos, Expected & expected, bool default_roles, bool id_mode, boost::intrusive_ptr<ASTRolesOrUsersSet> & roles)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{default_roles ? Keyword::DEFAULT_ROLE : Keyword::ROLE}.ignore(pos, expected))
                return false;

            ParserRolesOrUsersSet roles_p;
            roles_p.allowRoles().useIDMode(id_mode);
            if (default_roles)
                roles_p.allowAll();

            ASTPtr ast;
            if (!roles_p.parse(pos, ast, expected))
                return false;

            roles = boost::static_pointer_cast<ASTRolesOrUsersSet>(ast);
            roles->allow_users = false;
            return true;
        });
    }


    bool parseSettings(IParserBase::Pos & pos, Expected & expected, bool id_mode, boost::intrusive_ptr<ASTSettingsProfileElements> & settings)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            ASTPtr ast;
            ParserSettingsProfileElements elements_p;
            elements_p.useIDMode(id_mode);
            if (!elements_p.parse(pos, ast, expected))
                return false;

            settings = boost::static_pointer_cast<ASTSettingsProfileElements>(ast);
            return true;
        });
    }

    bool parseAlterSettings(IParserBase::Pos & pos, Expected & expected, boost::intrusive_ptr<ASTAlterSettingsProfileElements> & alter_settings)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            ASTPtr ast;
            ParserAlterSettingsProfileElements elements_p;
            if (!elements_p.parse(pos, ast, expected))
                return false;

            alter_settings = boost::static_pointer_cast<ASTAlterSettingsProfileElements>(ast);
            return true;
        });
    }

    bool parseGrantees(IParserBase::Pos & pos, Expected & expected, bool id_mode, boost::intrusive_ptr<ASTRolesOrUsersSet> & grantees)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{Keyword::GRANTEES}.ignore(pos, expected))
                return false;

            ASTPtr ast;
            ParserRolesOrUsersSet grantees_p;
            grantees_p.allowAny().allowUsers().allowCurrentUser().allowRoles().useIDMode(id_mode);
            if (!grantees_p.parse(pos, ast, expected))
                return false;

            grantees = boost::static_pointer_cast<ASTRolesOrUsersSet>(ast);
            return true;
        });
    }

    bool parseOnCluster(IParserBase::Pos & pos, Expected & expected, String & cluster)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            return ParserKeyword{Keyword::ON}.ignore(pos, expected) && ASTQueryWithOnCluster::parse(pos, cluster, expected);
        });
    }

    bool parseDefaultDatabase(IParserBase::Pos & pos, Expected & expected, boost::intrusive_ptr<ASTDatabaseOrNone> & default_database)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{Keyword::DEFAULT_DATABASE}.ignore(pos, expected))
                return false;

            ASTPtr ast;
            ParserDatabaseOrNone database_p;
            if (!database_p.parse(pos, ast, expected))
                return false;

            default_database = boost::static_pointer_cast<ASTDatabaseOrNone>(ast);
            return true;
        });
    }

    bool parseAddIdentifiedWith(IParserBase::Pos & pos, Expected & expected, std::vector<boost::intrusive_ptr<ASTAuthenticationData>> & auth_data)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{Keyword::ADD}.ignore(pos, expected))
            {
                return false;
            }

            return parseIdentifiedWith(pos, expected, auth_data, false);
        });
    }

    bool parseResetAuthenticationMethods(IParserBase::Pos & pos, Expected & expected)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            return ParserKeyword{Keyword::RESET_AUTHENTICATION_METHODS_TO_NEW}.ignore(pos, expected);
        });
    }
}


bool ParserCreateUserQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    bool alter = false;
    if (attach_mode)
    {
        if (!ParserKeyword{Keyword::ATTACH_USER}.ignore(pos, expected))
            return false;
    }
    else
    {
        if (ParserKeyword{Keyword::ALTER_USER}.ignore(pos, expected))
            alter = true;
        else if (!ParserKeyword{Keyword::CREATE_USER}.ignore(pos, expected))
            return false;
    }

    bool if_exists = false;
    bool if_not_exists = false;
    bool or_replace = false;
    if (alter)
    {
        if (ParserKeyword{Keyword::IF_EXISTS}.ignore(pos, expected))
            if_exists = true;
    }
    else
    {
        if (ParserKeyword{Keyword::IF_NOT_EXISTS}.ignore(pos, expected))
            if_not_exists = true;
        else if (ParserKeyword{Keyword::OR_REPLACE}.ignore(pos, expected))
            or_replace = true;
    }

    ASTPtr names_ast;
    if (!ParserUserNamesWithHost(/*allow_query_parameter=*/true).parse(pos, names_ast, expected))
        return false;
    auto names = boost::static_pointer_cast<ASTUserNamesWithHost>(names_ast);

    auto pos_after_parsing_names = pos;

    std::optional<String> new_name;
    std::optional<AllowedClientHosts> hosts;
    std::optional<AllowedClientHosts> add_hosts;
    std::optional<AllowedClientHosts> remove_hosts;
    std::vector<boost::intrusive_ptr<ASTAuthenticationData>> auth_data;
    boost::intrusive_ptr<ASTRolesOrUsersSet> roles;
    boost::intrusive_ptr<ASTRolesOrUsersSet> default_roles;
    boost::intrusive_ptr<ASTSettingsProfileElements> settings;
    boost::intrusive_ptr<ASTAlterSettingsProfileElements> alter_settings;
    boost::intrusive_ptr<ASTRolesOrUsersSet> grantees;
    boost::intrusive_ptr<ASTDatabaseOrNone> default_database;
    ASTPtr global_valid_until;
    String cluster;
    String storage_name;
    bool reset_authentication_methods_to_new = false;

    bool parsed_identified_with = false;
    bool parsed_add_identified_with = false;

    while (true)
    {
        if (auth_data.empty() && !reset_authentication_methods_to_new)
        {
            parsed_identified_with = parseIdentifiedOrNotIdentified(pos, expected, auth_data);

            if (parsed_identified_with)
            {
                continue;
            }
            else if (alter)
            {
                parsed_add_identified_with = parseAddIdentifiedWith(pos, expected, auth_data);
                if (parsed_add_identified_with)
                {
                    continue;
                }
            }
        }

        if (!reset_authentication_methods_to_new && alter && auth_data.empty())
        {
            reset_authentication_methods_to_new = parseResetAuthenticationMethods(pos, expected);
            if (reset_authentication_methods_to_new)
            {
                continue;
            }
        }

        AllowedClientHosts new_hosts;
        if (parseHosts(pos, expected, "", new_hosts))
        {
            if (!hosts)
                hosts.emplace();
            hosts->add(new_hosts);
            continue;
        }

        if (alter)
        {
            boost::intrusive_ptr<ASTAlterSettingsProfileElements> new_alter_settings;
            if (parseAlterSettings(pos, expected, new_alter_settings))
            {
                if (!alter_settings)
                    alter_settings = make_intrusive<ASTAlterSettingsProfileElements>();
                alter_settings->add(std::move(*new_alter_settings));
                continue;
            }
        }
        else
        {
            boost::intrusive_ptr<ASTSettingsProfileElements> new_settings;
            if (parseSettings(pos, expected, attach_mode, new_settings))
            {
                if (!settings)
                    settings = make_intrusive<ASTSettingsProfileElements>();
                settings->add(std::move(*new_settings));
                continue;
            }
        }

        if (!roles && !alter && !attach_mode && parseRoles(pos, expected, /* default_roles = */ false, attach_mode, roles))
            continue;

        if (!default_roles && parseRoles(pos, expected, /* default_roles = */ true, attach_mode, default_roles))
            continue;

        if (cluster.empty() && parseOnCluster(pos, expected, cluster))
            continue;

        if (!grantees && parseGrantees(pos, expected, attach_mode, grantees))
            continue;

        if (!default_database && parseDefaultDatabase(pos, expected, default_database))
            continue;

        if (alter)
        {
            if (!new_name && (names->size() == 1) && parseRenameTo(pos, expected, new_name))
                continue;

            if (parseHosts(pos, expected, toStringView(Keyword::ADD), new_hosts))
            {
                if (!add_hosts)
                    add_hosts.emplace();
                add_hosts->add(new_hosts);
                continue;
            }

            if (parseHosts(pos, expected, toStringView(Keyword::DROP), new_hosts))
            {
                if (!remove_hosts)
                    remove_hosts.emplace();
                remove_hosts->add(new_hosts);
                continue;
            }
        }

        if (storage_name.empty() && ParserKeyword{Keyword::IN}.ignore(pos, expected) && parseAccessStorageName(pos, expected, storage_name))
            continue;

        if (auth_data.empty() && !global_valid_until)
        {
            if (parseValidUntil(pos, expected, global_valid_until))
            {
                continue;
            }
        }

        break;
    }

    if (!alter && !hosts)
    {
        String common_host_pattern;
        if (names->getHostPatternIfCommon(common_host_pattern) && !common_host_pattern.empty())
            hosts.emplace().addLikePattern(common_host_pattern);
    }

    bool alter_query_with_no_changes = alter && pos_after_parsing_names == pos;

    if (alter_query_with_no_changes)
    {
        return false;
    }

    auto query = make_intrusive<ASTCreateUserQuery>();
    node = query;

    query->alter = alter;
    query->attach = attach_mode;
    query->if_exists = if_exists;
    query->if_not_exists = if_not_exists;
    query->or_replace = or_replace;
    query->cluster = std::move(cluster);
    query->names = std::move(names);
    query->new_name = std::move(new_name);
    query->authentication_methods = std::move(auth_data);
    query->hosts = std::move(hosts);
    query->add_hosts = std::move(add_hosts);
    query->remove_hosts = std::move(remove_hosts);
    query->roles = std::move(roles);
    query->default_roles = std::move(default_roles);
    query->settings = std::move(settings);
    query->alter_settings = std::move(alter_settings);
    query->grantees = std::move(grantees);
    query->default_database = std::move(default_database);
    query->global_valid_until = std::move(global_valid_until);
    query->storage_name = std::move(storage_name);
    query->reset_authentication_methods_to_new = reset_authentication_methods_to_new;
    query->add_identified_with = parsed_add_identified_with;
    query->replace_authentication_methods = parsed_identified_with;

    for (const auto & authentication_method : query->authentication_methods)
    {
        query->children.push_back(authentication_method);
    }

    if (query->global_valid_until)
        query->children.push_back(query->global_valid_until);

    return true;
}
}

namespace DB
{

void registerStatementUser(StatementFactory & factory)
{
    factory.registerStatement("CREATE USER",
    {
        .description = R"MARKDOWN(
Creates [user accounts](/concepts/features/security/access-rights#user-account-management).

Syntax:

```sql
CREATE USER [IF NOT EXISTS | OR REPLACE] name1 [, name2 [,...]] [ON CLUSTER cluster_name]
    [NOT IDENTIFIED | IDENTIFIED {[WITH {plaintext_password | sha256_password | sha256_hash | double_sha1_password | double_sha1_hash}] BY {'password' | 'hash'}} | WITH NO_PASSWORD | {WITH ldap SERVER 'server_name'} | {WITH kerberos [REALM 'realm']} | {WITH ssl_certificate CN 'common_name' | SAN 'TYPE:subject_alt_name'} | {WITH ssh_key BY KEY 'public_key' TYPE 'ssh-rsa|...'} | {WITH http SERVER 'server_name' [SCHEME 'Basic']} [VALID UNTIL datetime]
    [, {[{plaintext_password | sha256_password | sha256_hash | ...}] BY {'password' | 'hash'}} | {ldap SERVER 'server_name'} | {...} | ... [,...]]]
    [HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [VALID UNTIL datetime]
    [IN access_storage_type]
    [ROLE role [,...]]
    [DEFAULT ROLE role [,...]]
    [DEFAULT DATABASE database | NONE]
    [GRANTEES {user | role | ANY | NONE} [,...] [EXCEPT {user | role} [,...]]]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY | WRITABLE] | PROFILE 'profile_name'] [,...]
```

`ON CLUSTER` clause allows creating users on a cluster, see [Distributed DDL](/reference/statements/distributed-ddl).

## Identification {#identification}

There are multiple ways of user identification:

- `IDENTIFIED WITH no_password`
- `IDENTIFIED WITH plaintext_password BY 'qwerty'`
- `IDENTIFIED WITH sha256_password BY 'qwerty'` or `IDENTIFIED BY 'password'`
- `IDENTIFIED WITH sha256_hash BY 'hash'` or `IDENTIFIED WITH sha256_hash BY 'hash' SALT 'salt'`
- `IDENTIFIED WITH double_sha1_password BY 'qwerty'`
- `IDENTIFIED WITH double_sha1_hash BY 'hash'`
- `IDENTIFIED WITH bcrypt_password BY 'qwerty'`
- `IDENTIFIED WITH bcrypt_hash BY 'hash'`
- `IDENTIFIED WITH ldap SERVER 'server_name'`
- `IDENTIFIED WITH kerberos` or `IDENTIFIED WITH kerberos REALM 'realm'`
- `IDENTIFIED WITH ssl_certificate CN 'mysite.com:user'`
- `IDENTIFIED WITH ssh_key BY KEY 'public_key' TYPE 'ssh-rsa', KEY 'another_public_key' TYPE 'ssh-ed25519'`
- `IDENTIFIED WITH http SERVER 'http_server'` or `IDENTIFIED WITH http SERVER 'http_server' SCHEME 'basic'`
- `IDENTIFIED BY 'qwerty'`

Password complexity requirements can be edited in [config.xml](/concepts/features/configuration/server-config/configuration-files). Below is an example configuration that requires passwords to be at least 12 characters long and contain 1 number. Each password complexity rule requires a regex to match against passwords and a description of the rule.

```xml
<clickhouse>
    <password_complexity>
        <rule>
            <pattern>.{12}</pattern>
            <message>be at least 12 characters long</message>
        </rule>
        <rule>
            <pattern>\p{N}</pattern>
            <message>contain at least 1 numeric character</message>
        </rule>
    </password_complexity>
</clickhouse>
```

<Note>
In ClickHouse Cloud, by default, passwords must meet the following complexity requirements:
- Be at least 12 characters long
- Contain at least 1 numeric character
- Contain at least 1 uppercase character
- Contain at least 1 lowercase character
- Contain at least 1 special character
</Note>

## Examples {#examples}

1. The following username is `name1` and does not require a password - which obviously doesn't provide much security:

    ```sql
    CREATE USER name1 NOT IDENTIFIED
    ```

2. To specify a plaintext password:

    ```sql
    CREATE USER name2 IDENTIFIED WITH plaintext_password BY 'my_password'
    ```

<Tip>
    The password is stored in a SQL text file in `/var/lib/clickhouse/access`, so it's not a good idea to use `plaintext_password`. Try `sha256_password` instead, as demonstrated next...
</Tip>

3. The most common option is to use a password that is hashed using SHA-256. ClickHouse will hash the password for you when you specify `IDENTIFIED WITH sha256_password`. For example:

    ```sql
    CREATE USER name3 IDENTIFIED WITH sha256_password BY 'my_password'
    ```

    The `name3` user can now login using `my_password`, but the password is stored as the hashed value above. The following SQL file was created in `/var/lib/clickhouse/access` and gets executed at server startup:

    ```bash
    /var/lib/clickhouse/access $ cat 3843f510-6ebd-a52d-72ac-e021686d8a93.sql
    ATTACH USER name3 IDENTIFIED WITH sha256_hash BY '0C268556C1680BEF0640AAC1E7187566704208398DA31F03D18C74F5C5BE5053' SALT '4FB16307F5E10048196966DD7E6876AE53DE6A1D1F625488482C75F14A5097C7';
    ```

<Tip>
    If you have already created a hash value and corresponding salt value for a username, then you can use `IDENTIFIED WITH sha256_hash BY 'hash'` or `IDENTIFIED WITH sha256_hash BY 'hash' SALT 'salt'`. For identification with `sha256_hash` using `SALT` - hash must be calculated from concatenation of 'password' and 'salt'.
</Tip>

4. The `double_sha1_password` is not typically needed, but comes in handy when working with clients that require it (like the MySQL interface):

    ```sql
    CREATE USER name4 IDENTIFIED WITH double_sha1_password BY 'my_password'
    ```

    ClickHouse generates and runs the following query:

    ```response
    CREATE USER name4 IDENTIFIED WITH double_sha1_hash BY 'CCD3A959D6A004B9C3807B728BC2E55B67E10518'
    ```

5. The `bcrypt_password` is the most secure option for storing passwords. It uses the [bcrypt](https://en.wikipedia.org/wiki/Bcrypt) algorithm, which is resilient against brute force attacks even if the password hash is compromised.

    ```sql
    CREATE USER name5 IDENTIFIED WITH bcrypt_password BY 'my_password'
    ```

    The length of the password is limited to 72 characters with this method.
    The bcrypt work factor parameter, which defines the amount of computations and time needed to compute the hash and verify the password, can be modified in the server configuration:

    ```xml
    <bcrypt_workfactor>12</bcrypt_workfactor>
    ```

    The work factor must be between 4 and 31, with a default value of 12.

<Warning>
   For applications with high-frequency authentication,
   consider alternative authentication methods due to
   bcrypt's computational overhead at higher work factors.
</Warning>

6. The type of the password can also be omitted:

    ```sql
    CREATE USER name6 IDENTIFIED BY 'my_password'
    ```

    In this case, ClickHouse will use the default password type specified in the server configuration:

    ```xml
    <default_password_type>sha256_password</default_password_type>
    ```

    The available password types are: `plaintext_password`, `sha256_password`, `double_sha1_password`.

7. Multiple authentication methods can be specified:

   ```sql
   CREATE USER user1 IDENTIFIED WITH plaintext_password by '1', bcrypt_password by '2', plaintext_password by '3''
   ```

Notes:
1. Older versions of ClickHouse might not support the syntax of multiple authentication methods. Therefore, if the ClickHouse server contains such users and is downgraded to a version that does not support it, such users will become unusable and some user related operations will be broken. In order to downgrade gracefully, one must set all users to contain a single authentication method prior to downgrading. Alternatively, if the server was downgraded without the proper procedure, the faulty users should be dropped.
2. `no_password` can not co-exist with other authentication methods for security reasons. Therefore, you can only specify
`no_password` if it is the only authentication method in the query.

## User Host {#user-host}

User host is a host from which a connection to ClickHouse server could be established. The host can be specified in the `HOST` query section in the following ways:

- `HOST IP 'ip_address_or_subnetwork'` — User can connect to ClickHouse server only from the specified IP address or a [subnetwork](https://en.wikipedia.org/wiki/Subnetwork). Examples: `HOST IP '192.168.0.0/16'`, `HOST IP '2001:DB8::/32'`. For use in production, only specify `HOST IP` elements (IP addresses and their masks), since using `host` and `host_regexp` might cause extra latency.
- `HOST ANY` — User can connect from any location. This is a default option.
- `HOST LOCAL` — User can connect only locally.
- `HOST NAME 'fqdn'` — User host can be specified as FQDN. For example, `HOST NAME 'mysite.com'`.
- `HOST REGEXP 'regexp'` — You can use [pcre](http://www.pcre.org/) regular expressions when specifying user hosts. For example, `HOST REGEXP '.*\.mysite\.com'`.
- `HOST LIKE 'template'` — Allows you to use the [LIKE](/reference/functions/regular-functions/string-search-functions#like) operator to filter the user hosts. For example, `HOST LIKE '%'` is equivalent to `HOST ANY`, `HOST LIKE '%.mysite.com'` filters all the hosts in the `mysite.com` domain.

Another way of specifying host is to use `@` syntax following the username. Examples:

- `CREATE USER mira@'127.0.0.1'` — Equivalent to the `HOST IP` syntax.
- `CREATE USER mira@'localhost'` — Equivalent to the `HOST LOCAL` syntax.
- `CREATE USER mira@'192.168.%.%'` — Equivalent to the `HOST LIKE` syntax.

<Tip>
ClickHouse treats `user_name@'address'` as a username as a whole. Thus, technically you can create multiple users with the same `user_name` and different constructions after `@`. However, we do not recommend to do so.
</Tip>

## VALID UNTIL Clause {#valid-until-clause}

Allows you to specify the expiration date and, optionally, the time for an authentication method. It accepts a string as a parameter. It is recommended to use the `YYYY-MM-DD [hh:mm:ss] [timezone]` format for datetime, where `[timezone]` must be a numeric offset such as `+09:00` or one of `UTC`, `GMT`, `Z`, `MSK`, `MSD`; named IANA zones like `Asia/Tokyo` are not recognized (see the note below). By default, this parameter equals `'infinity'`.
The `VALID UNTIL` clause can only be specified along with an authentication method, except for the case where no authentication method has been specified in the query. In this scenario, the `VALID UNTIL` clause will be applied to all existing authentication methods.

Examples:

- `CREATE USER name1 VALID UNTIL '2025-01-01'`
- `CREATE USER name1 VALID UNTIL '2025-01-01 12:00:00 UTC'`
- `CREATE USER name1 VALID UNTIL '2025-01-01 12:00:00 +09:00'`
- `CREATE USER name1 VALID UNTIL 'infinity'`
- `CREATE USER name1 IDENTIFIED WITH plaintext_password BY 'no_expiration', bcrypt_password BY 'expiration_set' VALID UNTIL '2025-01-01'`

<Note>
The datetime string is parsed by `parseDateTimeBestEffort`, which only recognizes the timezone tokens `UTC`, `GMT`, `Z`, `MSK`, `MSD`, and numeric offsets such as `+09:00` or `-05:00`. Named IANA timezones like `Asia/Tokyo` or `Europe/London` are not supported, and a fixed offset is not equivalent to an IANA zone for regions that observe daylight saving time, so you must compute the correct offset for the specific date you are encoding.
</Note>

## GRANTEES Clause {#grantees-clause}

Specifies users or roles which are allowed to receive [privileges](/reference/statements/grant#privileges) from this user on the condition this user has also all required access granted with [GRANT OPTION](/reference/statements/grant#granting-privilege-syntax). Options of the `GRANTEES` clause:

- `user` — Specifies a user this user can grant privileges to.
- `role` — Specifies a role this user can grant privileges to.
- `ANY` — This user can grant privileges to anyone. It's the default setting.
- `NONE` — This user can grant privileges to none.

You can exclude any user or role by using the `EXCEPT` expression. For example, `CREATE USER user1 GRANTEES ANY EXCEPT user2`. It means if `user1` has some privileges granted with `GRANT OPTION` it will be able to grant those privileges to anyone except `user2`.

## Examples {#examples-1}

Create the user account `mira` protected by the password `qwerty`:

```sql
CREATE USER mira HOST IP '127.0.0.1' IDENTIFIED WITH sha256_password BY 'qwerty';
```

`mira` should start client app at the host where the ClickHouse server runs.

Create the user account `john` and assign roles:

```sql
CREATE USER john ROLE role1, role2;
```

Create the user account `john`, assign roles and make some of them default:

```sql
CREATE USER john ROLE role1, role2 DEFAULT ROLE role1;
```

or

```sql
CREATE USER john ROLE role1, role2 DEFAULT ROLE ALL EXCEPT role2;
```

Create the user account `john` and allow him to grant his privileges to the user with `jack` account:

```sql
CREATE USER john GRANTEES jack;
```

Use a query parameter to create the user account `john`:

```sql
SET param_user=john;
CREATE USER {user:Identifier};
```
)MARKDOWN",
        .syntax = R"(
CREATE USER [IF NOT EXISTS | OR REPLACE] name1 [, name2 [,...]] [ON CLUSTER cluster_name]
    [NOT IDENTIFIED | IDENTIFIED {[WITH {no_password | plaintext_password | sha256_password | sha256_hash | double_sha1_password | double_sha1_hash | bcrypt_password | bcrypt_hash | ldap | kerberos | ssl_certificate | ssh_key | http | jwt | scram_sha256_password | scram_sha256_hash}] BY {'password' | 'hash'}} [,...]]
    [HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [VALID UNTIL datetime]
    [IN access_storage_type]
    [DEFAULT ROLE role [,...]]
    [DEFAULT DATABASE database | NONE]
    [GRANTEES {user | role | ANY | NONE} [,...] [EXCEPT {user | role} [,...]]]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | PROFILE 'profile_name'] [,...]
)",
        .parent = "CREATE",
        .related = {"ALTER USER", "CREATE ROLE", "GRANT", "DROP", "SHOW"},
    });

    factory.registerStatement("ALTER USER",
    {
        .description = R"MARKDOWN(
Changes ClickHouse user accounts.

Syntax:

```sql
ALTER USER [IF EXISTS] name1 [RENAME TO new_name |, name2 [,...]]
    [ON CLUSTER cluster_name]
    [NOT IDENTIFIED | RESET AUTHENTICATION METHODS TO NEW | {IDENTIFIED | ADD IDENTIFIED} {[WITH {plaintext_password | sha256_password | sha256_hash | double_sha1_password | double_sha1_hash}] BY {'password' | 'hash'}} | WITH NO_PASSWORD | {WITH ldap SERVER 'server_name'} | {WITH kerberos [REALM 'realm']} | {WITH ssl_certificate CN 'common_name' | SAN 'TYPE:subject_alt_name'} | {WITH ssh_key BY KEY 'public_key' TYPE 'ssh-rsa|...'} | {WITH http SERVER 'server_name' [SCHEME 'Basic']} [VALID UNTIL datetime]
    [, {[{plaintext_password | sha256_password | sha256_hash | ...}] BY {'password' | 'hash'}} | {ldap SERVER 'server_name'} | {...} | ... [,...]]]
    [[ADD | DROP] HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [VALID UNTIL datetime]
    [DEFAULT ROLE role [,...] | ALL | ALL EXCEPT role [,...] ]
    [GRANTEES {user | role | ANY | NONE} [,...] [EXCEPT {user | role} [,...]]]
    [DROP ALL PROFILES]
    [DROP ALL SETTINGS]
    [DROP SETTINGS variable [,...] ]
    [DROP PROFILES 'profile_name' [,...] ]
    [ADD|MODIFY SETTINGS variable [=value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE|CONST|CHANGEABLE_IN_READONLY] [,...] ]
    [SET variable [=value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE|CONST|CHANGEABLE_IN_READONLY] [,...] ]
    [ADD PROFILES 'profile_name' [,...] ]
```

To use `ALTER USER` you must have the [ALTER USER](/reference/statements/grant#access-management) privilege.

`SET variable = value` is an alias for `MODIFY SETTING variable = value`: it changes a single setting in place while keeping the rest. Prefer it (or `MODIFY SETTING`) over the bare `SETTINGS` clause, which replaces the whole settings list and also removes all inherited (parent) profiles.

## GRANTEES Clause {#grantees-clause}

Specifies users or roles which are allowed to receive [privileges](/reference/statements/grant#privileges) from this user on the condition this user has also all required access granted with [GRANT OPTION](/reference/statements/grant#granting-privilege-syntax). Options of the `GRANTEES` clause:

- `user` — Specifies a user this user can grant privileges to.
- `role` — Specifies a role this user can grant privileges to.
- `ANY` — This user can grant privileges to anyone. It's the default setting.
- `NONE` — This user can grant privileges to none.

You can exclude any user or role by using the `EXCEPT` expression. For example, `ALTER USER user1 GRANTEES ANY EXCEPT user2`. It means if `user1` has some privileges granted with `GRANT OPTION` it will be able to grant those privileges to anyone except `user2`.

## Examples {#examples}

Set assigned roles as default:

```sql
ALTER USER user DEFAULT ROLE role1, role2
```

If roles aren't previously assigned to a user, ClickHouse throws an exception.

Set all the assigned roles to default:

```sql
ALTER USER user DEFAULT ROLE ALL
```

If a role is assigned to a user in the future, it will become default automatically.

Set all the assigned roles to default, excepting `role1` and `role2`:

```sql
ALTER USER user DEFAULT ROLE ALL EXCEPT role1, role2
```

Allows the user with `john` account to grant his privileges to the user with `jack` account:

```sql
ALTER USER john GRANTEES jack;
```

Adds new authentication methods to the user while keeping the existing ones:

```sql
ALTER USER user1 ADD IDENTIFIED WITH plaintext_password by '1', bcrypt_password by '2', plaintext_password by '3'
```

Notes:
1. Older versions of ClickHouse might not support the syntax of multiple authentication methods. Therefore, if the ClickHouse server contains such users and is downgraded to a version that does not support it, such users will become unusable and some user related operations will be broken. In order to downgrade gracefully, one must set all users to contain a single authentication method prior to downgrading. Alternatively, if the server was downgraded without the proper procedure, the faulty users should be dropped.
2. `no_password` can not co-exist with other authentication methods for security reasons.
Because of that, it is not possible to `ADD` a `no_password` authentication method. The below query will throw an error:

```sql
ALTER USER user1 ADD IDENTIFIED WITH no_password
```

If you want to drop authentication methods for a user and rely on `no_password`, you must specify in the below replacing form.

Reset authentication methods and adds the ones specified in the query (effect of leading IDENTIFIED without the ADD keyword):

```sql
ALTER USER user1 IDENTIFIED WITH plaintext_password by '1', bcrypt_password by '2', plaintext_password by '3'
```

Reset authentication methods and keep the most recent added one:
```sql
ALTER USER user1 RESET AUTHENTICATION METHODS TO NEW
```

## VALID UNTIL Clause {#valid-until-clause}

Allows you to specify the expiration date and, optionally, the time for an authentication method. It accepts a string as a parameter. It is recommended to use the `YYYY-MM-DD [hh:mm:ss] [timezone]` format for datetime. By default, this parameter equals `'infinity'`.
The `VALID UNTIL` clause can only be specified along with an authentication method, except for the case where no authentication method has been specified in the query. In this scenario, the `VALID UNTIL` clause will be applied to all existing authentication methods.

Examples:

- `ALTER USER name1 VALID UNTIL '2025-01-01'`
- `ALTER USER name1 VALID UNTIL '2025-01-01 12:00:00 UTC'`
- `ALTER USER name1 VALID UNTIL 'infinity'`
- `ALTER USER name1 IDENTIFIED WITH plaintext_password BY 'no_expiration', bcrypt_password BY 'expiration_set' VALID UNTIL'2025-01-01''`
)MARKDOWN",
        .syntax = R"(
ALTER USER [IF EXISTS] name1 [RENAME TO new_name |, name2 [,...]]
    [ON CLUSTER cluster_name]
    [NOT IDENTIFIED | RESET AUTHENTICATION METHODS TO NEW | {IDENTIFIED | ADD IDENTIFIED} {...} [,...]]
    [[ADD | DROP] HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [VALID UNTIL datetime]
    [DEFAULT ROLE role [,...] | ALL | ALL EXCEPT role [,...] ]
    [DEFAULT DATABASE database | NONE]
    [GRANTEES {user | role | ANY | NONE} [,...] [EXCEPT {user | role} [,...]]]
    [SETTINGS variable [= value] ... | PROFILE 'profile_name'] [,...]
)",
        .parent = "ALTER",
        .related = {"CREATE USER", "ALTER", "GRANT", "SHOW"},
    });
}

}
