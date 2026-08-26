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
#include <Parsers/Access/parseAccessRightsElements.h>
#include <Parsers/Access/parseUserName.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
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

    bool parseValidUntil(IParserBase::Pos & pos, Expected & expected, ASTPtr & valid_until, bool & is_interval)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (ParserKeyword{Keyword::VALID_UNTIL}.ignore(pos, expected))
            {
                is_interval = false;
                ParserStringAndSubstitution until_p;
                return until_p.parse(pos, valid_until, expected);
            }

            /// VALID FOR <interval> is a shortcut: the deadline is computed as `now` plus the interval
            /// at query execution time and stored in the VALID UNTIL form.
            if (ParserKeyword{Keyword::VALID_FOR}.ignore(pos, expected))
            {
                is_interval = true;
                ParserExpression interval_p;
                if (!interval_p.parse(pos, valid_until, expected))
                    return false;

                /// `IN` is a normal operator in expression parsing, so with the trailing access-storage
                /// clause (`CREATE USER ... VALID FOR INTERVAL 1 DAY IN <storage>`) the expression parser
                /// greedily consumes `IN <storage>` as part of the interval expression instead of leaving
                /// it for `parseAccessStorageName`. (`VALID UNTIL` is not affected: its value parser stops
                /// at the string literal.) Detect this exact shape - a top-level `in` whose right side is
                /// a bare one-token access-storage name - and give the clause back to the caller: keep the
                /// left side as the interval and rewind the position to the `IN` keyword. Anything else,
                /// e.g. a genuine membership test, is left as-is and rejected by the interval type check
                /// at execution time.
                if (const auto * maybe_in = valid_until->as<ASTFunction>();
                    maybe_in && maybe_in->name == "in" && maybe_in->arguments && maybe_in->arguments->children.size() == 2)
                {
                    /// `parseAccessStorageName` accepts both an identifier and a string literal, so both
                    /// `IN memory` and `IN 'memory'` have to be given back.
                    const auto & storage_ast = maybe_in->arguments->children[1];
                    const auto * storage_identifier = storage_ast->as<ASTIdentifier>();
                    const auto * storage_literal = storage_ast->as<ASTLiteral>();
                    const bool is_storage_name = (storage_identifier && storage_identifier->isShort())
                        || (storage_literal && storage_literal->value.getType() == Field::Types::String);

                    if (is_storage_name)
                    {
                        /// A short identifier, a string literal and the `IN` keyword are one token each.
                        /// Verify the rewound position really points at `IN` before acting on it.
                        IParserBase::Pos in_pos = pos;
                        --in_pos;
                        --in_pos;
                        if (ParserKeyword{Keyword::IN}.checkWithoutMoving(in_pos, expected))
                        {
                            valid_until = maybe_in->arguments->children[0];
                            pos = in_pos;
                        }
                    }
                }
                return true;
            }

            return false;
        });
    }

    bool parseGrants(IParserBase::Pos & pos, Expected & expected, AccessRightsElements & grants)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{Keyword::GRANTS}.ignore(pos, expected))
                return false;

            if (!ParserToken{TokenType::OpeningRoundBracket}.ignore(pos, expected))
                return false;

            AccessRightsElements elements;
            if (!parseAccessRightsElementsWithoutOptions(pos, expected, elements))
                return false;

            if (!ParserToken{TokenType::ClosingRoundBracket}.ignore(pos, expected))
                return false;

            grants = std::move(elements);
            return true;
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

            ASTPtr method_valid_until;
            if (parseValidUntil(pos, expected, method_valid_until, auth_data->valid_until_is_interval))
                auth_data->setValidUntil(std::move(method_valid_until));
            parseGrants(pos, expected, auth_data->grants);

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

                ASTPtr method_valid_until;
                if (parseValidUntil(pos, expected, method_valid_until, authentication_methods.back()->valid_until_is_interval))
                    authentication_methods.back()->setValidUntil(std::move(method_valid_until));
                parseGrants(pos, expected, authentication_methods.back()->grants);

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
    bool global_valid_until_is_interval = false;
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
            if (parseValidUntil(pos, expected, global_valid_until, global_valid_until_is_interval))
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

    /// `VALID FOR <interval>` is resolved to an absolute deadline at query execution time and stored
    /// (and shown) in the `VALID UNTIL` form. It therefore never appears in the on-disk (`ATTACH`)
    /// representation, and it cannot be evaluated during attach anyway: there is no query context, and
    /// re-resolving `now` on every startup would let the deadline drift forever. Reject it here with a
    /// clear message instead of failing later, deep inside `deserializeAccessEntity`, while loading a
    /// hand-written access definition.
    if (attach_mode)
    {
        bool has_valid_for = global_valid_until_is_interval;
        for (const auto & authentication_method : auth_data)
            has_valid_for = has_valid_for || authentication_method->valid_until_is_interval;

        if (has_valid_for)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "VALID FOR is not allowed in ATTACH USER queries; the deadline must be stored as an absolute VALID UNTIL value");
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
    query->global_valid_until_is_interval = global_valid_until_is_interval;
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
        .description = R"DOCS_MD(
Creates [user accounts](/concepts/features/security/access-rights#user-account-management).

Syntax:

```sql
CREATE USER [IF NOT EXISTS | OR REPLACE] name1 [, name2 [,...]] [ON CLUSTER cluster_name]
    [{VALID UNTIL datetime | VALID FOR interval}]
    [NOT IDENTIFIED | IDENTIFIED {[WITH {plaintext_password | sha256_password | sha256_hash | double_sha1_password | double_sha1_hash}] BY {'password' | 'hash'}} | WITH NO_PASSWORD | {WITH ldap SERVER 'server_name'} | {WITH kerberos [REALM 'realm']} | {WITH ssl_certificate CN 'common_name' | SAN 'TYPE:subject_alt_name'} | {WITH ssh_key BY KEY 'public_key' TYPE 'ssh-rsa|...'} | {WITH http SERVER 'server_name' [SCHEME 'Basic']} [{VALID UNTIL datetime | VALID FOR interval}] [GRANTS (privilege ON object [,...])]
    [, {[{plaintext_password | sha256_password | sha256_hash | ...}] BY {'password' | 'hash'}} | {ldap SERVER 'server_name'} | {...} | ... [,...]]]
    [HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
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

Allows you to specify the expiration date and, optionally, the time for an authentication method. It accepts a string as a parameter. It is recommended to use the `YYYY-MM-DD [hh:mm:ss] [timezone]` format for datetime, where `[timezone]` must be a numeric offset such as `+09:00` or one of `UTC`, `GMT`, `Z`, `MSK`, `MSD`; named IANA zones like `Asia/Tokyo` are not recognized (see the note below). By default, this parameter equals `'infinity'`. The accepted deadline range is `1900-01-01 00:00:00 UTC` through `9999-12-31 09:59:59 UTC` — the latest instant that stays within year 9999 in every time zone, so the stored instant is never clamped when it is rendered. A deadline in the past means the credentials are already expired. Deadlines before `1970-01-01 00:00:01 UTC` are accepted only as an "already expired" marker: they are canonicalized to the smallest expired instant, one second after the Unix epoch (`1970-01-01 00:00:01 UTC`), so `SHOW CREATE USER` reports that instant instead of the deadline you wrote. Deadlines from that instant onward are stored exactly.

A deadline is stored as an absolute instant, but `SHOW CREATE USER` and [`system.users`](/reference/system-tables/users) render it in the server or session time zone, so the same stored instant appears as different wall-clock text on differently configured servers: the canonicalized expired instant above, for example, renders as `1970-01-01 00:00:01` on a server in `UTC` and as `1970-01-01 14:00:01` on a server in `Pacific/Kiritimati`. Enforcement always uses the stored instant, not its rendering.

The placement of the clause determines which authentication methods it applies to:

- Before the `IDENTIFIED` clause (or when the query specifies no authentication method at all): the deadline is a user-level deadline that applies to every authentication method of the user.
- After an authentication method: the deadline applies to that method only. A clause written after the whole `IDENTIFIED` list therefore binds to the last method only, leaving the earlier methods non-expiring.

Examples:

- `CREATE USER name1 VALID UNTIL '2025-01-01'`
- `CREATE USER name1 VALID UNTIL '2025-01-01 12:00:00 UTC'`
- `CREATE USER name1 VALID UNTIL '2025-01-01 12:00:00 +09:00'`
- `CREATE USER name1 VALID UNTIL 'infinity'`
- `CREATE USER name1 VALID UNTIL '2025-01-01' IDENTIFIED WITH plaintext_password BY 'password_1', bcrypt_password BY 'password_2'` — the user-level deadline applies to both methods.
- `CREATE USER name1 IDENTIFIED WITH plaintext_password BY 'no_expiration', bcrypt_password BY 'expiration_set' VALID UNTIL '2025-01-01'` — the deadline applies only to the `bcrypt_password` method; `plaintext_password` never expires.

<Note>
The datetime string is parsed by `parseDateTimeBestEffort`, which only recognizes the timezone tokens `UTC`, `GMT`, `Z`, `MSK`, `MSD`, and numeric offsets such as `+09:00` or `-05:00`. Named IANA timezones like `Asia/Tokyo` or `Europe/London` are not supported, and a fixed offset is not equivalent to an IANA zone for regions that observe daylight saving time, so you must compute the correct offset for the specific date you are encoding.
</Note>

## VALID FOR Clause {#valid-for-clause}

The `VALID FOR` clause is a convenience shorthand for `VALID UNTIL`. Instead of an absolute date and time, it accepts an [interval](/reference/data-types/special-data-types/interval), and the expiration deadline is computed as the current time plus that interval at the moment the query is executed. The result is then stored in the `VALID UNTIL` form, so `SHOW CREATE USER` always displays the resolved absolute deadline. It can be used everywhere `VALID UNTIL` can, and it follows the same placement rules: before `IDENTIFIED` (or with no authentication method) it is a user-level deadline that applies to every method, while after an authentication method it applies to that method only. The deadline is stored and enforced with second precision, so sub-second intervals (`NANOSECOND`, `MICROSECOND`, `MILLISECOND`) are rejected; the smallest accepted unit is `SECOND`. A negative interval is accepted as a way to mark the credentials as already expired; if the resulting deadline falls before `1970-01-01 00:00:01 UTC`, it is canonicalized to that smallest expired instant, which is what `SHOW CREATE USER` then reports — rendered in the server or session time zone, as described for [`VALID UNTIL`](#valid-until-clause).

Examples:

- `CREATE USER name1 VALID FOR INTERVAL 1 DAY`
- `CREATE USER name1 VALID FOR INTERVAL 3 MONTH`
- `CREATE USER name1 VALID FOR INTERVAL 1 DAY + INTERVAL 12 HOUR`
- `CREATE USER name1 VALID FOR INTERVAL 30 DAY IDENTIFIED WITH plaintext_password BY 'password_1', bcrypt_password BY 'password_2'` — the user-level deadline applies to both methods.
- `CREATE USER name1 IDENTIFIED WITH plaintext_password BY 'no_expiration', bcrypt_password BY 'expiration_set' VALID FOR INTERVAL 30 DAY` — the deadline applies only to the `bcrypt_password` method; `plaintext_password` never expires.

## GRANTS Clause {#grants-clause}

Allows you to limit the access rights available to a session authenticated with a particular authentication method. It accepts a list of privileges in the same form as the [GRANT](/reference/statements/grant) statement, in parentheses. The clause is specified after an authentication method (after its `VALID UNTIL` clause, if any) and applies only to that method.

When a user logs in with such an authentication method, the access rights of the session are the intersection of the user's access rights (including the rights from granted roles) with the privileges listed in the clause. The clause never adds any access rights: if a listed privilege is not granted to the user, the session does not have it. Sessions authenticated with such a method also cannot grant privileges (the `GRANT OPTION` never survives the intersection) or administer roles. Administering roles includes not only creating, altering, dropping, granting and revoking roles, but also changing which roles are activated by default for a user (`SET DEFAULT ROLE` and `ALTER USER ... DEFAULT ROLE`), which is rejected as well.

`EXECUTE AS` switches the principal of the session, so a statement running under impersonation is limited by the intersection of the **target** user's access rights with the listed privileges, rather than by the rights of the user who logged in. The limit itself is never shed, and impersonating requires `IMPERSONATE ON target` to be both granted to the user and listed in the clause, so a limited credential can never reach further than the same user's unlimited credential.

This provides a convenient way to create tokens for applications: an additional credential with an expiration date and a limited set of privileges, which is tied to the user - it is displayed in `system.query_log` and `system.processes` as the user, it stops working if the user is deleted, and it loses access rights when the user loses them.

<Warning>
**Enforcement is initiator-only.** The authentication-method `GRANTS` limit and its `VALID UNTIL` expiration are enforced only on the node that receives the query (the initiator). They are **not** propagated to other nodes of a cluster, so do not rely on the clause to constrain execution cluster-wide. Remote nodes retain their usual role scoping. The clause is also not available in `users.xml`. The [query result cache](/concepts/features/performance/caches/query-cache) is shared by all authentication methods of a user: it isolates entries by user and roles, and a cache hit is not re-checked against the `GRANTS` of the method the session logged in with.
</Warning>

Examples:

- `CREATE USER name1 IDENTIFIED BY 'qwerty' GRANTS (SELECT ON db.*)`
- `ALTER USER name1 ADD IDENTIFIED WITH plaintext_password BY 'app_token' VALID UNTIL '2026-12-31' GRANTS (SELECT ON db.table, INSERT ON db.table)`

Note that the limit is a property of the authentication method, captured at the moment of the login: changing the clause with `ALTER USER` affects new sessions, not the already established ones.

Filtered source grants such as `READ ON S3('s3://bucket/.*')` are not supported in the clause yet: the intersection compares a source filter as an opaque string and cannot narrow one filter to another, so such a grant is rejected rather than silently granting no access.

The clause is supported only for authentication methods whose credentials are verified purely locally by the server. For methods whose verification contacts (or, in the case of `jwt`, may contact — for example to fetch the signing keys) an external system (`ldap`, `kerberos`, `http`, `jwt`) the clause is rejected: when several authentication methods accept the same credential, the limit is enforced by re-checking the credential against the other methods, and an extra probe of an external system is unsafe, so another method accepting the same credential could bypass the limit.

When the same effective credential is accepted by more than one authentication method, the login is limited fail-close by all of them: the session gets the intersection of the `GRANTS` of all matching methods and expires at the earliest of their `VALID UNTIL`. The earliest `VALID UNTIL` wins even when it has already passed — the login is rejected, exactly as if the single matched method had expired, so the expiry of a token never silently hands the shared credential the rights or lifetime of a broader method.

This combination is only checked among authentication methods verified locally by the server, for the same reason the clause itself is rejected on an externally verified method above: re-checking the credential there would require an unsafe extra probe of the external system. So if the same credential also happens to be accepted by an externally verified method (`ldap`, `kerberos`, `http`, `jwt`) on the same user, that method's own `VALID UNTIL` is not part of the combination, and an earlier expiry configured on it does not shorten the session obtained through the locally verified method.

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
)DOCS_MD",
        .syntax = R"(
CREATE USER [IF NOT EXISTS | OR REPLACE] name1 [, name2 [,...]] [ON CLUSTER cluster_name]
    [{VALID UNTIL datetime | VALID FOR interval}]
    [NOT IDENTIFIED | IDENTIFIED {[WITH {plaintext_password | sha256_password | sha256_hash | double_sha1_password | double_sha1_hash}] BY {'password' | 'hash'}} | WITH NO_PASSWORD | {WITH ldap SERVER 'server_name'} | {WITH kerberos [REALM 'realm']} | {WITH ssl_certificate CN 'common_name' | SAN 'TYPE:subject_alt_name'} | {WITH ssh_key BY KEY 'public_key' TYPE 'ssh-rsa|...'} | {WITH http SERVER 'server_name' [SCHEME 'Basic']} [{VALID UNTIL datetime | VALID FOR interval}] [GRANTS (privilege ON object [,...])]
    [, {[{plaintext_password | sha256_password | sha256_hash | ...}] BY {'password' | 'hash'}} | {ldap SERVER 'server_name'} | {...} | ... [,...]]]
    [HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [IN access_storage_type]
    [ROLE role [,...]]
    [DEFAULT ROLE role [,...]]
    [DEFAULT DATABASE database | NONE]
    [GRANTEES {user | role | ANY | NONE} [,...] [EXCEPT {user | role} [,...]]]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY | WRITABLE] | PROFILE 'profile_name'] [,...]
)",
        .parent = "CREATE",
        .related = {"ALTER USER", "CREATE ROLE", "GRANT", "DROP", "SHOW"},
    });

    factory.registerStatement("ALTER USER",
    {
        .description = R"DOCS_MD(
Changes ClickHouse user accounts.

Syntax:

```sql
ALTER USER [IF EXISTS] name1 [RENAME TO new_name |, name2 [,...]]
    [ON CLUSTER cluster_name]
    [{VALID UNTIL datetime | VALID FOR interval}]
    [NOT IDENTIFIED | RESET AUTHENTICATION METHODS TO NEW | {IDENTIFIED | ADD IDENTIFIED} {[WITH {plaintext_password | sha256_password | sha256_hash | double_sha1_password | double_sha1_hash}] BY {'password' | 'hash'}} | WITH NO_PASSWORD | {WITH ldap SERVER 'server_name'} | {WITH kerberos [REALM 'realm']} | {WITH ssl_certificate CN 'common_name' | SAN 'TYPE:subject_alt_name'} | {WITH ssh_key BY KEY 'public_key' TYPE 'ssh-rsa|...'} | {WITH http SERVER 'server_name' [SCHEME 'Basic']} [{VALID UNTIL datetime | VALID FOR interval}] [GRANTS (privilege ON object [,...])]
    [, {[{plaintext_password | sha256_password | sha256_hash | ...}] BY {'password' | 'hash'}} | {ldap SERVER 'server_name'} | {...} | ... [,...]]]
    [[ADD | DROP] HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [IN access_storage_type]
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

Allows you to specify the expiration date and, optionally, the time for an authentication method. It accepts a string as a parameter. It is recommended to use the `YYYY-MM-DD [hh:mm:ss] [timezone]` format for datetime. By default, this parameter equals `'infinity'`. The accepted deadline range is `1900-01-01 00:00:00 UTC` through `9999-12-31 09:59:59 UTC` — the latest instant that stays within year 9999 in every time zone, so the stored instant is never clamped when it is rendered. A deadline in the past means the credentials are already expired. Deadlines before `1970-01-01 00:00:01 UTC` are accepted only as an "already expired" marker: they are canonicalized to the smallest expired instant, one second after the Unix epoch (`1970-01-01 00:00:01 UTC`), so `SHOW CREATE USER` reports that instant instead of the deadline you wrote. Deadlines from that instant onward are stored exactly.

A deadline is stored as an absolute instant, but `SHOW CREATE USER` and [`system.users`](/reference/system-tables/users) render it in the server or session time zone, so the same stored instant appears as different wall-clock text on differently configured servers: the canonicalized expired instant above, for example, renders as `1970-01-01 00:00:01` on a server in `UTC` and as `1970-01-01 14:00:01` on a server in `Pacific/Kiritimati`. Enforcement always uses the stored instant, not its rendering.

The placement of the clause determines which authentication methods it applies to:

- Before the `IDENTIFIED` clause (or when the query specifies no authentication method at all): the deadline is a user-level deadline that applies to every authentication method of the user.
- After an authentication method: the deadline applies to that method only. A clause written after the whole `IDENTIFIED` list therefore binds to the last method only, leaving the earlier methods non-expiring.

Examples:

- `ALTER USER name1 VALID UNTIL '2025-01-01'`
- `ALTER USER name1 VALID UNTIL '2025-01-01 12:00:00 UTC'`
- `ALTER USER name1 VALID UNTIL 'infinity'`
- `ALTER USER name1 VALID UNTIL '2025-01-01' IDENTIFIED WITH plaintext_password BY 'password_1', bcrypt_password BY 'password_2'` — the user-level deadline applies to both methods.
- `ALTER USER name1 IDENTIFIED WITH plaintext_password BY 'no_expiration', bcrypt_password BY 'expiration_set' VALID UNTIL '2025-01-01'` — the deadline applies only to the `bcrypt_password` method; `plaintext_password` never expires.

## VALID FOR Clause {#valid-for-clause}

The `VALID FOR` clause is a convenience shorthand for `VALID UNTIL`. Instead of an absolute date and time it accepts an [interval](/reference/data-types/special-data-types/interval), and the expiration deadline is computed as the current time plus that interval at the moment the query is executed. The result is stored in the `VALID UNTIL` form, so `SHOW CREATE USER` always displays the resolved absolute deadline. It follows the same placement rules as `VALID UNTIL`: before `IDENTIFIED` (or with no authentication method) it is a user-level deadline that applies to every method, while after an authentication method it applies to that method only. The deadline is stored and enforced with second precision, so sub-second intervals (`NANOSECOND`, `MICROSECOND`, `MILLISECOND`) are rejected; the smallest accepted unit is `SECOND`. A negative interval is accepted as a way to mark the credentials as already expired; if the resulting deadline falls before `1970-01-01 00:00:01 UTC`, it is canonicalized to that smallest expired instant, which is what `SHOW CREATE USER` then reports — rendered in the server or session time zone, as described for [`VALID UNTIL`](#valid-until-clause).

Examples:

- `ALTER USER name1 VALID FOR INTERVAL 1 DAY`
- `ALTER USER name1 VALID FOR INTERVAL 3 MONTH`
- `ALTER USER name1 VALID FOR INTERVAL 30 DAY IDENTIFIED WITH plaintext_password BY 'password_1', bcrypt_password BY 'password_2'` — the user-level deadline applies to both methods.
- `ALTER USER name1 IDENTIFIED WITH plaintext_password BY 'no_expiration', bcrypt_password BY 'expiration_set' VALID FOR INTERVAL 30 DAY` — the deadline applies only to the `bcrypt_password` method; `plaintext_password` never expires.

## GRANTS Clause {#grants-clause}

Allows you to limit the access rights available to a session authenticated with a particular authentication method. See the [GRANTS clause of CREATE USER](/reference/statements/create/user#grants-clause) for details.

Together with `ADD IDENTIFIED`, this provides a convenient way to create tokens for applications: an additional credential with an expiration date and a limited set of privileges.

Example:

- `ALTER USER name1 ADD IDENTIFIED WITH plaintext_password BY 'app_token' VALID UNTIL '2026-12-31' GRANTS (SELECT ON db.table, INSERT ON db.table)`
)DOCS_MD",
        .syntax = R"(
ALTER USER [IF EXISTS] name1 [RENAME TO new_name |, name2 [,...]]
    [ON CLUSTER cluster_name]
    [{VALID UNTIL datetime | VALID FOR interval}]
    [NOT IDENTIFIED | RESET AUTHENTICATION METHODS TO NEW | {IDENTIFIED | ADD IDENTIFIED} {[WITH {plaintext_password | sha256_password | sha256_hash | double_sha1_password | double_sha1_hash}] BY {'password' | 'hash'}} | WITH NO_PASSWORD | {WITH ldap SERVER 'server_name'} | {WITH kerberos [REALM 'realm']} | {WITH ssl_certificate CN 'common_name' | SAN 'TYPE:subject_alt_name'} | {WITH ssh_key BY KEY 'public_key' TYPE 'ssh-rsa|...'} | {WITH http SERVER 'server_name' [SCHEME 'Basic']} [{VALID UNTIL datetime | VALID FOR interval}] [GRANTS (privilege ON object [,...])]
    [, {[{plaintext_password | sha256_password | sha256_hash | ...}] BY {'password' | 'hash'}} | {ldap SERVER 'server_name'} | {...} | ... [,...]]]
    [[ADD | DROP] HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [IN access_storage_type]
    [DEFAULT ROLE role [,...] | ALL | ALL EXCEPT role [,...] ]
    [GRANTEES {user | role | ANY | NONE} [,...] [EXCEPT {user | role} [,...]]]
    [DROP ALL PROFILES]
    [DROP ALL SETTINGS]
    [DROP SETTINGS variable [,...] ]
    [DROP PROFILES 'profile_name' [,...] ]
    [ADD|MODIFY SETTINGS variable [=value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE|CONST|CHANGEABLE_IN_READONLY] [,...] ]
    [SET variable [=value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE|CONST|CHANGEABLE_IN_READONLY] [,...] ]
    [ADD PROFILES 'profile_name' [,...] ]
)",
        .parent = "ALTER",
        .related = {"CREATE USER", "ALTER", "GRANT", "SHOW"},
    });
}

}
