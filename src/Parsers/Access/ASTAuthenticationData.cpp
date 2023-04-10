#include "config.h"

#include <Parsers/Access/ASTAuthenticationData.h>

#include <Access/AccessControl.h>
#include <Access/Common/AuthenticationData.h>
#include <Common/Exception.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTLiteral.h>
#include <IO/Operators.h>

#if USE_SSL
#     include <openssl/crypto.h>
#     include <openssl/rand.h>
#     include <openssl/err.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
    extern const int LOGICAL_ERROR;
    extern const int OPENSSL_ERROR;
}

void ASTAuthenticationData::checkPasswordComplexityRules(ContextPtr context) const
{
    if (expect_password)
    {
        if (const auto * password = children[0]->as<const ASTLiteral>())
        {
            context->getAccessControl().checkPasswordComplexityRules(password->value.safeGet<String>());
        }
    }
}

AuthenticationData ASTAuthenticationData::makeAuthenticationData(ContextPtr context, bool check_password_rules) const
{
    if (type && *type == AuthenticationType::NO_PASSWORD)
        return AuthenticationData();

    boost::container::flat_set<String> common_names;
    String value;

    if (expect_common_names)
    {
        for (const auto & ast_child : children[0]->children)
            common_names.insert(ast_child->as<const ASTLiteral &>().value.safeGet<String>());
    }
    else if (!children.empty())
    {
        value = children[0]->as<const ASTLiteral &>().value.safeGet<String>();
    }

    if (expect_password)
    {
        if (!context)
            throw Exception(DB::ErrorCodes::LOGICAL_ERROR, "Cannot get necessary parameters without context");

        auto & access_control = context->getAccessControl();
        AuthenticationType default_password_type = access_control.getDefaultPasswordType();

        /// NOTE: We will also extract bcrypt workfactor from access_control

        AuthenticationType current_type;

        if (type)
            current_type = *type;
        else
            current_type = default_password_type;

        AuthenticationData auth_data(current_type);

        if (check_password_rules)
            access_control.checkPasswordComplexityRules(value);

        if (type == AuthenticationType::SHA256_PASSWORD)
        {
#if USE_SSL
            ///random generator FIPS complaint
            uint8_t key[32];
            if (RAND_bytes(key, sizeof(key)) != 1)
            {
                char buf[512] = {0};
                ERR_error_string_n(ERR_get_error(), buf, sizeof(buf));
                throw Exception(ErrorCodes::OPENSSL_ERROR, "Cannot generate salt for password. OpenSSL {}", buf);
            }

            String salt;
            salt.resize(sizeof(key) * 2);
            char * buf_pos = salt.data();
            for (uint8_t k : key)
            {
                writeHexByteUppercase(k, buf_pos);
                buf_pos += 2;
            }
            value.append(salt);
            auth_data.setSalt(salt);
#else
            throw DB::Exception(
                "SHA256 passwords support is disabled, because ClickHouse was built without SSL library",
                DB::ErrorCodes::SUPPORT_IS_DISABLED);
#endif
        }

        auth_data.setPassword(value);
        return auth_data;
    }

    AuthenticationData auth_data(*type);

    if (expect_hash)
    {
        auth_data.setPasswordHashHex(value);

        if (type == AuthenticationType::SHA256_PASSWORD && children.size() == 2)
        {
            String parsed_salt = children[1]->as<const ASTLiteral &>().value.safeGet<String>();
            auth_data.setSalt(parsed_salt);
        }
    }
    else if (expect_ldap_server_name)
    {
        auth_data.setLDAPServerName(value);
    }
    else if (expect_kerberos_realm)
    {
        auth_data.setKerberosRealm(value);
    }
    else if (expect_common_names)
    {
        auth_data.setSSLCertificateCommonNames(std::move(common_names));
    }

    return auth_data;
}

std::shared_ptr<ASTAuthenticationData> ASTAuthenticationData::makeASTAuthenticationData(AuthenticationData auth_data)
{
    auto node = std::make_shared<ASTAuthenticationData>();
    auto auth_type = auth_data.getType();
    node->type = auth_type;

    switch (auth_type)
    {
        case AuthenticationType::PLAINTEXT_PASSWORD:
        {
            node->children.push_back(std::make_shared<ASTLiteral>(auth_data.getPassword()));
            break;
        }
        case AuthenticationType::SHA256_PASSWORD:
        {
            node->expect_hash = true;
            node->children.push_back(std::make_shared<ASTLiteral>(auth_data.getPasswordHashHex()));

            if (!auth_data.getSalt().empty())
                node->children.push_back(std::make_shared<ASTLiteral>(auth_data.getSalt()));
            break;
        }
        case AuthenticationType::DOUBLE_SHA1_PASSWORD:
        {
            node->expect_hash = true;
            node->children.push_back(std::make_shared<ASTLiteral>(auth_data.getPasswordHashHex()));
            break;
        }
        case AuthenticationType::LDAP:
        {
            node->expect_ldap_server_name = true;
            node->children.push_back(std::make_shared<ASTLiteral>(auth_data.getLDAPServerName()));
            break;
        }
        case AuthenticationType::KERBEROS:
        {
            node->expect_kerberos_realm = true;
            const auto & realm = auth_data.getKerberosRealm();

            if (!realm.empty())
                node->children.push_back(std::make_shared<ASTLiteral>(realm));

            break;
        }
        case AuthenticationType::SSL_CERTIFICATE:
        {
            node->expect_common_names = true;

            for (const auto & name : auth_data.getSSLCertificateCommonNames())
                node->children.push_back(std::make_shared<ASTLiteral>(name));

            break;
        }

        case AuthenticationType::NO_PASSWORD: [[fallthrough]];
        case AuthenticationType::MAX:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "AST: Unexpected authentication type {}", toString(auth_type));
    }

    return node;
}

void ASTAuthenticationData::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    if (type && *type == AuthenticationType::NO_PASSWORD)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " NOT IDENTIFIED"
                      << (settings.hilite ? IAST::hilite_none : "");
        return;
    }

    String auth_type_name;
    String prefix; /// "BY" or "SERVER" or "REALM"
    ASTPtr password; /// either a password or hash
    ASTPtr salt;
    ASTPtr parameter;
    ASTPtr parameters;

    if (type)
    {
        auth_type_name = AuthenticationTypeInfo::get(*type).name;

        switch (*type)
        {
            case AuthenticationType::PLAINTEXT_PASSWORD:
            {
                prefix = "BY";
                password = children[0];
                break;
            }
            case AuthenticationType::SHA256_PASSWORD:
            {
                if (expect_hash)
                    auth_type_name = "sha256_hash";

                prefix = "BY";
                password = children[0];
                if (children.size() == 2)
                    salt = children[1];
                break;
            }
            case AuthenticationType::DOUBLE_SHA1_PASSWORD:
            {
                if (expect_hash)
                    auth_type_name = "double_sha1_hash";

                prefix = "BY";
                password = children[0];
                break;
            }
            case AuthenticationType::LDAP:
            {
                prefix = "SERVER";
                parameter = children[0];
                break;
            }
            case AuthenticationType::KERBEROS:
            {
                if (!children.empty())
                {
                    prefix = "REALM";
                    parameter = children[0];
                }
                break;
            }
            case AuthenticationType::SSL_CERTIFICATE:
            {
                prefix = "CN";
                parameters = children[0];
                break;
            }
            case AuthenticationType::NO_PASSWORD: [[fallthrough]];
            case AuthenticationType::MAX:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "AST: Unexpected authentication type {}", toString(*type));
        }
    }
    else
    {
        /// Default password type
        prefix = "BY";
        password = children[0];
    }

    if (password && !settings.show_secrets)
    {
        prefix = "";
        password.reset();
        salt.reset();
        if (type)
            auth_type_name = AuthenticationTypeInfo::get(*type).name;
    }

    settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " IDENTIFIED" << (settings.hilite ? IAST::hilite_none : "");

    if (!auth_type_name.empty())
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " WITH " << auth_type_name
                        << (settings.hilite ? IAST::hilite_none : "");
    }

    if (!prefix.empty())
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " " << prefix << (settings.hilite ? IAST::hilite_none : "");
    }

    if (password)
    {
        settings.ostr << " ";
        password->format(settings);
    }

    if (salt)
    {
        settings.ostr << " SALT ";
        salt->format(settings);
    }

    if (parameter)
    {
        settings.ostr << " ";
        parameter->format(settings);
    }
    else if (parameters)
    {
        settings.ostr << " ";
        parameters->format(settings);
    }
}

}
