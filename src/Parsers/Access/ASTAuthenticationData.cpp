#include <Parsers/Access/ASTAuthenticationData.h>

#include <Access/AccessControl.h>
#include <Access/Common/AuthenticationData.h>
#include <Common/Exception.h>
#include <Parsers/ASTLiteral.h>
#include <IO/Operators.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

std::optional<String> ASTAuthenticationData::getPassword() const
{
    if (expect_password)
    {
        if (const auto * password = children[0]->as<const ASTLiteral>())
        {
            return password->value.safeGet<String>();
        }
    }

    return {};
}
std::optional<String> ASTAuthenticationData::getSalt() const
{
    if (type && *type == AuthenticationType::SHA256_PASSWORD && children.size() == 2)
    {
        if (const auto * salt = children[0]->as<const ASTLiteral>())
        {
            return salt->value.safeGet<String>();
        }
    }

    return {};
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

bool ASTAuthenticationData::hasSecretParts() const
{
    /// Default password type is used hence secret part
    if (!type)
        return true;

    auto auth_type = *type;
    if ((auth_type == AuthenticationType::PLAINTEXT_PASSWORD)
        || (auth_type == AuthenticationType::SHA256_PASSWORD)
        || (auth_type == AuthenticationType::DOUBLE_SHA1_PASSWORD))
        return true;

    return childrenHaveSecretParts();
}

}
