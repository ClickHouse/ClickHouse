#include <Parsers/Access/ASTAuthenticationData.h>

#include <Access/AccessControl.h>
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
    if (contains_password)
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
        if (const auto * salt = children[1]->as<const ASTLiteral>())
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
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " no_password"
                      << (settings.hilite ? IAST::hilite_none : "");
        return;
    }

    String auth_type_name;
    String prefix; /// "BY" or "SERVER" or "REALM"
    bool password = false; /// either a password or hash
    bool salt = false;
    bool parameter = false;
    bool parameters = false;
    bool scheme = false;

    if (type)
    {
        auth_type_name = AuthenticationTypeInfo::get(*type).name;

        switch (*type)
        {
            case AuthenticationType::PLAINTEXT_PASSWORD:
            {
                prefix = "BY";
                password = true;
                break;
            }
            case AuthenticationType::SHA256_PASSWORD:
            {
                if (contains_hash)
                    auth_type_name = "sha256_hash";

                prefix = "BY";
                password = true;
                if (children.size() == 2)
                    salt = true;
                break;
            }
            case AuthenticationType::DOUBLE_SHA1_PASSWORD:
            {
                if (contains_hash)
                    auth_type_name = "double_sha1_hash";

                prefix = "BY";
                password = true;
                break;
            }
            case AuthenticationType::JWT:
            {
                prefix = "CLAIMS";
                parameter = true;
                break;
            }
            case AuthenticationType::LDAP:
            {
                prefix = "SERVER";
                parameter = true;
                break;
            }
            case AuthenticationType::KERBEROS:
            {
                if (!children.empty())
                {
                    prefix = "REALM";
                    parameter = true;
                }
                break;
            }
            case AuthenticationType::SSL_CERTIFICATE:
            {
                prefix = ssl_cert_subject_type.value();
                parameters = true;
                break;
            }
            case AuthenticationType::BCRYPT_PASSWORD:
            {
                if (contains_hash)
                    auth_type_name = "bcrypt_hash";

                prefix = "BY";
                password = true;
                break;
            }
            case AuthenticationType::SSH_KEY:
            {
                prefix = "BY";
                parameters = true;
                break;
            }
            case AuthenticationType::HTTP:
            {
                prefix = "SERVER";
                parameter = true;
                if (children.size() == 2)
                    scheme = true;
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
        password = true;
    }

    if (password && !settings.show_secrets)
    {
        prefix = "";
        password = false;
        salt = false;
        if (type)
            auth_type_name = AuthenticationTypeInfo::get(*type).name;
    }

    if (!auth_type_name.empty())
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " " << auth_type_name << (settings.hilite ? IAST::hilite_none : "");
    }

    if (!prefix.empty())
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " " << prefix << (settings.hilite ? IAST::hilite_none : "");
    }

    if (password)
    {
        settings.ostr << " ";
        children[0]->format(settings);
    }

    if (salt)
    {
        settings.ostr << " SALT ";
        children[1]->format(settings);
    }

    if (parameter)
    {
        settings.ostr << " ";
        children[0]->format(settings);
    }
    else if (parameters)
    {
        settings.ostr << " ";
        bool need_comma = false;
        for (const auto & child : children)
        {
            if (std::exchange(need_comma, true))
                settings.ostr << ", ";
            child->format(settings);
        }
    }

    if (scheme)
    {
        settings.ostr << " SCHEME ";
        children[1]->format(settings);
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
