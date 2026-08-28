#include <Parsers/Access/ASTAuthenticationData.h>

#include <Common/Exception.h>
#include <Parsers/ASTLiteral.h>
#include <IO/Operators.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
    void formatValidUntil(const IAST & valid_until, bool is_interval, WriteBuffer & ostr, const IAST::FormatSettings & settings)
    {
        ostr << (is_interval ? " VALID FOR " : " VALID UNTIL ");
        valid_until.format(ostr, settings);
    }

    void formatGrants(const AccessRightsElements & grants, WriteBuffer & ostr)
    {
        ostr << " GRANTS (";
        if (grants.empty())
            /// The clause is present (checked by the caller with structurallyEmpty()) but grants nothing,
            /// e.g. `GRANTS (USAGE ON *.*)`. `formatElementsWithoutOptions` skips zero-flag elements, which
            /// would produce an empty and unparseable `GRANTS ()`, so emit the canonical no-privileges form.
            ostr << "USAGE ON *.*";
        else
            /// Render precisely: auth-method grants must never be widened by the backward-compatibility
            /// rewrites, otherwise a narrow token grant such as `ALTER USER ON alice` would round-trip as
            /// `ALTER USER ON *.*` through `SHOW CREATE USER`, backup, restart, or `ATTACH USER` and become
            /// broader. Older replicas cannot parse this clause anyway, so there is no compatibility to keep.
            grants.formatElementsWithoutOptions(ostr, /*precise=*/true);
        ostr << ")";
    }
}

ASTPtr ASTAuthenticationData::clone() const
{
    auto res = make_intrusive<ASTAuthenticationData>(*this);
    res->children.clear();
    res->valid_until = nullptr;

    for (const auto & child : children)
    {
        auto child_clone = child->clone();
        if (valid_until && child.get() == valid_until.get())
            res->valid_until = child_clone;
        res->children.push_back(std::move(child_clone));
    }

    return res;
}

void ASTAuthenticationData::setValidUntil(ASTPtr ast)
{
    if (!ast)
        return;
    setOrReplace(valid_until, std::move(ast));
}

void ASTAuthenticationData::forEachPointerToChild(std::function<void(IAST **, boost::intrusive_ptr<IAST> *)> f)
{
    f(nullptr, &valid_until);
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
    if (type && (*type == AuthenticationType::SHA256_PASSWORD || *type == AuthenticationType::SCRAM_SHA256_PASSWORD) && numPayloadChildren() == 2)
    {
        if (const auto * salt = children[1]->as<const ASTLiteral>())
        {
            return salt->value.safeGet<String>();
        }
    }

    return {};
}

void ASTAuthenticationData::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    if (type && *type == AuthenticationType::NO_PASSWORD)
    {
        ostr << " no_password"
                     ;

        if (valid_until)
        {
            formatValidUntil(*valid_until, valid_until_is_interval, ostr, settings);
        }

        if (!grants.structurallyEmpty())
        {
            formatGrants(grants, ostr);
        }

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
                if (numPayloadChildren() == 2)
                    salt = true;
                break;
            }
            case AuthenticationType::SCRAM_SHA256_PASSWORD:
            {
                if (contains_hash)
                    auth_type_name = "scram_sha256_hash";

                prefix = "BY";
                password = true;
                if (numPayloadChildren() == 2)
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
                prefix = jwt_use_authenticator ? "AUTHENTICATOR" : "CLAIMS";
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
                if (numPayloadChildren() != 0)
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
                if (numPayloadChildren() == 2)
                    scheme = true;
                break;
            }
            case AuthenticationType::NO_AUTHENTICATION:
                break;
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
        ostr << " " << auth_type_name;
    }

    if (!prefix.empty())
    {
        ostr << " " << prefix;
    }

    if (password)
    {
        ostr << " ";
        children[0]->format(ostr, settings);
    }

    if (salt)
    {
        ostr << " SALT ";
        children[1]->format(ostr, settings);
    }

    if (parameter)
    {
        ostr << " ";
        children[0]->format(ostr, settings);
    }
    else if (parameters)
    {
        ostr << " ";
        bool need_comma = false;
        for (size_t i = 0; i < numPayloadChildren(); ++i)
        {
            if (std::exchange(need_comma, true))
                ostr << ", ";
            children[i]->format(ostr, settings);
        }
    }

    if (scheme)
    {
        ostr << " SCHEME ";
        children[1]->format(ostr, settings);
    }

    if (valid_until)
    {
        formatValidUntil(*valid_until, valid_until_is_interval, ostr, settings);
    }

    if (!grants.structurallyEmpty())
    {
        formatGrants(grants, ostr);
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
        || (auth_type == AuthenticationType::SCRAM_SHA256_PASSWORD)
        || (auth_type == AuthenticationType::DOUBLE_SHA1_PASSWORD)
        || (auth_type == AuthenticationType::BCRYPT_PASSWORD))
        return true;

    return childrenHaveSecretParts();
}

}
