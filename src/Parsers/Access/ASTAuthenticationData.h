#pragma once

#include <Parsers/IAST.h>
#include <Access/Common/AuthenticationType.h>
#include <optional>


namespace DB
{

/** Represents authentication data in CREATE/ALTER USER query:
  *  ... IDENTIFIED WITH sha256_password BY 'password'
  *
  * Can store password, hash and salt, LDAP server name, Kerberos Realm, or common names.
  * They are stored in children vector as ASTLiteral or ASTQueryParameter.
  * ASTAuthenticationData without a type represents authentication data with
  *  the default password type that will be later inferred from the server parameters.
  */

class ASTAuthenticationData : public IAST
{
public:
    String getID(char) const override { return "AuthenticationData"; }

    ASTPtr clone() const override
    {
        auto clone = std::make_shared<ASTAuthenticationData>(*this);
        clone->cloneChildren();
        return clone;
    }

    bool hasSecretParts() const override;

    std::optional<String> getPassword() const;
    std::optional<String> getSalt() const;

    /// If type is empty we use the default password type.
    /// AuthenticationType::NO_PASSWORD is specified explicitly.
    std::optional<AuthenticationType> type;

    bool contains_password = false;
    bool contains_hash = false;

protected:
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
