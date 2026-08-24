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
        auto clone = make_intrusive<ASTAuthenticationData>(*this);
        clone->cloneChildren();
        return clone;
    }

    bool hasSecretParts() const override;

    std::optional<String> getPassword() const;
    std::optional<String> getSalt() const;
    std::optional<String> ssl_cert_subject_type; /// CN or SubjectAltName

    /// If type is empty we use the default password type.
    /// AuthenticationType::NO_PASSWORD is specified explicitly.
    std::optional<AuthenticationType> type;

    bool contains_password = false;
    bool contains_hash = false;
    bool jwt_use_authenticator = false;
    /// IDENTIFIED WITH jwt accepts two optional clauses:
    ///   PROCESSOR '<token-processor-name>'
    ///   CLAIMS    '<json>'
    /// Both are stored in `children` in this order; flags below tell which slots
    /// are populated (children layout depends on which were specified). The
    /// processor pin is what protects against the H-14 / H-17 cache-priming
    /// bypass for SQL-declared JWT users; without it the per-user lookup goes
    /// through the iterate-all-processors auto-discovery path with empty pin.
    bool has_jwt_processor = false;
    bool has_jwt_claims = false;
    ASTPtr valid_until;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
