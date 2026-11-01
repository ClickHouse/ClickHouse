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

    ASTPtr clone() const override;

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
    /// The method-level deadline from `VALID UNTIL <datetime>` or `VALID FOR <interval>`.
    /// It is registered in `children` (always after the payload children), so that the generic
    /// AST machinery - depth/size limits, clone-based visitors - sees the subtree; assign it
    /// only through `setValidUntil` to keep the two in sync.
    ASTPtr valid_until;
    /// If true, `valid_until` holds an interval expression coming from `VALID FOR <interval>`
    /// (the deadline is `now` plus the interval); otherwise it holds a `VALID UNTIL` value.
    bool valid_until_is_interval = false;

    void setValidUntil(ASTPtr ast);

    /// The number of children that carry the authentication payload (password, salt, parameters,
    /// SSH keys, HTTP scheme). They always precede `valid_until`, which, when present, is also
    /// stored in `children` and must be excluded from positional payload access.
    size_t numPayloadChildren() const { return children.size() - (valid_until ? 1 : 0); }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
    void forEachPointerToChild(std::function<void(IAST **, boost::intrusive_ptr<IAST> *)> f) override;
};

}
