#pragma once

#include <Parsers/IAST.h>
#include <Access/Common/AuthenticationData.h>
#include <optional>


namespace DB
{

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

    /// TODO: Only expect_password and expect_hash are actually needed
    bool expect_password = false;
    bool expect_hash = false;
    bool expect_ldap_server_name = false;
    bool expect_kerberos_realm = false;
    bool expect_common_names = false;

protected:
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
