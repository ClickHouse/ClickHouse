#pragma once

#include <Account/RoleAttributes.h>
#include <Poco/Net/IPAddress.h>


namespace DB
{
/// Attributes of a user. Every user can be used as a role.
struct UserAttributes : public RoleAttributes
{
    //UserPassword password;
    //UserAllowedHosts allowed_hosts;
    //Quotas quotas;

    //SettingsChanges settings;
    //SettingsConstraints settings_constraints;

    /*
    /// Password, if required.
    String password; /// in plaintext
    String password_sha256; /// in SHA256

    /// List of allowed addresses of the client hosts.
    struct IPAddressPattern
    {
        /// Address of mask. Always transformed to IPv6.
        Poco::Net::IPAddress mask_address;
        /// Mask of net (ip form). Always transformed to IPv6.
        Poco::Net::IPAddress subnet_mask;
    };
    std::vector<IPAddressPattern> allowed_addresses;
    std::vector<String> allowed_hosts;
    std::vector<String> allowed_host_regexps;
    */

    /// Verifies user's connection. This includes checking the client host from which the user connect and provided password.
    /// Throws an exception on fail.
    virtual void authorize(const String & password_, const Poco::Net::IPAddress & address_);

    UserAttributes() : RoleAttributes(Type::USER) {}
    std::shared_ptr<IAccessAttributes> clone() const override;
    bool isEqual(const IAccessAttributes & other) const override;
protected:
    void copyTo(RoleAttributes & dest) const;
};

using UserAttributesPtr = std::shared_ptr<const UserAttributes>;
}
