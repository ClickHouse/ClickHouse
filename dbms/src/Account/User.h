#pragma once

#include <Account/Role.h>


namespace DB
{
/// Represents an user in Role-based Access Control.
class User2 : public Role
{
public:
    struct Attributes : public Role::Attributes
    {
        //PasswordHash password_hash;
        //AllowedHosts allowed_hosts;
        //SettingsChanges settings;

        Type getType() const override { return Type::USER; }
        std::shared_ptr<IAccessControlElement::Attributes> clone() const override;

    protected:
        bool equal(const IAccessControlElement::Attributes & other) const override;
    };

    using AttributesPtr = std::shared_ptr<const Attributes>;
    using Role::Role;

    AttributesPtr getAttributes() const { return getAttributesImpl<Attributes>(); }
    AttributesPtr tryGetAttributes() const { return tryGetAttributesImpl<Attributes>(); }

   /// Sets the default roles, i.e. roles which are set immediately after login.
    /// You can set only granted roles to be default.
    void setDefaultRoles(const std::vector<Role> & roles) { perform(setDefaultRolesOp(roles)); }
    Operation setDefaultRolesOp(const std::vector<Role> & roles) const;
    std::vector<Role> getDefaultRoles() const;

    class Context;
    virtual std::unique_ptr<Context> authorize();

private:
    Operation prepareOperation(const std::function<void(Attributes &)> & fn) const;
    const String & getTypeName() const override;
    int getNotFoundErrorCode() const override;
    int getAlreadyExistsErrorCode() const override;
};


class User2::Context
{
public:
};

}
