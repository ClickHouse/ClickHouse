#pragma once

#include <ACL/Role.h>


namespace DB
{
/// Represents an user in Role-based Access Control.
class ConstUser : public ConstRole
{
public:
    struct Attributes : public ConstRole::Attributes
    {
        //PasswordHash password_hash;
        //AllowedHosts allowed_hosts;

        //std::unordered_set<UUID> default_roles;
        //bool default_all_roles = false;

        //SettingsChanges settings;

        static const Type TYPE;
        const Type & getType() const override { return TYPE; }
        std::shared_ptr<IAttributes> clone() const override { return cloneImpl<Attributes>(); }
        bool equal(const IAttributes & other) const override;
        bool hasReferences(const UUID & id) const override;
        void removeReferences(const UUID & id) override;
    };

    using AttributesPtr = std::shared_ptr<const Attributes>;
    using Storage = IAttributesStorage;
    using Type = Attributes::Type;
    static const Type & TYPE;

    ConstUser(const UUID & id_, const Storage & storage_) : ConstRole(id_, storage_) {}
    AttributesPtr getAttributes() const;
    AttributesPtr tryGetAttributes() const;
};



class User2 : public ConstUser
{
public:
    User2(const UUID & id_, Storage & storage_) : ConstUser(id_, storage_) {}

    void update(const std::function<void(Attributes &)> & update_func);
    void drop(bool if_exists);

protected:
    Storage & getStorage() { return const_cast<Storage &>(storage); }
};
}
