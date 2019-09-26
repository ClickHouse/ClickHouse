#pragma once

#include <ACL/IAttributes.h>
#include <ACL/AllowedDatabases.h>
#include <Parsers/IAST_fwd.h>
#include <functional>
#include <unordered_set>


namespace DB
{
class IAttributesStorage;


template <AttributesT>
class AttributableImpl
{
    using Attributes = AttributesT;
    using AttributesPtr = std::shared_ptr<const Attributes>;
    using Storage = IAttributesStorage;
    using Type = Attributes::Type;
    static const Type & TYPE;

    AttributableImpl(const UUID & id_, Storage & storage_, bool can_modify_storage_) : id(id_), storage(storage_), can_modify_storage(can_modify_storage_) {}
    const UUID & getID() const { return id; }

    AttributesPtr getAttributes() const;
    AttributesPtr tryGetAttributes() const;

    void update(const std::function<void(Attributes &)> & update_func);
    bool drop(bool if_exists);

protected:
    const UUID id;
    Storage & storage;
    bool can_modify_storage = true;
};


struct RoleAttributes : public IAttributes
{
    AllowedDatabases allowed_databases_by_grant_option[2 /* 0 - without grant option, 1 - with grant option */];
    std::unordered_set<UUID> granted_roles_by_admin_option[2 /* 0 - without admin option, 1 - with admin option */];

    static const Type TYPE;
    const Type & getType() const override { return TYPE; }
    std::shared_ptr<IAttributes> clone() const override { return cloneImpl<Attributes>(); }
    bool equal(const IAttributes & other) const override;
    bool hasReferences(const UUID & id) const override;
    void removeReferences(const UUID & id) override;
};


class Role : public AttributableImpl<RoleAttributes>
{
public:
    using AttributableImpl<RoleAttributes>::AttributableImpl<RoleAttributes>;

    std::vector<ASTPtr> getGrantQueries() const;
};


/// Represents a role in Role-based Access Control.
/// Syntax:
/// CREATE ROLE [IF NOT EXISTS] name
///
/// DROP ROLE [IF EXISTS] name
class ConstRole
{
public:
    struct Attributes : public IAttributes
    {
        AllowedDatabases allowed_databases_by_grant_option[2 /* 0 - without grant option, 1 - with grant option */];
        std::unordered_set<UUID> granted_roles_by_admin_option[2 /* 0 - without admin option, 1 - with admin option */];

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

    ConstRole(const UUID & id_, const Storage & storage_) : id(id_), storage(storage_) {}
    const UUID & getID() const { return id; }

    AttributesPtr getAttributes() const;
    AttributesPtr tryGetAttributes() const;

    std::vector<ASTPtr> getGrantQueries() const;

protected:
    const UUID id;
    const Storage & storage;
};



class Role : public ConstRole
{
public:
    Role(const UUID & id_, Storage & storage_) : ConstRole(id_, storage_) {}

    void update(const std::function<void(Attributes &)> & update_func);
    bool drop(bool if_exists);

protected:
    Storage & getStorage() { return const_cast<Storage &>(storage); }
};
}
