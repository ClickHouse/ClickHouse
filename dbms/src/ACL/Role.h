#pragma once

#include <ACL/IControlAttributes.h>
#include <ACL/AllowedDatabases.h>
#include <functional>


namespace DB
{
class IControlAttributesStorage;


/// Represents a role in Role-based Access Control.
/// Syntax:
/// CREATE ROLE [IF NOT EXISTS] name
///
/// DROP ROLE [IF EXISTS] name
class ConstRole
{
public:
    struct Attributes : public IControlAttributes
    {
        AllowedDatabases allowed_databases;
        static const Type TYPE;
        const Type & getType() const override { return TYPE; }
        std::shared_ptr<IControlAttributes> clone() const override { return cloneImpl<Attributes>(); }
        bool equal(const IControlAttributes & other) const override;
    };

    using AttributesPtr = std::shared_ptr<const Attributes>;
    using Storage = IControlAttributesStorage;
    using Type = Attributes::Type;
    static const Type & TYPE;

    ConstRole(const UUID & id_, const Storage & storage_) : id(id_), storage(storage_) {}

    AttributesPtr getAttributes() const;
    AttributesPtr tryGetAttributes() const;

protected:
    UUID id;
    const Storage & storage;
};



class Role : public ConstRole
{
public:
    Role(const UUID & id_, Storage & storage_) : ConstRole(id_, storage_) {}

    void update(const std::function<void(Attributes &)> & update_func);
    void drop(bool if_exists);

protected:
    Storage & getStorage() { return const_cast<Storage &>(storage); }
};
}
