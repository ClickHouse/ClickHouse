#pragma once

#include <Account/IAccessAttributes.h>
#include <Core/Types.h>
#include <Core/UUID.h>
#include <functional>
#include <vector>
#include <optional>


namespace DB
{
/// Contains the attributes of users and/or roles.
class IAccessStorage
{
public:
    IAccessStorage() {}
    virtual ~IAccessStorage() {}

    /// Returns the name of this storage.
    virtual const String & getStorageName() const = 0;

    using Attributes = IAccessAttributes;
    using AttributesPtr = AccessAttributesPtr;
    using Type = Attributes::Type;

    /// Returns the identifiers of all the attributes of a specified type contained in the storage.
    virtual std::vector<UUID> findAll(Type type) const = 0;

    /// Searchs for a attributes with specified name and type.
    virtual std::optional<UUID> find(const String & name, Type type) const = 0;

    /// Returns whether there are attributes of a user with such ID in the storage.
    virtual bool exists(const UUID & id) const = 0;

    /// Inserts attributes of a new user to the storage. Throws an exception if cannot create.
    virtual UUID create(const Attributes & initial_attrs) = 0;

    /// Removes attributes from the storage. Does nothing if there is no such user.
    virtual void drop(const UUID & id) = 0;

    /// Reads the attributes of a user.
    virtual AttributesPtr read(const UUID & id) const = 0;

    class Subscription
    {
    public:
        virtual ~Subscription() {}
    };

    using OnChangedFunction = std::function<void(UUID id, const AttributesPtr &)>;

    /// Reads the attributes of a user and subscribes for changes.
    virtual std::pair<AttributesPtr, std::unique_ptr<Subscription>>
    readAndSubscribe(const UUID & id, const OnChangedFunction & on_changed) const = 0;

    using MakeChangeFunction = std::function<void(Attributes &)>;

    /// Changes the attributes of a user.
    virtual void write(const UUID & id, const MakeChangeFunction & make_change) = 0;

protected:
    [[noreturn]] static void throwAlreadyExists(const String & name, Type type);
    [[noreturn]] static void throwNotFound(const UUID & id);
};

using AccessStoragePtr = std::shared_ptr<IAccessStorage>;
}
