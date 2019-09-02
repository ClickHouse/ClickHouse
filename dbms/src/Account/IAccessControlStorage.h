#pragma once

#include <Account/IAccessControlElement.h>
#include <Core/Types.h>
#include <Core/UUID.h>
#include <functional>
#include <vector>
#include <optional>


namespace DB
{
/// Contains the attributes of users and/or roles.
class IAccessControlStorage
{
public:
    enum class Status
    {
        OK,
        NOT_FOUND,
        ALREADY_EXISTS,
    };

    using Attributes = IAccessControlElement::Attributes;
    using AttributesPtr = IAccessControlElement::AttributesPtr;
    using ElementType = IAccessControlElement::Type;

    IAccessControlStorage() {}
    virtual ~IAccessControlStorage() {}

    /// Returns the name of this storage.
    virtual const String & getStorageName() const = 0;

    /// Returns the identifiers of all the attributes of a specified type contained in the storage.
    virtual std::vector<UUID> findAll(ElementType type) const = 0;

    /// Searchs for a attributes with specified name and type.
    /// Returns zero if not found.
    virtual UUID find(const String & name, ElementType type) const = 0;

    /// Returns whether there are attributes with such ID in the storage.
    virtual bool exists(const UUID & id) const = 0;

    /// Inserts attributes to the storage.
    /// Returns either OK or ALREADY_EXISTS.
    virtual Status insert(const Attributes & attrs, UUID & id) = 0;

    /// Removes attributes.
    /// Returns either OK or NOT_FOUND.
    virtual Status remove(const UUID & id) = 0;

    /// Reads the attributes.
    /// Returns either OK or NOT_FOUND.
    virtual Status read(const UUID & id, AttributesPtr & attrs) const = 0;

    using MakeChangeFunction = std::function<void(Attributes &)>;

    /// Changes the attributes.
    /// Returns either OK or NOT_FOUND or ALREADY_EXISTS.
    virtual Status write(const UUID & id, const MakeChangeFunction & make_change) = 0;

    class Subscription
    {
    public:
        virtual ~Subscription() {}
    };

    /// Called after something has been inserted to the storage.
    using OnNewAttributesFunction = std::function<void(const UUID & id)>;

    /// Called after the attributes with the specified ID are changed or removed from the storage.
    using OnChangedFunction = std::function<void(const AttributesPtr &)>;

    /// Subscribes for new attributes in the storage.
    /// The function can returns nullptr if failed to subscribe.
    virtual std::unique_ptr<Subscription> subscribeForNew(ElementType type, const OnNewAttributesFunction & on_new_attrs) const = 0;

    /// Subscribes for changes. Returns nullptr if there is no such id in the storage or the storage is read-only.
    /// The function can returns nullptr if failed to subscribe.
    virtual std::unique_ptr<Subscription> subscribeForChanges(const UUID & id, const OnChangedFunction & on_changed) const = 0;
};

using AccessControlStoragePtr = std::shared_ptr<IAccessControlStorage>;
}
