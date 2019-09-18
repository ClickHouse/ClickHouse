#pragma once

#include <ACL/IControlAttributes.h>
#include <Core/Types.h>
#include <Core/UUID.h>
#include <functional>
#include <optional>
#include <vector>
#include <boost/function_types/parameter_types.hpp>
#include <boost/mpl/at.hpp>


namespace DB
{
/// Contains attributes, i.e. instances of classes derived from IControlAttributes.
/// The implementations of this class MUST be thread-safe.
class IControlAttributesStorage
{
public:
    using Attributes = IControlAttributes;
    using AttributesPtr = ControlAttributesPtr;
    using Type = Attributes::Type;

    IControlAttributesStorage() {}
    virtual ~IControlAttributesStorage() {}

    /// Returns the name of this storage.
    virtual const String & getStorageName() const = 0;

    /// Returns the identifiers of all the attributes of a specified type contained in the storage.
    std::vector<UUID> findAll(const Type & type) const { return findPrefixed(String{}, type); }

    /// Returns the identifiers of the attributes which names have a specified prefix.
    virtual std::vector<UUID> findPrefixed(const String & prefix, const Type & type) const = 0;

    /// Searchs for attributes with specified name and type. Returns std::nullopt if not found.
    virtual std::optional<UUID> find(const String & name, const Type & type) const = 0;

    /// Returns whether there are attributes with such identifier in the storage.
    virtual bool exists(const UUID & id) const = 0;

    /// Searchs for attributes with specified name and type. Throws an exception if not found.
    UUID getID(const String & name, const Type & type) const;

    /// Inserts attributes to the storage. Throws an exception if the specified name is already in use.
    UUID insert(const Attributes & attrs);

    /// Inserts attributes to the storage.
    /// Returns `{id, true}` if successfully inserted or `{id, false}` if the specified name is already in use.
    std::pair<UUID, bool> tryInsert(const Attributes & attrs);
    std::pair<UUID, bool> tryInsert(const Attributes & attrs, AttributesPtr & caused_name_collision);

    /// Removes the attributes from the storage. Throws an exception if not found.
    void remove(const UUID & id, const Type & type);

    /// Removes the attributes from the storage. Returns true if successfully dropped, or false if not found.
    bool tryRemove(const UUID & id);

    /// Reads the attributes. Throws an exception if not found.
    AttributesPtr read(const UUID & id, const Type & type) const;

    template <typename AttributesT>
    std::shared_ptr<const AttributesT> read(const UUID & id) const;

    /// Reads the attributes. Returns nullptr if not found.
    AttributesPtr tryRead(const UUID & id) const;
    AttributesPtr tryRead(const UUID & id, const Type & type) const;

    template <typename AttributesT>
    std::shared_ptr<const AttributesT> tryRead(const UUID & id) const;

    /// Updates the attributes in the storage.
    void update(const UUID & id, const Type & type, const std::function<void(Attributes &)> & update_func);

    template <typename AttributesT>
    void update(const UUID & id, const std::function<void(AttributesT &)> & update_func);

    /// Updates the attributes in the storage.
    /// `UpdateFunc` must be declared as `void updateFunc(AttributesT & attrs);` where `AttributesT` is a class
    /// derived from IControlAttributes.
    template <typename Func>
    void update(const UUID & id, const Func & update_func);

    class Subscription
    {
    public:
        virtual ~Subscription() {}
    };

    using SubscriptionPtr = std::unique_ptr<Subscription>;

    /// Subscribes for new attributes.
    /// Can return nullptr if cannot subscribe or if it doesn't make sense (the storage is read-only).
    using OnNewHandler = std::function<void(const UUID &)>;
    SubscriptionPtr subscribeForNew(const Type & type, const OnNewHandler & on_new) const { return subscribeForNewImpl(String{}, type, on_new); }
    SubscriptionPtr subscribeForNew(const String & prefix, const Type & type, const OnNewHandler & on_new) const { return subscribeForNewImpl(prefix, type, on_new); }

    /// Subscribes for changes.
    /// Can return nullptr if cannot subscribe (identifier not found) or if it doesn't make sense (the storage is read-only).
    using OnChangedHandler = std::function<void(const AttributesPtr &)>;
    SubscriptionPtr subscribeForChanges(const UUID & id, const OnChangedHandler & on_changed) const;

    template <typename AttributesT>
    SubscriptionPtr subscribeForChanges(const UUID & id, const std::function<void(const std::shared_ptr<const AttributesT> &)> & on_changed) const;

    /// Subscribes for changes.
    /// Can return nullptr if cannot subscribe (identifier not found) or if it doesn't make sense (the storage is read-only).
    /// The `on_changed` function should be declared as `void onChanged(const std::shared_ptr<const AttributesT> & attrs);`
    /// where `AttributesT` is a class derived from IControlAttributes.
    template <typename Func>
    SubscriptionPtr subscribeForChanges(const UUID & id, const Func & on_changed) const;

protected:
    virtual std::pair<UUID, bool> tryInsertImpl(const Attributes & attrs, AttributesPtr & caused_name_collision) = 0;
    virtual bool tryRemoveImpl(const UUID & id) = 0;
    virtual AttributesPtr tryReadImpl(const UUID & id) const = 0;
    virtual void updateImpl(const UUID & id, const Type & type, const std::function<void(Attributes &)> & update_func) = 0;

    virtual SubscriptionPtr subscribeForChangesImpl(const UUID & id, const OnChangedHandler & on_changed) const = 0;
    virtual SubscriptionPtr subscribeForNewImpl(const String & prefix, const Type & type, const OnNewHandler & on_new) const = 0;

    static UUID generateRandomID();
    [[noreturn]] static void throwNotFound(const String & name, const Type & type);
    [[noreturn]] static void throwNotFound(const UUID & id, const Type & type);
    [[noreturn]] static void throwCannotRenameNewNameIsUsed(const String & name, const Type & type, const String & existing_name, const Type & existing_type);
};


template <typename AttributesT>
std::shared_ptr<const AttributesT> IControlAttributesStorage::read(const UUID & id) const
{
    static const Type type = AttributesT::TYPE;
    return std::static_pointer_cast<const AttributesT>(read(id, type));
}


template <typename AttributesT>
std::shared_ptr<const AttributesT> IControlAttributesStorage::tryRead(const UUID & id) const
{
    static const Type type = AttributesT::TYPE;
    return std::static_pointer_cast<const AttributesT>(tryRead(id, type));
}


template <typename AttributesT>
void IControlAttributesStorage::update(const UUID & id, const std::function<void(AttributesT &)> & update_func)
{
    updateImpl(id, AttributesT::TYPE, [update_func](Attributes & attrs) { update_func(*attrs.cast<AttributesT>()); });
}


template <typename Func>
void IControlAttributesStorage::update(const UUID & id, const Func & update_func)
{
    using ParamTypes = typename boost::function_types::parameter_types<decltype(&Func::operator())>::type;
    using ArgType = typename boost::mpl::at<ParamTypes, boost::mpl::int_<1> >::type;
    using AttributesT = std::remove_reference_t<ArgType>;

    updateImpl(id, AttributesT::TYPE, [update_func](Attributes & attrs) { update_func(*attrs.cast<AttributesT>()); });
}


template <typename AttributesT>
IControlAttributesStorage::SubscriptionPtr IControlAttributesStorage::subscribeForChanges(
    const UUID & id, const std::function<void(const std::shared_ptr<const AttributesT> &)> & on_changed) const
{
    return subscribeForChangesImpl(id, [on_changed](const AttributesPtr & attrs)
    {
        auto casted_attrs = attrs ? attrs->tryCast<const AttributesT>() : nullptr;
        on_changed(casted_attrs);
    });
}


template <typename Func>
IControlAttributesStorage::SubscriptionPtr IControlAttributesStorage::subscribeForChanges(const UUID & id, const Func & on_changed) const
{
    using ParamTypes = typename boost::function_types::parameter_types<decltype(&Func::operator())>::type;
    using ArgType = typename boost::mpl::at<ParamTypes, boost::mpl::int_<1> >::type;
    using AttributesT = typename std::remove_const_t<std::remove_reference_t<ArgType>>::element_type;

    return subscribeForChangesImpl(id, [on_changed](const AttributesPtr & attrs)
    {
        auto casted_attrs = attrs ? attrs->tryCast<const AttributesT>() : nullptr;
        on_changed(casted_attrs);
    });
}
}
