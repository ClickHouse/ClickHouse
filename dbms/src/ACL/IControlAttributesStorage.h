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
/// Contains the attributes of access control's elements: users, roles, quotas.
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

    /// Searchs for a attributes with specified name and type.
    virtual std::optional<UUID> find(const String & name, const Type & type) const = 0;

    /// Returns whether there are attributes with such ID in the storage.
    virtual bool exists(const UUID & id) const = 0;

    /// Reads the attributes. Throws an exception if not found.
    AttributesPtr read(const UUID & id, const Type & type) const;

    template <typename AttributesT>
    void read(const UUID & id, std::shared_ptr<const AttributesT> & attrs) const;

    /// Reads the attributes. Returns nullptr if not found.
    AttributesPtr tryRead(const UUID & id) const;
    AttributesPtr tryRead(const UUID & id, const Type & type) const;

    template <typename AttributesT>
    void tryRead(const UUID & id, std::shared_ptr<const AttributesT> & attrs) const;

    /// Inserts attributes to the storage.
    /// Returns `{id, true}` if successfully inserted or `{id, false}` if such name is already used by `id`.
    virtual std::pair<UUID, bool> insert(const Attributes & attrs) = 0;

    /// Drops the attributes from the storage. Returns true if successfully dropped, or false if not found.
    virtual bool remove(const UUID & id) = 0;

    /// Updates the attributes in the storage.
    void update(const UUID & id, const Type & type, const std::function<void(Attributes &)> & update_func);

    /// Updates the attributes in the storage.
    /// `UpdateFunc` must be declared as `void updateFunc(AttributesT & attrs);` where `AttributesT` is a class
    /// derived from IControlAttributes.
    template <typename UpdateFunc>
    void update(const UUID & id, const UpdateFunc & update_func);

    class Subscription
    {
    public:
        virtual ~Subscription() {}
    };

    using SubscriptionPtr = std::unique_ptr<Subscription>;

    /// Subscribes for new entries.
    using OnNewHandler = std::function<void(const UUID &)>;
    SubscriptionPtr subscribeForNew(const Type & type, const OnNewHandler & on_new) const { return subscribeForNewImpl(String{}, type, on_new); }
    SubscriptionPtr subscribeForNew(const String & prefix, const Type & type, const OnNewHandler & on_new) const { return subscribeForNewImpl(prefix, type, on_new); }

    /// Subscribes for changes.
    /// Returns nullptr if there is no such id in the storage or the storage is read-only.
    /// The `on_changed` function should be declared like this:
    /// void onChanged(const std::shared_ptr<const Attributes> & attrs);
    /// where `Attributes` is any type derived from IControlAttributes.
    template <typename Func>
    SubscriptionPtr subscribeForChanges(const UUID & id, const Func & on_changed);

protected:
    virtual AttributesPtr tryReadImpl(const UUID & id) const = 0;
    virtual void updateImpl(const UUID & id, const Type & type, const std::function<void(Attributes &)> & update_func) = 0;

    using OnChangedHandler = std::function<void(const AttributesPtr &)>;
    virtual SubscriptionPtr subscribeForChangesImpl(const UUID & id, const OnChangedHandler & on_changed) const = 0;
    virtual SubscriptionPtr subscribeForNewImpl(const String & prefix, const Type & type, const OnNewHandler & on_new) const = 0;

    static UUID generateRandomID();
    [[noreturn]] static void throwNotFound(const UUID & id, const Type & type);
    [[noreturn]] static void throwCannotRenameNewNameIsUsed(const String & name, const Type & type, const String & existing_name, const Type & existing_type);
};


template <typename AttributesT>
void IControlAttributesStorage::read(const UUID & id, std::shared_ptr<const AttributesT> & attrs) const
{
    static const Type type = AttributesT::TYPE;
    attrs = std::static_pointer_cast<const AttributesT>(read(id, type));
}


template <typename AttributesT>
void IControlAttributesStorage::tryRead(const UUID & id, std::shared_ptr<const AttributesT> & attrs) const
{
    static const Type type = AttributesT::TYPE;
    attrs = std::static_pointer_cast<const AttributesT>(tryRead(id, type));
}


template <typename Func>
IControlAttributesStorage::SubscriptionPtr IControlAttributesStorage::subscribeForChanges(const UUID & id, const Func & on_changed)
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
