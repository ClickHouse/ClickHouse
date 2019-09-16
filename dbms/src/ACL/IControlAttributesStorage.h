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

    enum class ChangeType
    {
        INSERT, /// insert new attributes of type `attributes_type` with `id`, throw an exception if this `id` is already in use
        UPDATE, /// update existing attributes, throw an exception if the new name is already in use
        REMOVE, /// remove existing attributes, throw an exception if they don't exist and `if_exists == false`
        REMOVE_REFERENCES, /// update all attributes having references to this `id`, remove these references
    };

    struct Change
    {
        ChangeType change_type = ChangeType::UPDATE;
        UUID id = UUID(UInt128(0));
        const Type * type = nullptr; /// not used if type == REMOVE_REFERENCES
        std::function<void(IControlAttributes &)> update_func; /// only used if type == UPDATE
        bool if_exists = false; /// don't throw if entry isn't found. Only used if type == UPDATE or REMOVE
    };

    using Changes = std::vector<Change>;

    /// Changes the attributes atomically.
    virtual void write(const Changes & changes) = 0;

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

    using OnChangedHandler = std::function<void(const AttributesPtr &)>;
    virtual SubscriptionPtr subscribeForChangesImpl(const UUID & id, const OnChangedHandler & on_changed) const = 0;
    virtual SubscriptionPtr subscribeForNewImpl(const String & prefix, const Type & type, const OnNewHandler & on_new) const = 0;

    [[noreturn]] static void throwNotFound(const UUID & id, const Type & type);
    [[noreturn]] static void throwNotFound(const String & name, const Type & type);
    [[noreturn]] static void throwCannotInsertIDIsUsed(const UUID & id, const Type & type, const String & existing_name, const Type & existing_type);
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
