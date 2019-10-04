#pragma once

#include <Access/IAttributes.h>
#include <Core/Types.h>
#include <Core/UUID.h>
#include <functional>
#include <optional>
#include <vector>
#include <boost/function_types/parameter_types.hpp>
#include <boost/mpl/at.hpp>


namespace DB
{
/// Contains attributes, i.e. instances of classes derived from IAttributes.
/// The implementations of this class MUST be thread-safe.
class IAttributesStorage
{
public:
    using Type = IAttributes::Type;

    IAttributesStorage() {}
    virtual ~IAttributesStorage() {}

    /// Returns the name of this storage.
    virtual const String & getStorageName() const = 0;

    /// Returns the identifiers of all the attributes of a specified type contained in the storage.
    template <typename AttributesT>
    std::vector<UUID> findAll() const { return findAll(AttributesT::TYPE); }
    std::vector<UUID> findAll(const Type & type) const { return findPrefixed(String{}, type); }

    /// Returns the identifiers of the attributes which names have a specified prefix.
    template <typename AttributesT>
    std::vector<UUID> findPrefixed(const String & prefix) const { return findPrefixed(prefix, AttributesT::TYPE); }
    std::vector<UUID> findPrefixed(const String & prefix, const Type & type) const { return findPrefixedImpl(prefix, type); }

    /// Searchs for attributes with specified name and type. Returns std::nullopt if not found.
    template <typename AttributesT>
    std::optional<UUID> find(const String & name) const { return findImpl(name, AttributesT::TYPE); }
    std::optional<UUID> find(const String & name, const Type & type) const { return findImpl(name, type); }

    /// Returns whether there are attributes with such identifier in the storage.
    bool exists(const UUID & id) const { return existsImpl(id); }

    /// Searchs for attributes with specified name and type. Throws an exception if not found.
    template <typename AttributesT>
    UUID getID(const String & name) const { return getID(name, AttributesT::TYPE); }
    UUID getID(const String & name, const Type & type) const;

    /// Reads the attributes. Throws an exception if not found.
    template <typename AttributesT = IAttributes>
    std::shared_ptr<const AttributesT> read(const UUID & id) const;

    template <typename AttributesT = IAttributes>
    std::shared_ptr<const AttributesT> read(const String & name) const;

    /// Reads the attributes. Returns nullptr if not found.
    template <typename AttributesT = IAttributes>
    std::shared_ptr<const AttributesT> tryRead(const UUID & id) const;

    template <typename AttributesT = IAttributes>
    std::shared_ptr<const AttributesT> tryRead(const String & name) const;

    /// Reads only name and type of the attributes.
    String readName(const UUID & id) const;
    std::optional<String> tryReadName(const UUID & id) const;

    /// Inserts attributes to the storage. If the specified name is already in use and `replace_if_exists == false` throws an exception.
    UUID insert(const IAttributes & attrs, bool replace_if_exists = false);
    UUID insert(const AttributesPtr & attrs, bool replace_if_exists = false);
    std::vector<UUID> insert(const std::vector<AttributesPtr> & multiple_attrs, bool replace_if_exists = false);

    /// Inserts attributes to the storage.
    /// Returns ID if successfully inserted or `nullopt` if the specified name is already in use.
    std::optional<UUID> tryInsert(const IAttributes & attrs);
    std::optional<UUID> tryInsert(const AttributesPtr & attrs);
    std::vector<std::optional<UUID>> tryInsert(const std::vector<AttributesPtr> & multiple_attrs);

    /// Removes the attributes from the storage. Throws an exception if not found.
    void remove(const UUID & id) { removeImpl(id); }
    void remove(const std::vector<UUID> & ids);

    template <typename AttributesT>
    void remove(const String & name) { remove(name, AttributesT::TYPE); }
    void remove(const String & name, const Type & type) { removeImpl(getID(name, type)); }

    template <typename AttributesT>
    void remove(const Strings & names) { remove(names, AttributesT::TYPE); }
    void remove(const Strings & names, const Type & type);

    /// Removes the attributes from the storage. Returns true if successfully dropped, or false if not found.
    bool tryRemove(const UUID & id);
    void tryRemove(const std::vector<UUID> & ids, std::vector<UUID> * failed_to_remove = nullptr);

    template <typename AttributesT>
    bool tryRemove(const String & name) { return tryRemove(name, AttributesT::TYPE); }
    bool tryRemove(const String & name, const Type & type);

    template <typename AttributesT>
    void tryRemove(const Strings & names, Strings * failed_to_remove = nullptr) { tryRemove(names, AttributesT::TYPE, failed_to_remove); }
    void tryRemove(const Strings & names, const Type & type, Strings * failed_to_remove = nullptr);

    /// Updates the attributes in the storage.
    /// `update_func` should have signature `void(AttributesT & attrs)` where `AttributesT` is a class derived from IAttributes.
    template <typename UpdateFuncT>
    void update(const UUID & id, const UpdateFuncT & update_func) { updateImpl(id, castUpdateFunc(update_func)); }

    template <typename UpdateFuncT>
    void update(const std::vector<UUID> & ids, const UpdateFuncT & update_func) { updateHelper(ids, castUpdateFunc(update_func)); }

    template <typename AttributesT, typename UpdateFuncT>
    void update(const String & name, const UpdateFuncT & update_func) { update(name, AttributesT::TYPE, update_func); }

    template <typename UpdateFuncT>
    void update(const String & name, const Type & type, const UpdateFuncT & update_func) { updateImpl(getID(name, type), castUpdateFunc(update_func)); }

    template <typename AttributesT, typename UpdateFuncT>
    void update(const Strings & names, const UpdateFuncT & update_func) { update(names, AttributesT::TYPE, update_func); }

    template <typename UpdateFuncT>
    void update(const Strings & names, const Type & type, const UpdateFuncT & update_func) { updateHelper(names, type, castUpdateFunc(update_func)); }

    template <typename UpdateFuncT>
    bool tryUpdate(const UUID & id, const UpdateFuncT & update_func) { return tryUpdateHelper(id, castUpdateFunc(update_func)); }

    template <typename UpdateFuncT>
    void tryUpdate(const std::vector<UUID> & ids, const UpdateFuncT & update_func, std::vector<UUID> * failed_to_update = nullptr) { tryUpdateHelper(ids, castUpdateFunc(update_func), failed_to_update); }

    template <typename AttributesT, typename UpdateFuncT>
    bool tryUpdate(const String & name, const UpdateFuncT & update_func) { return tryUpdate(name, AttributesT::TYPE, update_func); }

    template <typename UpdateFuncT>
    bool tryUpdate(const String & name, const Type & type, const UpdateFuncT & update_func) { return tryUpdateHelper(name, type, castUpdateFunc(update_func)); }

    template <typename AttributesT, typename UpdateFuncT>
    void tryUpdate(const Strings & names, const UpdateFuncT & update_func, Strings * failed_to_update = nullptr) { tryUpdate(names, AttributesT::TYPE, update_func, failed_to_update); }

    template <typename UpdateFuncT>
    void tryUpdate(const Strings & names, const Type & type, const UpdateFuncT & update_func, Strings * failed_to_update = nullptr) { tryUpdateHelper(names, type, castUpdateFunc(update_func), failed_to_update); }

    class Subscription
    {
    public:
        virtual ~Subscription() {}
    };

    using SubscriptionPtr = std::unique_ptr<Subscription>;

    /// Subscribes for new attributes.
    /// Can return nullptr if cannot subscribe or if it doesn't make sense (the storage is read-only).
    using OnNewHandler = std::function<void(const UUID &)>;
    template <typename AttributesT>
    SubscriptionPtr subscribeForNew(const OnNewHandler & on_new) const { return subscribeForNew(String{}, AttributesT::TYPE, on_new); }
    SubscriptionPtr subscribeForNew(const Type & type, const OnNewHandler & on_new) const { return subscribeForNew(String{}, type, on_new); }

    template <typename AttributesT>
    SubscriptionPtr subscribeForNew(const String & prefix, const OnNewHandler & on_new) const { return subscribeForNew(prefix, AttributesT::TYPE, on_new); }
    SubscriptionPtr subscribeForNew(const String & prefix, const Type & type, const OnNewHandler & on_new) const { return subscribeForNewImpl(prefix, type, on_new); }

    /// Subscribes for changes.
    /// Can return nullptr if cannot subscribe (identifier not found) or if it doesn't make sense (the storage is read-only).
    /// `on_changed` should have signature `void(const std::shared_ptr<const AttributesT> & attrs)` where `AttributesT` is any class derived from IAttributes.
    template <typename OnChangedHandlerT>
    SubscriptionPtr subscribeForChanges(const UUID & id, const OnChangedHandlerT & on_changed) const { return subscribeForChangesImpl(id, castOnChangedHandler(on_changed)); }

    template <typename AttributesT, typename OnChangedHandlerT>
    SubscriptionPtr subscribeForChanges(const String & name, const OnChangedHandlerT & on_changed) const { return subscribeForChanges(name, AttributesT::TYPE, on_changed); }

    template <typename OnChangedHandlerT>
    SubscriptionPtr subscribeForChanges(const String & name, const Type & type, const OnChangedHandlerT & on_changed) const;

protected:
    using UpdateFunc = std::function<void(IAttributes &)>;
    using OnChangedHandler = std::function<void(const AttributesPtr &)>;

    virtual std::vector<UUID> findPrefixedImpl(const String & prefix, const Type & type) const = 0;
    virtual std::optional<UUID> findImpl(const String & name, const Type & type) const = 0;
    virtual bool existsImpl(const UUID & id) const = 0;
    virtual AttributesPtr readImpl(const UUID & id) const = 0;
    virtual String readNameImpl(const UUID & id) const = 0;
    virtual UUID insertImpl(const IAttributes & attrs, bool replace_if_exists) = 0;
    virtual void removeImpl(const UUID & id) = 0;
    virtual void updateImpl(const UUID & id, const UpdateFunc & update_func) = 0;
    virtual SubscriptionPtr subscribeForNewImpl(const String & prefix, const Type & type, const OnNewHandler & on_new) const = 0;
    virtual SubscriptionPtr subscribeForChangesImpl(const UUID & id, const OnChangedHandler & on_changed) const = 0;

    static UUID generateRandomID();
    [[noreturn]] static void throwNotFound(const UUID & id);
    [[noreturn]] static void throwNotFound(const String & name, const Type & type);
    [[noreturn]] static void throwNameCollisionCannotInsert(const String & name, const Type & type, const Type & type_of_existing);
    [[noreturn]] static void throwNameCollisionCannotRename(const String & old_name, const String & new_name, const Type & type, const Type & type_of_existing);

    using OnChangeNotification = std::pair<OnChangedHandler, AttributesPtr>;
    using OnChangeNotifications = std::vector<OnChangeNotification>;
    using OnNewNotification = std::pair<OnNewHandler, UUID>;
    using OnNewNotifications = std::vector<OnNewNotification>;
    static void notify(const OnChangeNotifications & notifications);
    static void notify(const OnNewNotifications & notifications);

private:
    AttributesPtr tryReadHelper(const UUID & id) const;
    void updateHelper(const std::vector<UUID> & ids, const UpdateFunc & update_func);
    void updateHelper(const Strings & names, const Type & type, const UpdateFunc & update_func);
    bool tryUpdateHelper(const UUID & id, const UpdateFunc & update_func);
    void tryUpdateHelper(const std::vector<UUID> & ids, const UpdateFunc & update_func, std::vector<UUID> * failed_to_update = nullptr);
    bool tryUpdateHelper(const String & name, const Type & type, const UpdateFunc & update_func);
    void tryUpdateHelper(const Strings & names, const Type & type, const UpdateFunc & update_func, Strings * failed_to_update = nullptr);

    template <typename UpdateFuncT>
    static UpdateFunc castUpdateFunc(const UpdateFuncT & update_func);

    template <typename OnChangedHandlerT>
    static OnChangedHandler castOnChangedHandler(const OnChangedHandlerT & on_changed);
};


template <typename AttributesT>
std::shared_ptr<const AttributesT> IAttributesStorage::read(const UUID & id) const
{
    auto attrs = readImpl(id);
    if (!attrs)
        return nullptr;
    if constexpr (std::is_same_v<AttributesT, IAttributes>)
        return attrs;
    else
        return attrs->cast<AttributesT>();
}


template <typename AttributesT>
std::shared_ptr<const AttributesT> IAttributesStorage::read(const String & name) const
{
    return read<AttributesT>(getID(name, AttributesT::TYPE));
}


template <typename AttributesT>
std::shared_ptr<const AttributesT> IAttributesStorage::tryRead(const UUID & id) const
{
    auto attrs = tryReadHelper(id);
    if (!attrs)
        return nullptr;
    if constexpr (std::is_same_v<AttributesT, IAttributes>)
        return attrs;
    else
        return attrs->tryCast<AttributesT>();
}


template <typename AttributesT>
std::shared_ptr<const AttributesT> IAttributesStorage::tryRead(const String & name) const
{
    auto id = find(name, AttributesT::TYPE);
    return id ? tryRead<AttributesT>(*id) : nullptr;
}


template <typename UpdateFuncT>
IAttributesStorage::UpdateFunc IAttributesStorage::castUpdateFunc(const UpdateFuncT & update_func)
{
    using ParamTypes = typename boost::function_types::parameter_types<decltype(&UpdateFuncT::operator())>::type;
    using ArgType = typename boost::mpl::at<ParamTypes, boost::mpl::int_<1> >::type;
    using AttributesT = std::remove_reference_t<ArgType>;

    return [update_func](IAttributes & attrs)
    {
        if constexpr (std::is_same_v<AttributesT, IAttributes>)
            update_func(attrs);
        else
            update_func(*attrs.cast<AttributesT>());
    };
}


template <typename OnChangedHandlerT>
IAttributesStorage::OnChangedHandler IAttributesStorage::castOnChangedHandler(const OnChangedHandlerT & on_changed)
{
    using ParamTypes = typename boost::function_types::parameter_types<decltype(&OnChangedHandlerT::operator())>::type;
    using ArgType = typename boost::mpl::at<ParamTypes, boost::mpl::int_<1> >::type;
    using AttributesT = typename std::remove_const_t<typename std::remove_const_t<std::remove_reference_t<ArgType>>::element_type>;

    return [on_changed](const AttributesPtr & attrs)
    {
        if constexpr (std::is_same_v<AttributesT, IAttributes>)
            on_changed(attrs);
        else
            on_changed(attrs ? attrs->cast<AttributesT>() : nullptr);
    };
}


template <typename OnChangedHandlerT>
IAttributesStorage::SubscriptionPtr IAttributesStorage::subscribeForChanges(const String & name, const Type & type, const OnChangedHandlerT & on_changed) const
{
    auto id = find(name, type);
    return id ? subscribeForChanges(*id, on_changed) : nullptr;
}
}
