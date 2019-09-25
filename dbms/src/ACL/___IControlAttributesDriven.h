#pragma once

#include <ACL/IAttributesStorage.h>
#include <optional>


namespace DB
{
class IAttributesStorageManager;


/// A named object that controls access somehow.
/// Role, user, quota are derived from this class.
/// Threads should not share the same instance of IControlAttributesDriven, it's not thread-safe.
/// But it's OK to have simultaneous instances of IControlAttributesDriven in multiple threads controlling the same attributes.
class IControlAttributesDriven
{
public:
    using Attributes = IAttributes;
    using AttributesPtr = ControlAttributesPtr;
    using Type = Attributes::Type;
    using Storage = IAttributesStorage;
    using Manager = IAttributesStorageManager;

    struct CreateTag {};

    IControlAttributesDriven();
    IControlAttributesDriven(const String & name_, Manager & manager_, Storage * storage_ = nullptr);
    IControlAttributesDriven(const UUID & id_, Manager & manager_, Storage * storage_ = nullptr);
    IControlAttributesDriven(CreateTag{}, const Attributes & attrs, Manager & manager_, Storage * storage_ = nullptr);
    IControlAttributesDriven(const IControlAttributesDriven & src);
    IControlAttributesDriven & operator =(const IControlAttributesDriven & src);
    IControlAttributesDriven(IControlAttributesDriven && src);
    IControlAttributesDriven & operator =(IControlAttributesDriven && src);
    virtual ~IControlAttributesDriven();

    virtual const Type & getType() const = 0;

    friend bool operator ==(const IControlAttributesDriven & lhs, const IControlAttributesDriven & rhs);
    friend bool operator !=(const IControlAttributesDriven & lhs, const IControlAttributesDriven & rhs) { return !(lhs == rhs); }

    UUID getID() const;
    std::optional<UUID> tryGetID() const;
    Manager & getManager() const;
    Manager * tryGetManager() const;
    Storage & getStorage() const;
    Storage * tryGetStorage() const;

    /// Whether this access control element is valid, i.e. controls the access somehow.
    bool isValid() const { return tryGetAttributes() != nullptr; }

    /// Returns the attributes. Never returns nullptr.
    /// Throws an exception if can't get the attributes.
    AttributesPtr getAttributes() const;

    /// Returns the attributes. Returns nullptr if not valid.
    AttributesPtr tryGetAttributes() const;

    class Changes;

    /// Inserts this object to the storage.
    void insert(const AttributesPtr & new_attrs, bool if_not_exists = false);
    Changes insertChanges(const AttributesPtr & new_attrs, bool if_not_exists = false);

    /// Returns the name of this object.
    String getName() const;

    /// Changes the name of this object.
    void setName(const String & name);
    Changes setNameChanges(const String & name);

    /// Drops this object.
    void drop(bool if_exists = false);
    Changes dropChanges(bool if_exists = false);

protected:
    template <typename AttributesT>
    std::shared_ptr<const AttributesT> getAttributesImpl() const
    {
        auto attrs = tryGetAttributesImpl<AttributesT>();
        if (!attrs)
            throwNotFound();
        return attrs;
    }

    template <typename AttributesT>
    std::shared_ptr<const AttributesT> tryGetAttributesImpl() const
    {
        tryGetStorage();
        tryGetID();
        return (storage && id) ? storage->tryRead<AttributesT>(*id) : nullptr;
    }

private:
    [[noreturn]] void throwNotFound();

    mutable std::optional<UUID> id;
    std::optional<String> name;
    Manager * manager = nullptr;
    mutable Storage * storage = nullptr;
    mutable AttributesPtr attrs;
};


/// Changes that can be written to multiple storages.
class IControlAttributesDriven::Changes
{
public:
    struct InsertTag {};
    struct UpdateTag {};
    struct RemoveTag {};
    struct RemoveReferencesTag {};

    Changes() {}
    Changes(const Changes & src) { then(src); }
    Changes & operator =(const Changes & src);
    Changes(Changes && src) { then(src); }
    Changes & operator =(Changes && src);

    Changes(InsertTag, Storage & storage, const ControlAttributesPtr & new_attrs, bool if_not_exists = false);
    Changes(UpdateTag, Storage & storage, const UUID & id, const Type & type, const std::function<void(IAttributes &)> & update_func, bool if_exists = false);

    template <typename UpdateFunc>
    Changes(UpdateTag, Storage & storage, const UUID & id, const UpdateFunc & update_func, bool if_exists = false)
    {
        then(UpdateTag{}, storage, id, update_func, if_exists);
    }

    Changes(RemoveTag, Storage & storage, const UUID & id, const Type & type, bool if_exists = false);
    Changes(RemoveReferencesTag, Storage & storage, const UUID & id);

    Changes & then(const Changes & other);
    Changes & then(Changes && other);

    Changes & then(InsertTag, Storage & storage, const ControlAttributesPtr & new_attrs, bool if_not_exists = false);

    Changes & then(UpdateTag, Storage & storage, const UUID & id, const Type & type, const std::function<void(IAttributes &)> & update_func, bool if_exists = false);

    template <typename UpdateFunc>
    Changes & then(UpdateTag, Storage & storage, const UUID & id, const UpdateFunc & update_func, bool if_exists = false)
    {
        using ParamTypes = typename boost::function_types::parameter_types<decltype(&UpdateFunc::operator())>::type;
        using ArgType = typename boost::mpl::at<ParamTypes, boost::mpl::int_<1> >::type;
        using AttributesT = typename std::remove_reference_t<ArgType>;
        const Type & type = AttributesT::TYPE;

        return then(
            UpdateTag{},
            id,
            type,
            [update_func](IAttributes & attrs)
            {
                update_func(*attrs.cast<AttributesT>());
                return true;
            },
            if_exists);
    }

    Changes & then(RemoveTag, Storage & storage, const UUID & id, const Type & type, bool if_exists = false);
    Changes & then(RemoveReferencesTag, Storage & storage, const UUID & id);

    void apply() const;

private:
    Storage::Change & addChange(Storage & storage);
    Storage::Changes & findStoragePosition(Storage & storage);

    std::vector<std::pair<IAttributesStorage *, Storage::Changes>> all_changes;
};
}
