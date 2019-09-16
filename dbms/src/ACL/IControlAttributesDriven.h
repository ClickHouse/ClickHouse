#pragma once

#include <ACL/IControlAttributesStorage.h>
#include <atomic>
#include <optional>


namespace DB
{
class IControlAttributesStorageManager;


/// A named object that controls access somehow.
/// Role, user, quota are derived from this class.
/// This class is thread-safe.
class IControlAttributesDriven
{
public:
    using Attributes = IControlAttributes;
    using AttributesPtr = ControlAttributesPtr;
    using Type = Attributes::Type;
    using Storage = IControlAttributesStorage;
    using Manager = IControlAttributesStorageManager;

    IControlAttributesDriven();
    IControlAttributesDriven(const UUID & id_, Manager & manager_);
    IControlAttributesDriven(const UUID & id_, Manager & manager_, Storage & storage_);
    IControlAttributesDriven(const String & name_, Manager & manager_);
    IControlAttributesDriven(const String & name_, Manager & manager_, Storage & storage_);
    IControlAttributesDriven(Manager & manager_, Storage & storage_);
    IControlAttributesDriven(const IControlAttributesDriven & src);
    IControlAttributesDriven & operator =(const IControlAttributesDriven & src);
    virtual ~IControlAttributesDriven();

    virtual const Type & getType() const = 0;

    friend bool operator ==(const IControlAttributesDriven & lhs, const IControlAttributesDriven & rhs) { return lhs.getID() == rhs.getID(); }
    friend bool operator !=(const IControlAttributesDriven & lhs, const IControlAttributesDriven & rhs) { return !(lhs == rhs); }

    UUID getID() const;
    Manager * tryGetManager() const;

    /// Whether this access control element is valid, i.e. controls the access somehow.
    bool isValid() const { return tryGetAttributes() != nullptr; }

    /// Returns the attributes. Never returns nullptr.
    /// Throws an exception if can't get the attributes.
    AttributesPtr getAttributes() const;

    /// Returns the attributes. Returns nullptr if not valid.
    AttributesPtr tryGetAttributes() const;

    class Changes;

    /// Inserts this object to the storage.
    void insert();
    Changes insertChanges();

    /// Returns the name of this object.
    String getName() const;

    /// Changes the name of this object.
    void setName(const String & name);
    Changes setNameChanges(const String & name);

    /// Drops this object.
    void drop(bool if_exists = false);
    Changes dropChanges(bool if_exists = false);

    using IDAndStorage = std::pair<UUID, std::reference_wrapper<Storage>>;
    using IDAndStorageAndManager = std::tuple<UUID, std::reference_wrapper<Storage>, std::reference_wrapper<Manager>>;
    using AttributesAndManager = std::pair<AttributesPtr, std::reference_wrapper<Manager>>;
    IDAndStorage getIDAndStorage() const;
    IDAndStorageAndManager getIDAndStorageAndManager() const;
    AttributesAndManager getAttributesAndManager() const;

protected:
    template <typename AttributesT>
    std::shared_ptr<const AttributesT> getAttributesImpl() const
    {
        return getAttributes()->cast<AttributesT>();
    }

    template <typename AttributesT>
    std::shared_ptr<const AttributesT> tryGetAttributesImpl() const
    {
        auto attrs = tryGetAttributes();
        return attrs ? attrs->tryCast<AttributesT>() : nullptr;
    }

private:
    struct Data
    {
        UInt128 id;
        Manager * manager;
        std::optional<Storage *> storage;
    };

    Data loadData() const;
    Data loadDataTryGetStorage() const;
    Data loadDataGetStorage() const;

    mutable std::atomic<Data> atomic_data;
};


/// Changes that can be written to multiple storages.
class IControlAttributesDriven::Changes
{
public:
    struct InsertTag {};
    struct RemoveTag {};
    struct RemoveReferencesTag {};

    Changes() {}
    Changes(const Changes & src) { then(src); }
    Changes & operator =(const Changes & src);
    Changes(Changes && src) { then(src); }
    Changes & operator =(Changes && src);

    Changes(const IDAndStorage & id_and_storage, InsertTag, const Type & type);
    Changes(const IDAndStorage & id_and_storage, const std::function<void(IControlAttributes &)> & update_func, const Type & type, bool if_exists = false);

    template <typename UpdateFunc>
    Changes(const IDAndStorage & id_and_storage, const UpdateFunc & update_func, bool if_exists = false)
    {
        then(id_and_storage, update_func, if_exists);
    }

    Changes(const IDAndStorage & id_and_storage, RemoveTag, const Type & type, bool if_exists = false);
    Changes(const IDAndStorage & id_and_storage, RemoveReferencesTag);

    Changes & then(const Changes & other);
    Changes & then(Changes && other);

    Changes & then(const IDAndStorage & id_and_storage, InsertTag, const Type & type);

    Changes & then(const IDAndStorage & id_and_storage, const std::function<void(IControlAttributes &)> & update_func, const Type & type, bool if_exists = false);

    template <typename UpdateFunc>
    Changes & then(const IDAndStorage & id_and_storage, const UpdateFunc & update_func, bool if_exists = false)
    {
        using ParamTypes = typename boost::function_types::parameter_types<decltype(&UpdateFunc::operator())>::type;
        using ArgType = typename boost::mpl::at<ParamTypes, boost::mpl::int_<1> >::type;
        using AttributesT = typename std::remove_reference_t<ArgType>;
        const Type & type = AttributesT::TYPE;

        return then(
            id_and_storage,
            [update_func](IControlAttributes & attrs)
            {
                update_func(*attrs.cast<AttributesT>());
                return true;
            },
            type,
            if_exists);
    }

    Changes & then(const IDAndStorage & id_and_storage, RemoveTag, const Type & type, bool if_exists = false);
    Changes & then(const IDAndStorage & id_and_storage, RemoveReferencesTag);

    void apply() const;

private:
    Storage::Change & addChange(Storage & storage);
    Storage::Changes & findStoragePosition(Storage & storage);

    std::vector<std::pair<IControlAttributesStorage *, Storage::Changes>> all_changes;
};
}
