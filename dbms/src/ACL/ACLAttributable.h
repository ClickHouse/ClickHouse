#pragma once

#include <ACL/IACLAttributesStorage.h>
#include <memory>
#include <atomic>


namespace DB
{
class IACLAttributableManager;


/// A named object that controls access somehow.
/// Role, user, quota are derived from this class.
/// This class is thread-safe.
class ACLAttributable
{
public:
    using Attributes = IACLAttributes;
    using AttributesPtr = std::shared_ptr<const Attributes>;
    class Operation;

    ACLAttributable();
    ACLAttributable(const UUID & id_, IACLAttributableManager & manager_);
    ACLAttributable(const UUID & id_, IACLAttributableManager & manager_, IACLAttributesStorage & storage_);
    ACLAttributable(const ACLAttributable & src);
    ACLAttributable & operator =(const ACLAttributable & src);
    virtual ~ACLAttributable();

    friend bool operator ==(const ACLAttributable & lhs, const ACLAttributable & rhs) { return lhs.getID() == rhs.getID(); }
    friend bool operator !=(const ACLAttributable & lhs, const ACLAttributable & rhs) { return !(lhs == rhs); }

    UUID getID() const;
    IACLAttributableManager * getManager() const;
    IACLAttributesStorage * getStorage() const;

    /// Whether this access control element is valid, i.e. controls the access somehow.
    bool isValid() const;

    /// Returns the attributes. Returns nullptr if not valid.
    AttributesPtr getAttributes() const;

    /// Returns the attributes. Never returns nullptr.
    /// Throws an exception if can't get the attributes.
    AttributesPtr getAttributesStrict() const;

    /// Returns the name of this object.
    String getName() const;

    /// Changes the name of this object.
    void setName(const String & name);
    Operation setNameOp(const String & name);

    /// Returns the name or ID. This function returns a string representation of this object
    /// for logging or exception message. This function tries not to throw exceptions.
    String getNameOrID() const;

    /// Drops this object.
    /// Returns true if successfully dropped, or false if this object didn't exist.
    void drop(bool if_exists = false);
    Operation dropOp(bool if_exists = false);

protected:
    virtual ACLAttributesType getType() const = 0;

    std::pair<AttributesPtr, IACLAttributableManager *> getAttributesWithManagerStrict() const;

    template <typename UpdateFunc>
    Operation prepareOperation(const UpdateFunc & update_func);

private:
    struct Data
    {
        UInt128 id;
        IACLAttributableManager * manager;
        std::optional<IACLAttributesStorage *> storage;
    };

    Data loadData() const;
    Data loadDataStrict() const;

    mutable std::atomic<Data> atomic_data;
};


/// Operations can be performed on one or more instances of ACLAttributable to alter or drop them atomically.
class ACLAttributable::Operation
{
public:
    struct RemoveTag {};
    struct RemoveReferencesTag {};

    Operation() {}
    Operation(const Operation & src) { then(src); }
    Operation & operator =(const Operation & src);
    Operation(Operation && src) { then(src); }
    Operation & operator =(Operation && src);

    template <typename UpdateFunc>
    Operation(IACLAttributesStorage & storage, const UUID & id, const UpdateFunc & update_func, ACLAttributesType attributes_type) { then(storage, id, update_func, attributes_type); }

    Operation(IACLAttributesStorage & storage, const UUID & id, RemoveTag, ACLAttributesType attributes_type, bool if_exists = false);
    Operation(IACLAttributesStorage & storage, const UUID & id, RemoveReferencesTag);

    Operation & then(const Operation & other);
    Operation & then(Operation && other);

    template <typename UpdateFunc>
    Operation & then(IACLAttributesStorage & storage, const UUID & id, const UpdateFunc & update_func, ACLAttributesType attributes_type)
    {
        using ParamTypes = typename boost::function_types::parameter_types<decltype(&UpdateFunc::operator())>::type;
        using ArgType = typename boost::mpl::at<ParamTypes, boost::mpl::int_<1> >::type;
        using AttributesT = typename std::remove_reference_t<ArgType>;

        auto & change = addChange(storage);
        change.type = Change::Type::UPDATE;
        change.id = id;
        change.attributes_type = attributes_type;
        change.update_func = [update_func, attributes_type](IACLAttributes & attrs)
        {
            auto a = dynamic_cast<AttributesT *>(&attrs);
            if (!a)
                throwNotFound(attrs.name, attributes_type);
            update_func(*a);
        };
        return *this;
    }

    Operation & then(IACLAttributesStorage & storage, const UUID & id, RemoveTag, ACLAttributesType attributes_type, bool if_exists = false);
    Operation & then(IACLAttributesStorage & storage, const UUID & id, RemoveReferencesTag);

    void execute() const;

private:
    using Change = IACLAttributesStorage::Change;
    using Changes = IACLAttributesStorage::Changes;

    Change & addChange(IACLAttributesStorage & storage);
    Changes & findPosition(IACLAttributesStorage & storage);
    [[noreturn]] static void throwNotFound(const String & name, ACLAttributesType type);

    std::vector<std::pair<IACLAttributesStorage *, Changes>> all_changes;
};


template <typename UpdateFunc>
ACLAttributable::Operation ACLAttributable::prepareOperation(const UpdateFunc & update_func)
{
    auto data = loadDataStrict();
    return {**data.storage, UUID(data.id), update_func, getType()};
}

}
