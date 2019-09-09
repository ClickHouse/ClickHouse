#pragma once

#include <ACL/IACLAttributes.h>
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
class IACLAttributesStorage
{
public:
    IACLAttributesStorage() {}
    virtual ~IACLAttributesStorage() {}

    /// Returns the name of this storage.
    virtual const String & getStorageName() const = 0;

    /// Returns the identifiers of all the attributes of a specified type contained in the storage.
    virtual std::vector<UUID> findAll(ACLAttributesType type) const = 0;

    /// Searchs for a attributes with specified name and type.
    virtual std::optional<UUID> find(const String & name, ACLAttributesType type) const = 0;

    /// Returns whether there are attributes with such ID in the storage.
    virtual bool exists(const UUID & id) const = 0;

    /// Inserts attributes to the storage.
    /// Throws an exception if such name is already in use and `if_not_exists == false`.
    /// Returns {id, true} if inserted or {id, false} if already exists.
    virtual std::pair<UUID, bool> insert(const IACLAttributes & attrs, bool if_not_exists) = 0;

    /// Reads the attributes. Returns nullptr if not found.
    ACLAttributesPtr read(const UUID & id) const;
    ACLAttributesPtr read(const UUID & id, ACLAttributesType desired_type) const;

    /// Reads the attributes. Assigns `attrs` to nullptr if not found.
    template <typename AttributesT>
    void read(const UUID & id, std::shared_ptr<const AttributesT> & attrs) const;

    /// Reads the attributes. Throws an exception if not found.
    ACLAttributesPtr readStrict(const UUID & id, ACLAttributesType desired_type) const;

    template <typename AttributesT>
    void readStrict(const UUID & id, std::shared_ptr<const AttributesT> & attrs) const;

    struct Change
    {
        enum class Type
        {
            UPDATE,
            REMOVE,
            REMOVE_REFERENCES,
        };
        Type type;
        UUID id;
        std::function<void(IACLAttributes &)> update_func; /// used if type == Type::UPDATE
        bool if_exists; /// used if type == Type::REMOVE
        ACLAttributesType attributes_type; /// can be used only to format the message to throw an exception
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

    /// Subscribes for changes. Returns nullptr if there is no such id in the storage or the storage is read-only.
    /// The function can returns nullptr if failed to subscribe.
    template <typename Func>
    SubscriptionPtr subscribeForChanges(const UUID & id, const Func & on_changed);

protected:
    virtual ACLAttributesPtr readImpl(const UUID & id) const = 0;

    using OnChangedFunction = std::function<void(const ACLAttributesPtr &)>;
    virtual SubscriptionPtr subscribeForChangesImpl(const UUID & id, const OnChangedFunction & on_changed) const = 0;

    [[noreturn]] static void throwNotFound(const UUID & id, ACLAttributesType type);
    [[noreturn]] static void throwNotFound(const String & name, ACLAttributesType type);
    [[noreturn]] static void throwCannotInsertAlreadyExists(const String & name, ACLAttributesType type, ACLAttributesType type_of_existing);
    [[noreturn]] static void throwCannotRenameNewNameInUse(const String & name, const String & new_name, ACLAttributesType type, ACLAttributesType type_of_existing);
};


template <typename AttributesT>
void IACLAttributesStorage::read(const UUID & id, std::shared_ptr<const AttributesT> & attrs) const
{
    attrs = std::dynamic_pointer_cast<const AttributesT>(readImpl(id));
}


template <typename AttributesT>
void IACLAttributesStorage::readStrict(const UUID & id, std::shared_ptr<const AttributesT> & attrs) const
{
    read(id, attrs);
    if (!attrs)
        throwNotFound(id, AttributesT{}.getType());
}


template <typename Func>
IACLAttributesStorage::SubscriptionPtr IACLAttributesStorage::subscribeForChanges(const UUID & id, const Func & on_changed)
{
    using ParamTypes = typename boost::function_types::parameter_types<decltype(&Func::operator())>::type;
    using ArgType = typename boost::mpl::at<ParamTypes, boost::mpl::int_<1> >::type;
    using AttributesT = typename std::remove_const_t<std::remove_reference_t<ArgType>>::element_type;

    return subscribeForChangesImpl(id, [on_changed](const ACLAttributesPtr & attrs)
    {
        auto a = std::dynamic_pointer_cast<AttributesT>(attrs);
        on_changed(a);
    });
}
}
