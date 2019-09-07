#pragma once

#include <ACL/IACLAttributes.h>
#include <Core/Types.h>
#include <Core/UUID.h>
#include <functional>
#include <vector>
#include <boost/function_types/parameter_types.hpp>
#include <boost/mpl/at.hpp>


namespace DB
{
/// Contains the attributes of access control's elements: users, roles, quotas.
/// The implementations of this class must be thread-safe.
class IACLAttributesStorage
{
public:
    enum class Status
    {
        OK,
        NOT_FOUND,
        ALREADY_EXISTS,
    };

    IACLAttributesStorage() {}
    virtual ~IACLAttributesStorage() {}

    /// Returns the name of this storage.
    virtual const String & getStorageName() const = 0;

    /// Returns the identifiers of all the attributes of a specified type contained in the storage.
    virtual std::vector<UUID> findAll(IACLAttributes::Type type) const = 0;

    /// Searchs for a attributes with specified name and type.
    /// Returns zero if not found.
    virtual UUID find(const String & name, IACLAttributes::Type type) const = 0;

    /// Returns whether there are attributes with such ID in the storage.
    virtual bool exists(const UUID & id) const = 0;

    /// Inserts attributes to the storage.
    /// Returns either OK or ALREADY_EXISTS.
    virtual Status insert(const IACLAttributes & attrs, UUID & id) = 0;

    /// Removes attributes.
    /// Returns either OK or NOT_FOUND.
    virtual Status remove(const UUID & id) = 0;

    /// Reads the attributes.
    /// Returns either OK or NOT_FOUND.
    template <typename AttributesType>
    Status read(const UUID & id, std::shared_ptr<const AttributesType> & attrs) const;

    class Operation;

    /// Changes the attributes.
    /// Returns either OK or NOT_FOUND or ALREADY_EXISTS.
    Status write(const UUID & id, const Operation & operation);

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
    using MakeChangeFunction = std::function<Status(IACLAttributes & attrs)>;
    virtual Status writeImpl(const UUID & id, const MakeChangeFunction & make_change) = 0;

    virtual Status readImpl(const UUID & id, ACLAttributesPtr & attrs) const = 0;

    using OnChangedFunction = std::function<void(const ACLAttributesPtr &)>;
    virtual SubscriptionPtr subscribeForChangesImpl(const UUID & id, const OnChangedFunction & on_changed) const = 0;
};


class IACLAttributesStorage::Operation
{
public:
    Operation() {}
    Operation(const Operation & src) { then(src); }
    Operation & operator =(const Operation & src) { workers.clear(); return then(src); }
    Operation(Operation && src) { then(src); }
    Operation & operator =(Operation && src) { workers.clear(); return then(src); }

    template <typename Func>
    Operation(const Func & func) { then(func); }

    Operation & then(const Operation & other)
    {
        workers.insert(workers.end(), other.workers.begin(), other.workers.end());
        return *this;
    }

    Operation & then(Operation && other)
    {
        if (workers.empty())
        {
            std::swap(workers, other.workers);
            return *this;
        }
        workers.insert(workers.end(), std::make_move_iterator(other.workers.begin()), std::make_move_iterator(other.workers.end()));
        other.workers.clear();
        return *this;
    }

    template <typename Func>
    Operation & then(const Func & func)
    {
        using ParamTypes = typename boost::function_types::parameter_types<decltype(&Func::operator())>::type;
        using ArgType = typename boost::mpl::at<ParamTypes, boost::mpl::int_<1> >::type;
        using AttributesType = typename std::remove_reference_t<ArgType>;

        workers.emplace_back([func](IACLAttributes & attrs)
        {
            auto a = dynamic_cast<AttributesType *>(&attrs);
            if (!a)
                return Status::NOT_FOUND;
            func(*a);
            return Status::OK;
        });
        return *this;
    }

    Status run(IACLAttributes & attrs) const
    {
        for (const auto & fn : workers)
        {
            Status status = fn(attrs);
            if (status != Status::OK)
                return status;
        }
        return Status::OK;
    }

private:
    std::vector<std::function<Status(IACLAttributes &)>> workers;
};


template <typename AttributesType>
IACLAttributesStorage::Status IACLAttributesStorage::read(const UUID & id, std::shared_ptr<const AttributesType> & attrs) const
{
    attrs = nullptr;

    ACLAttributesPtr a;
    Status status = readImpl(id, a);
    if (status != Status::OK)
        return status;

    attrs = std::dynamic_pointer_cast<const AttributesType>(a);
    if (!attrs)
        return Status::NOT_FOUND;
    return Status::OK;
}


IACLAttributesStorage::Status IACLAttributesStorage::write(const UUID & id, const Operation & operation)
{
    return writeImpl(id, [&operation](IACLAttributes & attrs) { return operation.run(attrs); });
}


template <typename Func>
IACLAttributesStorage::SubscriptionPtr IACLAttributesStorage::subscribeForChanges(const UUID & id, const Func & on_changed)
{
    using ParamTypes = typename boost::function_types::parameter_types<decltype(&Func::operator())>::type;
    using ArgType = typename boost::mpl::at<ParamTypes, boost::mpl::int_<1> >::type;
    using AttributesType = typename std::remove_const_t<std::remove_reference_t<ArgType>>::element_type;

    return subscribeForChangesImpl(id, [on_changed](const ACLAttributesPtr & attrs)
    {
        auto a = std::dynamic_pointer_cast<AttributesType>(attrs);
        on_changed(a);
    });
}
}
