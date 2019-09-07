#include <ACL/IACLAttributable.h>
#include <ACL/IACLAttributableManager.h>
#include <Common/Exception.h>
#include <IO/WriteHelpers.h>


namespace DB
{
using Operation = IACLAttributable::Operation;
using Status = IACLAttributesStorage::Status;


IACLAttributable::IACLAttributable()
{
    atomic_data.store({UInt128{0}, nullptr, std::nullopt});
}


IACLAttributable::IACLAttributable(const UUID & id_, IACLAttributableManager * manager_)
{
    atomic_data.store({id_, manager_, std::nullopt});
}


IACLAttributable::IACLAttributable(const UUID & id_, IACLAttributableManager * manager_, IACLAttributesStorage * storage_)
{
    atomic_data.store({id_, manager_, storage_});
}


IACLAttributable::IACLAttributable(const IACLAttributable & src)
{
    *this = src;
}


IACLAttributable & IACLAttributable::operator =(const IACLAttributable & src)
{
    atomic_data.store(src.atomic_data.load());
    return *this;
}


IACLAttributable::~IACLAttributable() = default;


UUID IACLAttributable::getID() const
{
    return UUID(atomic_data.load().id);
}


IACLAttributableManager * IACLAttributable::getManager() const
{
    return atomic_data.load().manager;
}


bool IACLAttributable::isValid() const
{
    return getAttributes() != nullptr;
}


Operation IACLAttributable::setNameOp(const String & name) const
{
    //manager->checkNewName(id, name, getType());
    return [name](IACLAttributes & attrs)
    {
        attrs.name = name;
    };
}


String IACLAttributable::getName() const
{
    return getAttributesStrict()->name;
}


String IACLAttributable::getNameOrID() const
{
    return getNameOrID(loadDataWithStorage());
}


String IACLAttributable::getNameOrID(const Data & data)
{
    if (*data.storage)
    {
        AttributesPtr attrs;
        if (((*data.storage)->read(UUID(data.id), attrs) == Status::OK) && attrs)
            return attrs->name;
    }
    if (data.manager)
    {
        String name = data.manager->findNameInCache(UUID(data.id));
        if (!name.empty())
            return name;
    }
    return toString(UUID(data.id));
}


void IACLAttributable::perform(const Operation & operation)
{
    Data data = loadDataWithStorage();
    if (!data.storage)
        throw Exception("There is no such " + getTypeName() + " " + getNameOrID(data), getNotFoundErrorCode());

    auto status = (*data.storage)->write(UUID(data.id), operation);
    if (status != Status::OK)
    {
        if (status == Status::NOT_FOUND)
            throw Exception("There is no such " + getTypeName() + " " + getNameOrID(data), getNotFoundErrorCode());
        if (status == Status::ALREADY_EXISTS)
            throw Exception("The new name is already in use, cannot rename " + getTypeName() + " " + getNameOrID(data), getAlreadyExistsErrorCode());
    }
}


bool IACLAttributable::drop()
{
    auto data = loadDataWithStorage();
    if (!data.storage)
        return false;
    bool dropped = ((*data.storage)->remove(UUID(data.id)) == Status::OK);
    auto new_data = data;
    new_data.storage = nullptr;
    atomic_data.compare_exchange_strong(data, new_data);
    return dropped;
}


IACLAttributable::Data IACLAttributable::loadDataWithStorage() const
{
    Data data = atomic_data.load();
    while (!data.storage)
    {
        Data new_data = data;
        new_data = data;
        if (new_data.manager && (new_data.id != UInt128(0)))
            new_data.storage = new_data.manager->findStorage(UUID(data.id));
        else
            new_data.storage = nullptr;
        if (atomic_data.compare_exchange_strong(data, new_data))
            return new_data;
    }
    return data;
}


void IACLAttributable::throwException(const String & message, int error_code)
{
    throw Exception(message, error_code);
}

}
