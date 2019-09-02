#include <Account/IAccessControlElement.h>
#include <Account/AccessControlManager.h>
#include <Account/IAccessControlStorage.h>
#include <Common/Exception.h>
#include <IO/WriteHelpers.h>


namespace DB
{
using Operation = IAccessControlElement::Operation;


Operation & Operation::then(const std::function<void(Attributes &)> & fn)
{
    ops.emplace_back(fn);
    return *this;
}


Operation & Operation::then(Operation && src)
{
    ops.insert(ops.end(), std::make_move_iterator(src.ops.begin()), std::make_move_iterator(src.ops.end()));
    src.ops.clear();
    return *this;
}


IAccessControlElement::IAccessControlElement() = default;


IAccessControlElement::IAccessControlElement(const UUID & id_, AccessControlManager * manager_)
    : id(id_), manager(manager_)
{}


IAccessControlElement::IAccessControlElement(const UUID & id_, AccessControlManager * manager_, const StoragePtr & storage_)
    : id(id_), manager(manager_), storage_cached(storage_)
{
}


IAccessControlElement::IAccessControlElement(const IAccessControlElement & src) = default;
IAccessControlElement & IAccessControlElement::operator =(const IAccessControlElement & src) = default;
IAccessControlElement::~IAccessControlElement() = default;


bool IAccessControlElement::isValid() const
{
    return tryGetAttributes() != nullptr;
}


IAccessControlElement::AttributesPtr IAccessControlElement::getAttributesInternal() const
{
    auto * storage = getStorage();
    if (!storage)
        return nullptr;
    AttributesPtr attrs;
    auto status = storage->read(id, attrs);
    if (status != Storage::Status::OK)
        return nullptr;
    return attrs;
}


Operation IAccessControlElement::setNameOp(const String & name) const
{
    //manager->checkNewName(id, name, getType());
    return prepareOperation([name](Attributes & attrs)
    {
        attrs.name = name;
    });
}


String IAccessControlElement::getName() const
{
    return getAttributes()->name;
}


String IAccessControlElement::getNameOrID() const
{
    auto attrs = tryGetAttributes();
    if (attrs)
        return attrs->name;
    if (manager)
    {
        String name = manager->findNameInCache(id);
        if (!name.empty())
            return name;
    }
    return toString(id);
}


IAccessControlElement::Storage * IAccessControlElement::getStorage() const
{
    if (!storage_cached)
    {
        if (manager && (id != UUID(UInt128(0))))
            storage_cached = manager->findStorage(id);
        else
            storage_cached = nullptr;
    }
    return storage_cached->get();
}


void IAccessControlElement::perform(const Operation & operation)
{
    auto * storage = getStorage();
    if (!storage)
        throw Exception("There is no such " + getTypeName() + " " + getNameOrID(), getNotFoundErrorCode());

    auto status = storage->write(id, [&](Attributes & attrs)
    {
        for (const auto & op : operation.ops)
            op(attrs);
    });

    if (status != Storage::Status::OK)
    {
        if (status == Storage::Status::NOT_FOUND)
            throw Exception("There is no such " + getTypeName() + " " + getNameOrID(), getNotFoundErrorCode());
        if (status == Storage::Status::ALREADY_EXISTS)
            throw Exception("The new name is already in use, cannot rename " + getTypeName() + " " + getNameOrID(), getAlreadyExistsErrorCode());
    }
}


bool IAccessControlElement::drop()
{
    auto * storage = getStorage();
    if (!storage)
        return false;
    bool removed = (storage->remove(id) == Storage::Status::OK);
    storage_cached = nullptr;
    return removed;
}


void IAccessControlElement::throwException(const String & message, int error_code)
{
    throw Exception(message, error_code);
}

}
