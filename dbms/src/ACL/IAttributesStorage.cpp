#include <ACL/IAttributesStorage.h>
#include <Common/Exception.h>
#include <IO/WriteHelpers.h>
#include <Poco/UUIDGenerator.h>


namespace DB
{
String backQuoteIfNeed(const String & x);


UUID IAttributesStorage::getID(const String & name, const Type & type) const
{
    auto id = find(name, type);
    if (!id)
        throwNotFound(name, type);
    return *id;
}


UUID IAttributesStorage::insert(const Attributes & attrs)
{
    AttributesPtr caused_name_collision;
    auto [id, inserted] = tryInsertImpl(attrs, caused_name_collision);
    if (inserted)
        return id;
    throw Exception(
        String(attrs.getType().name) + " " + backQuoteIfNeed(attrs.name) + ": couldn't create because "
            + caused_name_collision->getType().name + " " + backQuoteIfNeed(caused_name_collision->name) + " already exists",
        caused_name_collision->getType().error_code_already_exists);
}


std::pair<UUID, bool> IAttributesStorage::tryInsert(const Attributes & attrs)
{
    AttributesPtr caused_name_collision;
    return tryInsertImpl(attrs, caused_name_collision);
}


std::pair<UUID, bool> IAttributesStorage::tryInsert(const Attributes & attrs, AttributesPtr & caused_name_collision)
{
    return tryInsertImpl(attrs, caused_name_collision);
}


void IAttributesStorage::remove(const UUID & id, const Type & type)
{
    if (!tryRemoveImpl(id))
        throwNotFound(id, type);
}


bool IAttributesStorage::tryRemove(const UUID & id)
{
    return tryRemoveImpl(id);
}


ControlAttributesPtr IAttributesStorage::read(const UUID & id, const Type & type) const
{
    auto attrs = tryReadImpl(id);
    if (!attrs)
        throwNotFound(id, type);
    attrs->checkIsDerived(type);
    return attrs;
}


ControlAttributesPtr IAttributesStorage::tryRead(const UUID & id) const
{
    return tryReadImpl(id);
}


ControlAttributesPtr IAttributesStorage::tryRead(const UUID & id, const Type & type) const
{
    auto attrs = tryReadImpl(id);
    if (!attrs || !attrs->isDerived(type))
        return nullptr;
    return attrs;
}


void IAttributesStorage::update(const UUID & id, const Type & type, const std::function<void(Attributes &)> & update_func)
{
    updateImpl(id, type, update_func);
}


IAttributesStorage::SubscriptionPtr IAttributesStorage::subscribeForChanges(const UUID & id, const OnChangedHandler & on_changed) const
{
    return subscribeForChangesImpl(id, on_changed);
}


UUID IAttributesStorage::generateRandomID()
{
    static Poco::UUIDGenerator generator;
    UUID id;
    generator.createRandom().copyTo(reinterpret_cast<char *>(&id));
    return id;
}

void IAttributesStorage::throwNotFound(const UUID & id, const Type & type)
{
    throw Exception(String(type.name) + " {" + toString(id) + "} not found", type.error_code_not_found);
}


void IAttributesStorage::throwNotFound(const String & name, const Type & type)
{
    throw Exception(String(type.name) + " " + backQuoteIfNeed(name) + " not found", type.error_code_not_found);
}


void IAttributesStorage::throwCannotRenameNewNameIsUsed(const String & name, const Type & type, const String & existing_name, const Type & existing_type)
{
    throw Exception(
        String(type.name) + " " + backQuoteIfNeed(name) + ": cannot rename to " + backQuoteIfNeed(existing_name) + " because " + existing_type.name
            + " " + backQuoteIfNeed(existing_name) + " already exists",
        existing_type.error_code_already_exists);
}
}
