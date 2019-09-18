#include <ACL/IControlAttributesStorage.h>
#include <Common/Exception.h>
#include <IO/WriteHelpers.h>
#include <Poco/UUIDGenerator.h>


namespace DB
{
String backQuoteIfNeed(const String & x);


ControlAttributesPtr IControlAttributesStorage::read(const UUID & id, const Type & type) const
{
    auto attrs = tryReadImpl(id);
    if (!attrs)
        throwNotFound(id, type);
    attrs->checkIsDerived(type);
    return attrs;
}


ControlAttributesPtr IControlAttributesStorage::tryRead(const UUID & id) const
{
    return tryReadImpl(id);
}


ControlAttributesPtr IControlAttributesStorage::tryRead(const UUID & id, const Type & type) const
{
    auto attrs = tryReadImpl(id);
    if (!attrs || !attrs->isDerived(type))
        return nullptr;
    return attrs;
}


void IControlAttributesStorage::update(const UUID & id, const Type & type, const std::function<void(Attributes &)> & update_func)
{
    updateImpl(id, type, update_func);
}


UUID IControlAttributesStorage::generateRandomID()
{
    static Poco::UUIDGenerator generator;
    UUID id;
    generator.createRandom().copyTo(reinterpret_cast<char *>(&id));
    return id;
}

void IControlAttributesStorage::throwNotFound(const UUID & id, const Type & type)
{
    throw Exception(String(type.name) + " {" + toString(id) + "} not found", type.error_code_not_found);
}


void IControlAttributesStorage::throwCannotRenameNewNameIsUsed(const String & name, const Type & type, const String & existing_name, const Type & existing_type)
{
    throw Exception(
        String(type.name) + " " + backQuoteIfNeed(name) + ": cannot rename to " + backQuoteIfNeed(existing_name) + " because " + existing_type.name
            + " " + backQuoteIfNeed(existing_name) + " already exists",
        existing_type.error_code_already_exists);
}
}
