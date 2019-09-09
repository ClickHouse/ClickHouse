#include <ACL/IACLAttributesStorage.h>
#include <ACL/ACLAttributesType.h>
#include <Common/Exception.h>
#include <IO/WriteHelpers.h>


namespace DB
{
String backQuoteIfNeed(const String & x);


ACLAttributesPtr IACLAttributesStorage::read(const UUID & id) const
{
    return readImpl(id);
}


ACLAttributesPtr IACLAttributesStorage::read(const UUID & id, ACLAttributesType desired_type) const
{
    ACLAttributesPtr attrs = readImpl(id);
    if (!attrs)
        return nullptr;
    auto type = attrs->getType();
    while (type != desired_type)
    {
        const auto & base = ACLAttributesTypeInfo::get(type).base_type;
        if (!base)
            return nullptr;
        type = *base;
    }
    return attrs;
}


ACLAttributesPtr IACLAttributesStorage::readStrict(const UUID & id, ACLAttributesType desired_type) const
{
    ACLAttributesPtr attrs = read(id, desired_type);
    if (!attrs)
        throwNotFound(id, desired_type);
    return attrs;
}


void IACLAttributesStorage::throwNotFound(const UUID & id, ACLAttributesType type)
{
    const auto & info = ACLAttributesTypeInfo::get(type);
    throw Exception(info.title + " {" + toString(id) + "} not found", info.not_found_error_code);
}


void IACLAttributesStorage::throwNotFound(const String & name, ACLAttributesType type)
{
    const auto & info = ACLAttributesTypeInfo::get(type);
    throw Exception(info.title + " " + backQuoteIfNeed(name) + " not found", info.not_found_error_code);
}


void IACLAttributesStorage::throwCannotInsertAlreadyExists(const String & name, ACLAttributesType type, ACLAttributesType type_of_existing)
{
    const auto & info = ACLAttributesTypeInfo::get(type);
    const auto & info_of_existing = ACLAttributesTypeInfo::get(type_of_existing);
    throw Exception(
        info.title + " " + backQuoteIfNeed(name) + ": cannot create because " + info_of_existing.title + " " + backQuoteIfNeed(name)
            + " already exists",
        info.already_exists_error_code);
}


void IACLAttributesStorage::throwCannotRenameNewNameInUse(const String & name, const String & new_name, ACLAttributesType type, ACLAttributesType type_of_existing)
{
    const auto & info = ACLAttributesTypeInfo::get(type);
    const auto & info_of_existing = ACLAttributesTypeInfo::get(type_of_existing);
    throw Exception(
        info.title + " " + backQuoteIfNeed(name) + ": cannot rename to " + backQuoteIfNeed(new_name) + " because " + info_of_existing.title
            + " " + backQuoteIfNeed(new_name) + " already exists",
        info_of_existing.already_exists_error_code);
}
}
