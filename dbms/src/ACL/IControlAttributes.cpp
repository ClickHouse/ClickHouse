#include <ACL/IControlAttributes.h>
#include <Common/Exception.h>
#include <IO/WriteHelpers.h>


namespace DB
{
String backQuoteIfNeed(const String & x);


bool IControlAttributes::equal(const IControlAttributes & other) const
{
    return (name == other.name) && (getType() == other.getType());
}


bool IControlAttributes::Type::isDerived(const Type & base_type_) const
{
    const Type * type = this;
    while (*type != base_type_)
    {
        if (!type->base_type)
            return false;
        type = type->base_type;
    }
    return true;
}


bool IControlAttributes::isDerived(const Type & base_type) const
{
    return getType().isDerived(base_type);
}


void IControlAttributes::checkIsDerived(const Type & base_type) const
{
    if (!isDerived(base_type))
    {
        const Type & type = getType();
        throw Exception(
            String(type.name) + " " + backQuoteIfNeed(name) + ": expected to be of type " + base_type.name,
            base_type.error_code_not_found);
    }
}
}
