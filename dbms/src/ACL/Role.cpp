#include <ACL/Role.h>
#include <ACL/IControlAttributesStorage.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ROLE_NOT_FOUND;
    extern const int ROLE_ALREADY_EXISTS;
}


namespace AccessControlNames
{
    extern const size_t ROLE_NAMESPACE_IDX;
}


const ConstRole::Type ConstRole::Attributes::TYPE = {"Role",
                                                     nullptr,
                                                     ErrorCodes::ROLE_NOT_FOUND,
                                                     ErrorCodes::ROLE_ALREADY_EXISTS,
                                                     AccessControlNames::ROLE_NAMESPACE_IDX};

const ConstRole::Type & ConstRole::TYPE = Role::Attributes::TYPE;


bool ConstRole::Attributes::equal(const IControlAttributes & other) const
{
    return IControlAttributes::equal(other);
}


ConstRole::AttributesPtr ConstRole::getAttributes() const
{
    return storage.read<Attributes>(id);
}


ConstRole::AttributesPtr ConstRole::tryGetAttributes() const
{
    return storage.tryRead<Attributes>(id);
}


void Role::update(const std::function<void(Attributes &)> & update_func)
{
    getStorage().update(id, update_func);
}


void Role::drop(bool if_exists)
{
    if (if_exists)
         getStorage().tryRemove(id);
    else
         getStorage().remove(id, TYPE);
}
}
