#include <ACL/User2.h>
#include <ACL/IControlAttributesStorage.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int USER_NOT_FOUND;
    extern const int USER_ALREADY_EXISTS;
}


namespace AccessControlNames
{
    extern const size_t ROLE_NAMESPACE_IDX;
}


const ConstUser::Type ConstUser::Attributes::TYPE{"User",
                                                  AccessControlNames::ROLE_NAMESPACE_IDX,
                                                  &Role::TYPE,
                                                  ErrorCodes::USER_NOT_FOUND,
                                                  ErrorCodes::USER_ALREADY_EXISTS};

const ConstUser::Type & ConstUser::TYPE = ConstUser::Attributes::TYPE;


bool ConstUser::Attributes::equal(const IControlAttributes & other) const
{
    if (!ConstRole::Attributes::equal(other))
        return false;
    //const auto & o = *other.cast<Attributes>();
    return true;
}


bool ConstUser::Attributes::hasReferences(const UUID & id) const
{
    return ConstRole::Attributes::hasReferences(id);
}


void ConstUser::Attributes::removeReferences(const UUID & id)
{
    ConstRole::Attributes::removeReferences(id);
}


ConstUser::AttributesPtr ConstUser::getAttributes() const
{
    return storage.read<Attributes>(id);
}


ConstUser::AttributesPtr ConstUser::tryGetAttributes() const
{
    return storage.tryRead<Attributes>(id);
}


void User2::update(const std::function<void(Attributes &)> & update_func)
{
    getStorage().update(id, update_func);
}


void User2::drop(bool if_exists)
{
    if (if_exists)
         getStorage().tryRemove(id);
    else
         getStorage().remove(id, TYPE);
}
}
