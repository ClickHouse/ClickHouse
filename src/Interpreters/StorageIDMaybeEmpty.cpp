#include <Interpreters/StorageIDMaybeEmpty.h>


namespace DB
{

StorageIDMaybeEmpty::StorageIDMaybeEmpty()
    : StorageIDMaybeEmpty(StorageID::createEmpty())
{
}


StorageIDMaybeEmpty::StorageIDMaybeEmpty(const StorageID & other) // NOLINT this is an implicit c-tor
    : StorageID(other)
{
}


bool StorageIDMaybeEmpty::operator==(const StorageIDMaybeEmpty & other) const
{
    if (empty() && other.empty())
        return true;
    if (empty() || other.empty())
        return false;
    return StorageID::operator==(other);
}

}
