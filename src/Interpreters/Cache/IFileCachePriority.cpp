#include "IFileCachePriority.h"
#include <Interpreters/Cache/FileCache.h>


namespace DB
{

IFileCachePriority::Entry::Entry(
    const Key & key_,
    size_t offset_,
    size_t size_,
    LockedKeyCreatorPtr key_transaction_creator_)
    : key(key_)
    , offset(offset_)
    , size(size_)
    , key_transaction_creator(std::move(key_transaction_creator_))
{
}

LockedKeyPtr IFileCachePriority::Entry::createLockedKey() const
{
    return key_transaction_creator->create();
}

}
