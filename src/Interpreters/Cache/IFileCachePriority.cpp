#include "IFileCachePriority.h"
#include <Interpreters/Cache/FileCache.h>


namespace DB
{

IFileCachePriority::Entry::Entry(
    const Key & key_,
    size_t offset_,
    size_t size_,
    KeyTransactionCreatorPtr key_transaction_creator_)
    : key(key_)
    , offset(offset_)
    , size(size_)
    , key_transaction_creator(std::move(key_transaction_creator_))
{
}

KeyTransactionPtr IFileCachePriority::Entry::createKeyTransaction() const
{
    return key_transaction_creator->create();
}

}
