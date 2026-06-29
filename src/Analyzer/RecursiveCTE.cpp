#include <Analyzer/RecursiveCTE.h>

#include <Storages/IStorage.h>

namespace DB
{

RecursiveCTETable::RecursiveCTETable(TemporaryTableHolderPtr holder_,
    StoragePtr storage_,
    NamesAndTypes columns_)
    : holder(std::move(holder_))
    , storage(std::move(storage_))
    , columns(std::move(columns_))
{}

StorageID RecursiveCTETable::getStorageID() const
{
    return storage->getStorageID();
}

}
