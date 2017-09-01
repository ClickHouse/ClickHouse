#include <Storages/IStorage.h>


namespace DB
{

TableStructureReadLock::TableStructureReadLock(StoragePtr storage_, bool lock_structure, bool lock_data, const std::string & who)
    : storage(storage_)
{
    if (lock_data)
        data_lock = storage->data_lock->getLock(RWLockFIFO::Read, who);
    if (lock_structure)
        structure_lock = storage->structure_lock->getLock(RWLockFIFO::Read, who);
}

}
