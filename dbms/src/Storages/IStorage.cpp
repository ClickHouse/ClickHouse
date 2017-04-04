#include <Storages/IStorage.h>


namespace DB
{

TableStructureReadLock::TableStructureReadLock(StoragePtr storage_, bool lock_structure, bool lock_data)
    : storage(storage_)
{
    if (lock_data)
        data_lock.emplace(storage->data_lock);
    if (lock_structure)
        structure_lock.emplace(storage->structure_lock);
}

}
