#include <Storages/IStorage.h>


namespace DB
{

TableStructureReadLock::TableStructureReadLock(StoragePtr storage_, bool lock_structure, bool lock_data)
    : storage(storage_), data_lock(storage->data_lock, std::defer_lock), structure_lock(storage->structure_lock, std::defer_lock)
{
    if (lock_data)
        data_lock.lock();
    if (lock_structure)
        structure_lock.lock();
}

}
