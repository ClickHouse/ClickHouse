#include <Disks/IDisk.h>
#include <Disks/DiskLocal.h>
#include <Disks/DiskMemory.h>

template <typename T>
DB::DiskPtr createDisk();

template <>
DB::DiskPtr createDisk<DB::DiskMemory>();

template <>
DB::DiskPtr createDisk<DB::DiskLocal>();

template <typename T>
void destroyDisk(DB::DiskPtr & disk);

template <>
void destroyDisk<DB::DiskLocal>(DB::DiskPtr & disk);
