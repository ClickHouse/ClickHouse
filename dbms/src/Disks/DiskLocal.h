#pragma once

#include <Disks/IDisk.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>

#include <mutex>
#include <Poco/DirectoryIterator.h>
#include <Poco/File.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class DiskLocalReservation;

class DiskLocal : public IDisk
{
public:
    friend class DiskLocalReservation;

    DiskLocal(const String & name_, const String & path_, UInt64 keep_free_space_bytes_)
        : name(name_), disk_path(path_), keep_free_space_bytes(keep_free_space_bytes_)
    {
        if (disk_path.back() != '/')
            throw Exception("Disk path must ends with '/', but '" + disk_path + "' doesn't.", ErrorCodes::LOGICAL_ERROR);
    }

    const String & getName() const override { return name; }

    const String & getPath() const override { return disk_path; }

    ReservationPtr reserve(UInt64 bytes) override;

    UInt64 getTotalSpace() const override;

    UInt64 getAvailableSpace() const override;

    UInt64 getUnreservedSpace() const override;

    UInt64 getKeepingFreeSpace() const override { return keep_free_space_bytes; }

    bool exists(const String & path) const override;

    bool isFile(const String & path) const override;

    bool isDirectory(const String & path) const override;

    void createDirectory(const String & path) override;

    void createDirectories(const String & path) override;

    DiskDirectoryIteratorPtr iterateDirectory(const String & path) override;

    void moveFile(const String & from_path, const String & to_path) override;

    void copyFile(const String & from_path, const String & to_path) override;

    std::unique_ptr<ReadBuffer> readFile(const String & path) const override;

    std::unique_ptr<WriteBuffer> writeFile(const String & path) override;

private:
    bool tryReserve(UInt64 bytes);

private:
    const String name;
    const String disk_path;
    const UInt64 keep_free_space_bytes;

    /// Used for reservation counters modification
    static std::mutex mutex;
    UInt64 reserved_bytes = 0;
    UInt64 reservation_count = 0;
};

using DiskLocalPtr = std::shared_ptr<DiskLocal>;


class DiskLocalDirectoryIterator : public IDiskDirectoryIterator
{
public:
    explicit DiskLocalDirectoryIterator(const String & path) : iter(path) {}

    void next() override { ++iter; }

    bool isValid() const override { return iter != Poco::DirectoryIterator(); }

    String name() const override { return iter.name(); }

private:
    Poco::DirectoryIterator iter;
};

class DiskLocalReservation : public IReservation
{
public:
    DiskLocalReservation(const DiskLocalPtr & disk_, UInt64 size_)
        : disk(disk_), size(size_), metric_increment(CurrentMetrics::DiskSpaceReservedForMerge, size_)
    {
    }

    UInt64 getSize() const override { return size; }

    DiskPtr getDisk() const override { return disk; }

    void update(UInt64 new_size) override;

    ~DiskLocalReservation() override;

private:
    DiskLocalPtr disk;
    UInt64 size;
    CurrentMetrics::Increment metric_increment;
};

class DiskFactory;
void registerDiskLocal(DiskFactory & factory);

}
