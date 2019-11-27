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
        : name(name_), path(path_), keep_free_space_bytes(keep_free_space_bytes_)
    {
        if (path.back() != '/')
            throw Exception("Disk path must ends with '/', but '" + path + "' doesn't.", ErrorCodes::LOGICAL_ERROR);
    }

    const String & getName() const override { return name; }

    const String & getPath() const override { return path; }

    ReservationPtr reserve(UInt64 bytes) const override;

    UInt64 getTotalSpace() const override;

    UInt64 getAvailableSpace() const override;

    UInt64 getUnreservedSpace() const override;

    UInt64 getKeepingFreeSpace() const override { return keep_free_space_bytes; }

    DiskFilePtr file(const String & path) const override;

private:
    bool tryReserve(UInt64 bytes) const;

private:
    const String name;
    const String path;
    const UInt64 keep_free_space_bytes;

    /// Used for reservation counters modification
    static std::mutex mutex;
    mutable UInt64 reserved_bytes = 0;
    mutable UInt64 reservation_count = 0;
};

using DiskLocalPtr = std::shared_ptr<const DiskLocal>;


class DiskLocalFile : public IDiskFile
{
public:
    DiskLocalFile(const DiskPtr & disk_ptr_, const String & rel_path_)
        : IDiskFile(disk_ptr_, rel_path_), file(disk_ptr->getPath() + rel_path)
    {
    }

    bool exists() const override { return file.exists(); }

    bool isDirectory() const override { return file.isDirectory(); }

    void createDirectory() override { file.createDirectory(); }

    void createDirectories() override { file.createDirectories(); }

    void moveTo(const String & new_path) override { file.renameTo(new_path); }

    void copyTo(const String & new_path) override { file.copyTo(new_path); }

    std::unique_ptr<ReadBuffer> read() const override { return std::make_unique<ReadBufferFromFile>(file.path()); }

    std::unique_ptr<WriteBuffer> write() override { return std::make_unique<WriteBufferFromFile>(file.path()); }

private:
    DiskDirectoryIteratorImplPtr iterateDirectory() override;

private:
    Poco::File file;
};

class DiskLocalDirectoryIterator : public IDiskDirectoryIteratorImpl
{
public:
    explicit DiskLocalDirectoryIterator(const DiskFilePtr & parent_);

    const String & name() const override { return iter.name(); }

    const DiskFilePtr & get() const override { return current_file; }

    void next() override;

    bool isValid() const override { return bool(current_file); }

private:
    void updateCurrentFile();

private:
    DiskFilePtr parent;
    Poco::DirectoryIterator iter;
    DiskFilePtr current_file;
};

/**
 * Information about reserved size on concrete local disk.
 * Doesn't reserve bytes in constructor.
 */
class DiskLocalReservation : public IReservation
{
public:
    DiskLocalReservation(const DiskLocalPtr & disk_, UInt64 size_) : IReservation(disk_, size_) {}

    void update(UInt64 new_size) override;

    ~DiskLocalReservation() override;
};

}
