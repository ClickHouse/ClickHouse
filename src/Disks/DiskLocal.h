#pragma once

#include <Disks/IDisk.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFile.h>

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

    size_t getFileSize(const String & path) const override;

    void createDirectory(const String & path) override;

    void createDirectories(const String & path) override;

    void clearDirectory(const String & path) override;

    void moveDirectory(const String & from_path, const String & to_path) override;

    DiskDirectoryIteratorPtr iterateDirectory(const String & path) override;

    void createFile(const String & path) override;

    void moveFile(const String & from_path, const String & to_path) override;

    void replaceFile(const String & from_path, const String & to_path) override;

    void copyFile(const String & from_path, const String & to_path) override;

    void copy(const String & from_path, const std::shared_ptr<IDisk> & to_disk, const String & to_path) override;

    void listFiles(const String & path, std::vector<String> & file_names) override;

    std::unique_ptr<ReadBufferFromFileBase> readFile(
        const String & path,
        size_t buf_size,
        size_t estimated_size,
        size_t aio_threshold,
        size_t mmap_threshold) const override;

    std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const String & path,
        size_t buf_size,
        WriteMode mode,
        size_t estimated_size,
        size_t aio_threshold) override;

    void remove(const String & path) override;

    void removeRecursive(const String & path) override;

    void setLastModified(const String & path, const Poco::Timestamp & timestamp) override;

    Poco::Timestamp getLastModified(const String & path) override;

    void setReadOnly(const String & path) override;

    void createHardLink(const String & src_path, const String & dst_path) override;

    int open(const String & path, mode_t mode) const override;
    void close(int fd) const override;
    void sync(int fd) const override;

    void truncateFile(const String & path, size_t size) override;

    const String getType() const override { return "local"; }

private:
    bool tryReserve(UInt64 bytes);

private:
    const String name;
    const String disk_path;
    const UInt64 keep_free_space_bytes;

    UInt64 reserved_bytes = 0;
    UInt64 reservation_count = 0;

    static std::mutex reservation_mutex;
};

}
