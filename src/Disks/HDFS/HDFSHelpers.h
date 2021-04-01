#pragma once
#include "DiskHDFS.h"

#include <Storages/HDFS/ReadBufferFromHDFS.h>
#include <Storages/HDFS/WriteBufferFromHDFS.h>

#include <random>
#include <utility>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Poco/File.h>
#include <Interpreters/Context.h>
#include <Common/checkStackSize.h>
#include <Common/createHardLink.h>
#include <Common/quoteString.h>
#include <Common/filesystemHelpers.h>
#include <common/logger_useful.h>
#include <Common/thread_local_rng.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

namespace
{
    String getRandomName()
    {
        std::uniform_int_distribution<int> distribution('a', 'z');
        String res(32, ' '); /// The number of bits of entropy should be not less than 128.
        for (auto & c : res)
            c = distribution(thread_local_rng);
        return res;
    }

    /*
    template <typename Result, typename Error>
    void throwIfError(Aws::Utils::Outcome<Result, Error> && response)
    {
        if (!response.IsSuccess())
        {
            const auto & err = response.GetError();
            throw Exception(err.GetMessage(), static_cast<int>(err.GetErrorType()));
        }
    }
    */

    struct Metadata
    {
        /// Metadata file version.
        static constexpr UInt32 VERSION = 1;

        using PathAndSize = std::pair<String, size_t>;

        /// Disk path.
        const String & disk_path;
        /// Relative path to metadata file on local FS.
        String metadata_file_path;
        /// Total size of all HDFS objects.
        size_t total_size;
        /// HDFS objects paths and their sizes.
        std::vector<PathAndSize> hdfs_objects;
        /// Number of references (hardlinks) to this metadata file.
        UInt32 ref_count;

        /// Load metadata by path or create empty if `create` flag is set.
        explicit Metadata(const String & disk_path_, const String & metadata_file_path_, bool create = false)
            : disk_path(disk_path_), metadata_file_path(metadata_file_path_), total_size(0), hdfs_objects(0), ref_count(0)
        {
            if (create)
                return;

            DB::ReadBufferFromFile buf(disk_path + metadata_file_path, 1024); /* reasonable buffer size for small file */

            UInt32 version;
            readIntText(version, buf);

            if (version != VERSION)
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR,
                    "Unknown metadata file version. Path: {}. Version: {}. Expected version: {}",
                    disk_path + metadata_file_path, std::to_string(version), std::to_string(VERSION));

            assertChar('\n', buf);

            UInt32 hdfs_objects_count;
            readIntText(hdfs_objects_count, buf);
            assertChar('\t', buf);
            readIntText(total_size, buf);
            assertChar('\n', buf);
            hdfs_objects.resize(hdfs_objects_count);
            for (UInt32 i = 0; i < hdfs_objects_count; ++i)
            {
                String hdfs_object_path;
                size_t hdfs_object_size;
                readIntText(hdfs_object_size, buf);
                assertChar('\t', buf);
                readEscapedString(hdfs_object_path, buf);
                assertChar('\n', buf);
                hdfs_objects[i] = {hdfs_object_path, hdfs_object_size};
            }

            readIntText(ref_count, buf);
            assertChar('\n', buf);
        }

        void addObject(const String & path, size_t size)
        {
            total_size += size;
            hdfs_objects.emplace_back(path, size);
        }

        /// Fsync metadata file if 'sync' flag is set.
        void save(bool sync = false)
        {
            DB::WriteBufferFromFile buf(disk_path + metadata_file_path, 1024);

            writeIntText(VERSION, buf);
            writeChar('\n', buf);

            writeIntText(hdfs_objects.size(), buf);
            writeChar('\t', buf);
            writeIntText(total_size, buf);
            writeChar('\n', buf);
            for (const auto & [hdfs_object_path, hdfs_object_size] : hdfs_objects)
            {
                writeIntText(hdfs_object_size, buf);
                writeChar('\t', buf);
                writeEscapedString(hdfs_object_path, buf);
                writeChar('\n', buf);
            }

            writeIntText(ref_count, buf);
            writeChar('\n', buf);

            buf.finalize();
            if (sync)
                buf.sync();
        }
    };


}

namespace DB
{

class DiskHDFSDirectoryIterator final : public IDiskDirectoryIterator
{
public:
    DiskHDFSDirectoryIterator(const String & full_path, const String & folder_path_) : iter(full_path), folder_path(folder_path_) {}

    void next() override { ++iter; }

    bool isValid() const override { return iter != Poco::DirectoryIterator(); }

    String path() const override
    {
        if (iter->isDirectory())
            return folder_path + iter.name() + '/';
        else
            return folder_path + iter.name();
    }

    String name() const override { return iter.name(); }

private:
    Poco::DirectoryIterator iter;
    String folder_path;
};


class DiskHDFSReservation : public IReservation
{
public:
    DiskHDFSReservation(const DiskHDFSPtr & disk_, UInt64 size_)
        : disk(disk_), size(size_), metric_increment(CurrentMetrics::DiskSpaceReservedForMerge, size_)
    {
    }

    ~DiskHDFSReservation() override
    {
        try
        {
            std::lock_guard lock(disk->reservation_mutex);
            if (disk->reserved_bytes < size)
            {
                disk->reserved_bytes = 0;
                //LOG_ERROR(&Poco::Logger::get("DiskLocal"), "Unbalanced reservations size for disk '{}'", disk->getName());
            }
            else
            {
                disk->reserved_bytes -= size;
            }

            if (disk->reservation_count == 0)
            {
                //LOG_ERROR(&Poco::Logger::get("DiskLocal"), "Unbalanced reservation count for disk '{}'", disk->getName());
            }
            else
                --disk->reservation_count;
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    UInt64 getSize() const override { return size; }

    DiskPtr getDisk(size_t /* i */) const override { return disk; }

    void update(UInt64 new_size) override
    {
        std::lock_guard lock(disk->reservation_mutex);
        disk->reserved_bytes -= size;
        size = new_size;
        disk->reserved_bytes += size;
    }

    Disks getDisks() const override { return {}; }

private:
    DiskHDFSPtr disk;
    UInt64 size;
    CurrentMetrics::Increment metric_increment;
};

}
