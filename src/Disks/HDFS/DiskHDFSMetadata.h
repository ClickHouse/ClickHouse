#pragma once

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>


namespace DB
{

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
    explicit Metadata(
        const String & disk_path_,
        const String & metadata_file_path_,
        bool create = false)
        : disk_path(disk_path_)
        , metadata_file_path(metadata_file_path_)
        , total_size(0)
        , hdfs_objects(0)
        , ref_count(0)
    {
        if (create)
            return;

        ReadBufferFromFile buf(disk_path + metadata_file_path, 1024); /* reasonable buffer size for small file */

        UInt32 version;
        readIntText(version, buf);

        if (version != VERSION)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
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
