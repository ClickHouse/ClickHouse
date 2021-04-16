#pragma once

#include <IO/WriteBufferFromFile.h>
#include <Disks/IDiskRemote.h>


namespace DB
{

/// Stores data in HDFS and adds the object key (HDFS path) and object size to metadata file on local FS.
class WriteIndirectBufferFromHDFS final : public WriteBufferFromFileBase
{
public:
    WriteIndirectBufferFromHDFS(
        const Poco::Util::AbstractConfiguration & config_,
        const String & hdfs_name_,
        const String & hdfs_path_,
        DiskHDFS::Metadata metadata_,
        size_t buf_size_)
        : WriteBufferFromFileBase(buf_size_, nullptr, 0)
        , impl(WriteBufferFromHDFS(hdfs_name_, config_, buf_size_))
        , metadata(std::move(metadata_))
        , hdfs_path(hdfs_path_)
    {
    }

    ~WriteIndirectBufferFromHDFS() override
    {
        try
        {
            finalize();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    void finalize() override
    {
        if (finalized)
            return;

        next();
        impl.finalize();

        metadata.addObject(hdfs_path, count());
        metadata.save();

        finalized = true;
    }

    void sync() override
    {
        if (finalized)
            metadata.save(true);
    }

    std::string getFileName() const override { return metadata.metadata_file_path; }

private:
    void nextImpl() override
    {
        /// Transfer current working buffer to WriteBufferFromHDFS.
        impl.swap(*this);

        /// Write actual data to HDFS.
        impl.next();

        /// Return back working buffer.
        impl.swap(*this);
    }

    WriteBufferFromHDFS impl;
    bool finalized = false;
    DiskHDFS::Metadata metadata;
    String hdfs_path;
};

}
