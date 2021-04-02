#pragma once

#include <IO/ReadBufferFromFile.h>
#include "DiskHDFSMetadata.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
    extern const int PATH_ACCESS_DENIED;
    extern const int FILE_ALREADY_EXISTS;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int UNKNOWN_FORMAT;
    extern const int CANNOT_REMOVE_FILE;
}


/// Reads data from HDFS using stored paths in metadata.
class ReadIndirectBufferFromHDFS final : public ReadBufferFromFileBase
{
public:
    ReadIndirectBufferFromHDFS(
            const Poco::Util::AbstractConfiguration & config_,
            const String & hdfs_name_,
            const String & /* bucket */,
            Metadata metadata_,
            size_t buf_size_)
        : config(config_)
        , hdfs_name(hdfs_name_)
        , metadata(std::move(metadata_))
        , buf_size(buf_size_)
    {
    }

    off_t seek(off_t offset_, int whence) override
    {
        if (whence == SEEK_CUR)
        {
            /// If position within current working buffer - shift pos.
            if (working_buffer.size() && size_t(getPosition() + offset_) < absolute_position)
            {
                pos += offset_;
                return getPosition();
            }
            else
            {
                absolute_position += offset_;
            }
        }
        else if (whence == SEEK_SET)
        {
            /// If position within current working buffer - shift pos.
            if (working_buffer.size() && size_t(offset_) >= absolute_position - working_buffer.size()
                && size_t(offset_) < absolute_position)
            {
                pos = working_buffer.end() - (absolute_position - offset_);
                return getPosition();
            }
            else
            {
                absolute_position = offset_;
            }
        }
        else
            throw Exception("Only SEEK_SET or SEEK_CUR modes are allowed.", ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

        current_buf = initialize();
        pos = working_buffer.end();

        return absolute_position;
    }

    off_t getPosition() override { return absolute_position - available(); }

    std::string getFileName() const override { return metadata.metadata_file_path; }


private:
    std::unique_ptr<ReadBufferFromHDFS> initialize()
    {
        size_t offset = absolute_position;

        for (size_t i = 0; i < metadata.hdfs_objects.size(); ++i)
        {
            current_buf_idx = i;
            const auto & [path, size] = metadata.hdfs_objects[i];

            if (size > offset)
            {
                auto buf = std::make_unique<ReadBufferFromHDFS>(hdfs_name + path, config, buf_size);
                //buf->seek(offset, SEEK_SET);
                return buf;
            }
            offset -= size;

        }

        return nullptr;
    }

    bool nextImpl() override
    {
        /// Find first available buffer that fits to given offset.
        if (!current_buf)
            current_buf = initialize();

        /// If current buffer has remaining data - use it.
        if (current_buf && current_buf->next())
        {
            working_buffer = current_buf->buffer();
            absolute_position += working_buffer.size();
            return true;
        }

        /// If there is no available buffers - nothing to read.
        if (current_buf_idx + 1 >= metadata.hdfs_objects.size())
            return false;

        ++current_buf_idx;
        const auto & path = metadata.hdfs_objects[current_buf_idx].first;
        current_buf = std::make_unique<ReadBufferFromHDFS>(hdfs_name + path, config, buf_size);
        current_buf->next();
        working_buffer = current_buf->buffer();
        absolute_position += working_buffer.size();

        return true;
    }

    const Poco::Util::AbstractConfiguration & config;
    const String & hdfs_name;
    Metadata metadata;
    size_t buf_size;

    size_t absolute_position = 0;
    size_t current_buf_idx = 0;
    std::unique_ptr<ReadBufferFromHDFS> current_buf;
};

}
