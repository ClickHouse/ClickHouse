#pragma once

#include <string>
#include <vector>
#include <boost/noncopyable.hpp>
#include <Disks/IDisk.h>

namespace DB
{

struct RemoveRequest
{
    std::string path; /// Relative path.
    bool if_exists = false;

    explicit RemoveRequest(std::string path_, bool if_exists_ = false)
        : path(std::move(path_)), if_exists(std::move(if_exists_))
    {
    }
};

using RemoveBatchRequest = std::vector<RemoveRequest>;

/// Simple interface batch execution of write disk operations.
/// Method are almost equal to disk methods.
struct IDiskTransaction : private boost::noncopyable
{
public:
    /// Tries to commit all accumulated operations simultaneously.
    /// If something fails rollback and throw exception.
    virtual void commit() = 0;

    virtual ~IDiskTransaction() = default;

    /// Create directory.
    virtual void createDirectory(const std::string & path) = 0;

    /// Create directory and all parent directories if necessary.
    virtual void createDirectories(const std::string & path) = 0;

    /// Remove all files from the directory. Directories are not removed.
    virtual void clearDirectory(const std::string & path) = 0;

    /// Move directory from `from_path` to `to_path`.
    virtual void moveDirectory(const std::string & from_path, const std::string & to_path) = 0;

    virtual void moveFile(const String & from_path, const String & to_path) = 0;

    virtual void createFile(const String & path) = 0;

    /// Move the file from `from_path` to `to_path`.
    /// If a file with `to_path` path already exists, it will be replaced.
    virtual void replaceFile(const std::string & from_path, const std::string & to_path) = 0;

    /// Only copy of several files supported now. Disk interface support copy to another disk
    /// but it's impossible to implement correctly in transactions because other disk can
    /// use different metadata storage.
    /// TODO: maybe remove it at all, we don't want copies
    virtual void copyFile(const std::string & from_file_path, const std::string & to_file_path) = 0;

    /// Open the file for write and return WriteBufferFromFileBase object.
    virtual std::unique_ptr<WriteBufferFromFileBase> writeFile( /// NOLINT
        const std::string & path,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        WriteMode mode = WriteMode::Rewrite,
        const WriteSettings & settings = {},
        bool autocommit = true) = 0;

    /// Remove file. Throws exception if file doesn't exists or it's a directory.
    virtual void removeFile(const std::string & path) = 0;

    /// Remove file if it exists.
    virtual void removeFileIfExists(const std::string & path) = 0;

    /// Remove directory. Throws exception if it's not a directory or if directory is not empty.
    virtual void removeDirectory(const std::string & path) = 0;

    /// Remove file or directory with all children. Use with extra caution. Throws exception if file doesn't exists.
    virtual void removeRecursive(const std::string & path) = 0;

    /// Remove file. Throws exception if file doesn't exists or if directory is not empty.
    /// Differs from removeFile for S3/HDFS disks
    /// Second bool param is a flag to remove (true) or keep (false) shared data on S3
    virtual void removeSharedFile(const std::string & path, bool /* keep_shared_data */) = 0;

    /// Remove file or directory with all children. Use with extra caution. Throws exception if file doesn't exists.
    /// Differs from removeRecursive for S3/HDFS disks
    /// Second bool param is a flag to remove (false) or keep (true) shared data on S3.
    /// Third param determines which files cannot be removed even if second is true.
    virtual void removeSharedRecursive(const std::string & path, bool /* keep_all_shared_data */, const NameSet & /* file_names_remove_metadata_only */) = 0;

    /// Remove file or directory if it exists.
    /// Differs from removeFileIfExists for S3/HDFS disks
    /// Second bool param is a flag to remove (true) or keep (false) shared data on S3
    virtual void removeSharedFileIfExists(const std::string & path, bool /* keep_shared_data */) = 0;

    /// Batch request to remove multiple files.
    /// May be much faster for blob storage.
    /// Second bool param is a flag to remove (true) or keep (false) shared data on S3.
    /// Third param determines which files cannot be removed even if second is true.
    virtual void removeSharedFiles(const RemoveBatchRequest & files, bool keep_all_batch_data, const NameSet & file_names_remove_metadata_only) = 0;

    /// Set last modified time to file or directory at `path`.
    virtual void setLastModified(const std::string & path, const Poco::Timestamp & timestamp) = 0;

    /// Set file at `path` as read-only.
    virtual void setReadOnly(const std::string & path) = 0;

    /// Create hardlink from `src_path` to `dst_path`.
    virtual void createHardLink(const std::string & src_path, const std::string & dst_path) = 0;
};

using DiskTransactionPtr = std::shared_ptr<IDiskTransaction>;

}
