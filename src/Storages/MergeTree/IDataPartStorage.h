#pragma once
#include <IO/ReadSettings.h>
#include <base/types.h>
#include <Core/NamesAndTypes.h>
#include <optional>

namespace DB
{

class ReadBufferFromFileBase;


class IDiskDirectoryIterator;
using DiskDirectoryIteratorPtr = std::unique_ptr<IDiskDirectoryIterator>;

struct MergeTreeDataPartChecksums;

class IStoragePolicy;

/// This is an abstraction of storage for data part files.
/// Generally, it contains read-only methods from IDisk.
class IDataPartStorage
{
private:
public:
    virtual ~IDataPartStorage() = default;

        /// Open the file for read and return ReadBufferFromFileBase object.
    virtual std::unique_ptr<ReadBufferFromFileBase> readFile(
        const std::string & path,
        const ReadSettings & settings,
        std::optional<size_t> read_hint,
        std::optional<size_t> file_size) const = 0;

    virtual bool exists(const std::string & path) const = 0;
    virtual bool exists() const = 0;

    virtual Poco::Timestamp getLastModified() const = 0;

    virtual DiskDirectoryIteratorPtr iterate() const = 0;
    virtual DiskDirectoryIteratorPtr iterateDirectory(const std::string & path) const = 0;

    virtual size_t getFileSize(const std::string & path) const = 0;

    virtual std::string getFullPath() const = 0;
    virtual std::string getFullRelativePath() const = 0;

    virtual UInt64 calculateTotalSizeOnDisk() const = 0;

    virtual bool isStoredOnRemoteDisk() const { return false; }
    virtual bool supportZeroCopyReplication() const { return false; }

    /// Should remove it later
    virtual void writeChecksums(MergeTreeDataPartChecksums & checksums) const = 0;
    virtual void writeColumns(NamesAndTypesList & columns) const = 0;
    virtual void writeDeleteOnDestroyMarker(Poco::Logger * log) const = 0;

    /// A leak of abstraction
    virtual bool shallParticipateInMerges(const IStoragePolicy &) const { return true; }

    virtual void rename(const String & new_relative_path, Poco::Logger * log, bool remove_new_dir_if_exists, bool fsync);

    /// Disk name
    virtual std::string getName() const = 0;

    virtual std::shared_ptr<IDataPartStorage> getProjection(const std::string & name) const = 0;
};

class IDataPartStorageBuilder
{
public:
    virtual ~IDataPartStorageBuilder() = default;
};

using DataPartStoragePtr = std::shared_ptr<IDataPartStorage>;

}
