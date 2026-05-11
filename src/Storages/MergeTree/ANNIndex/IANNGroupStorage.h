#pragma once

#include <IO/ReadSettings.h>
#include <IO/WriteSettings.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Disks/WriteMode.h>
#include <Poco/Timestamp.h>

#include <memory>
#include <optional>
#include <string>

namespace DB
{

/// Storage handle for a single group directory (`ann_<uuid>/`, `tmp_ann_<uuid>/`, or
/// `deleting_ann_<uuid>/`) used by the table-level ANN index.
///
/// This is the **only** entry point into the filesystem for code that operates inside a
/// group directory. The terminal implementation (`ANNGroupStorageDiskFull`) is the only
/// file in the whole ANN module that is allowed to call `IDisk` directly.
///
/// Table-level artefacts (`meta.json`, group-directory enumeration, etc.) are handled
/// elsewhere — they are not per-group, so they are accessed through `VolumePtr +
/// relative_root_path` directly. Per-group reads/writes must flow through this interface.
class IANNGroupStorage
{
public:
    virtual ~IANNGroupStorage() = default;

    /// Absolute OS path of the group directory. Needed by the FFI layer that owns `mmap`.
    virtual std::string getFullPath() const = 0;

    /// Path relative to the volume root, e.g. `data/db/tbl/anns/ann_<uuid>`.
    virtual std::string getRelativePath() const = 0;

    /// Last path component, e.g. `ann_<uuid>`, `tmp_ann_<uuid>`, or `deleting_ann_<uuid>`.
    virtual std::string getGroupDir() const = 0;

    virtual bool exists() const = 0;
    virtual bool existsFile(const std::string & name) const = 0;

    virtual size_t getFileSize(const std::string & name) const = 0;
    virtual Poco::Timestamp getFileLastModified(const std::string & name) const = 0;

    virtual std::unique_ptr<ReadBufferFromFileBase> readFile(
        const std::string & name,
        const ReadSettings & settings,
        std::optional<size_t> read_hint) const = 0;

    virtual std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const std::string & name,
        size_t buf_size,
        WriteMode mode,
        const WriteSettings & settings) = 0;

    virtual void removeFileIfExists(const std::string & name) = 0;

    /// Rename a file inside the group directory. `to` must not already exist.
    virtual void renameFile(const std::string & from, const std::string & to) = 0;

    /// Move the entire group directory to `new_relative_path` (relative to the volume root).
    /// After a successful call both `getRelativePath` and `getFullPath` return the new location.
    virtual void renameDirectoryTo(const std::string & new_relative_path) = 0;
};

using ANNGroupStoragePtr = std::shared_ptr<IANNGroupStorage>;

}
