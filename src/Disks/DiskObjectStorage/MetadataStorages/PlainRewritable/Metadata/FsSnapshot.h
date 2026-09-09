#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/NormalizedPath.h>

#include <Common/CurrentMetrics.h>

#include <base/defines.h>

#include <unordered_map>
#include <memory>
#include <optional>
#include <string>
#include <vector>
#include <mutex>

namespace DB
{

struct FileRemoteInfo
{
    size_t bytes_size = 0;
    time_t last_modified = 0;
    /// Key of the blob relative to the common key prefix of the object storage, e.g. `aaealinyzgdzycgcnpgaapdssrjirnnr/hello.json`.
    /// Empty when the blob is stored at the default location, `<directory remote path>/<file name>`.
    /// Files that were hard-linked from another directory point to the blob of the original file, which lives elsewhere.
    std::string blob_key;
};

struct DirectoryRemoteInfo
{
    std::string remote_path;
    std::string etag;
    time_t last_modified = 0;
    std::unordered_map<std::string, FileRemoteInfo> files;
    /// The `prefix.path` object of this directory lists the files explicitly (see `PrefixPath.h`).
    /// In this form the set of files is defined by the list, not by the blobs stored under the directory prefix,
    /// so the blobs may be shared with other directories (hard links) and may have arbitrary names.
    bool has_explicit_file_list = false;
};

/// Key of the blob of a file, relative to the common key prefix of the object storage.
std::string getBlobKey(const DirectoryRemoteInfo & directory, const std::string & file_name, const FileRemoteInfo & file);
/// Blob key of a file stored at the default location.
std::string getDefaultBlobKey(const std::string & directory_remote_path, const std::string & file_name);

class BlobLinkCounts;

struct FsNode : public std::enable_shared_from_this<FsNode>
{
    std::optional<DirectoryRemoteInfo> info = {};
    std::unordered_map<std::string, std::shared_ptr<FsNode>> subdirectories = {};
};

/// Mutable snapshot of the virtual file system tree.
///
/// The tree itself is immutable and shared between snapshots (copy-on-write), so a snapshot can be discarded at any point.
/// The numbers of links to the blobs are shared with the committed metadata (`BlobLinkCounts`), and the snapshot records
/// its changes to them as deltas, which are applied to the committed state together with the tree in `FsMetadata::applySnapshot`.
class FsSnapshot
{
public:
    explicit FsSnapshot(std::shared_ptr<BlobLinkCounts> blob_link_counts_);
    FsSnapshot(std::shared_ptr<FsNode> root_, std::shared_ptr<BlobLinkCounts> blob_link_counts_);

    /// Directory Write Methods

    void recordDirectoryPath(const std::string & path, DirectoryRemoteInfo info);
    void moveDirectory(const std::string & from, const std::string & to);
    void removeDirectory(const std::string & path);
    /// Switches the directory to the explicit file list form. This is a one-way transition.
    void markDirectoryExplicit(const std::string & path);

    /// File Write Methods

    void recordFile(const std::string & path, FileRemoteInfo info);
    void removeFile(const std::string & path);

    /// Blob Link Methods. The blob must be referenced by at least one file.

    /// The number of files sharing the blob.
    uint32_t getBlobLinkCount(const std::string & blob_key) const;
    void addBlobLink(const std::string & blob_key);
    void removeBlobLink(const std::string & blob_key);

    /// Directory Read Methods

    std::vector<std::string> listDirectory(const std::string & path) const;
    bool existsDirectory(const std::string & path) const;
    std::unordered_map<std::string, std::optional<DirectoryRemoteInfo>> getSubtreeRemoteInfo(const std::string & path) const;
    std::optional<DirectoryRemoteInfo> getDirectoryRemoteInfo(const std::string & path) const;

    /// File Read Methods

    std::optional<FileRemoteInfo> getFileRemoteInfo(const std::string & path) const;
    bool existsFile(const std::string & path) const;

    /// Snapshot Methods

    std::shared_ptr<FsNode> getRoot() const;
    /// Also forgets all the deltas accumulated by the snapshot.
    void setRoot(std::shared_ptr<FsNode> new_root);
    std::pair<int64_t, int64_t> getRemoteLayoutDeltas() const;
    std::unordered_map<std::string, int64_t> getBlobLinkDeltas() const;
    /// Forgets the accumulated deltas without changing the tree. Called after the deltas have been folded
    /// into the committed state, so a snapshot promoted to the committed one does not double-count them.
    void resetDeltas();

private:
    mutable std::mutex mutex;
    std::shared_ptr<FsNode> root TSA_GUARDED_BY(mutex);
    const std::shared_ptr<BlobLinkCounts> blob_link_counts;
    std::unordered_map<std::string, int64_t> blob_link_deltas TSA_GUARDED_BY(mutex);
    mutable int64_t remote_layout_directories_delta TSA_GUARDED_BY(mutex) = 0;
    mutable int64_t remote_layout_files_delta TSA_GUARDED_BY(mutex) = 0;
};

}
