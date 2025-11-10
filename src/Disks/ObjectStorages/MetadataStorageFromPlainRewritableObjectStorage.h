#pragma once

#include <Disks/ObjectStorages/InMemoryDirectoryTree.h>
#include <Disks/ObjectStorages/MetadataStorageFromPlainObjectStorage.h>

#include <memory>

namespace DB
{

/** Stores data in immutable files, but allows atomic directory renames, which is suitable for MergeTree tables.
  *
  * The structure in object storage is as follows:
  * - every directory, regardless of its name and depth, is stored in a randomly-named directory at root;
  * - files inside the directory are stored as is;
  * - additionally, there is metadata, which contains mappings from random names to actual, logical paths;
  * - this directory (named `__meta` and located at root) contains subdirectories with the corresponding random name,
  *   each containing a single file, `prefix.path`, with the content as the logical path of the corresponding directory.
  * - when a logical directory is renamed or moved, we don't touch its randomly assigned name,
  *   and simply rewrite the contents of `prefix.path`.
  *
  * Example. Let's suppose, the logical filesystem structure is:
  * /hello/world/test1.txt
  * /test2.txt
  *
  * The physical structure will be:
  * /__meta/aaealinyzgdzycgcnpgaapdssrjirnnr/prefix.path, contents: /
  * /__meta/gfkoqxvyhaasroiodbeurnftnwieiihy/prefix.path, contents: /hello/world/
  * /__meta/xelohvynszqqinrvcygwzpdwvsklbxkk/prefix.path, contents: /hello/
  * /aaealinyzgdzycgcnpgaapdssrjirnnr/test2.txt
  * /gfkoqxvyhaasroiodbeurnftnwieiihy/test1.txt
  */
class MetadataStorageFromPlainRewritableObjectStorage final : public MetadataStorageFromPlainObjectStorage
{
public:
    MetadataStorageFromPlainRewritableObjectStorage(
        ObjectStoragePtr object_storage_, String storage_path_prefix_, size_t object_metadata_cache_size);

    MetadataStorageType getType() const override { return MetadataStorageType::PlainRewritable; }

    bool existsFile(const std::string & path) const override;

    bool existsDirectory(const std::string & path) const override;

    bool existsFileOrDirectory(const std::string & path) const override;

    std::vector<std::string> listDirectory(const std::string & path) const override;

    std::optional<Poco::Timestamp> getLastModifiedIfExists(const String & path) const override;

    void refresh(UInt64 not_sooner_than_milliseconds) override;

private:
    const std::string metadata_key_prefix;
    std::shared_ptr<InMemoryDirectoryTree> fs_tree;
    AtomicStopwatch previous_refresh;

    void load(bool is_initial_load);
    std::mutex load_mutex;

    std::string getMetadataKeyPrefix() const override { return metadata_key_prefix; }
    std::shared_ptr<InMemoryDirectoryTree> getFsTree() const override { return fs_tree; }
};

}
