#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/InMemoryDirectoryTree.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableLayout.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>

#include <memory>
#include <string>

namespace DB
{

std::string resolvePlainRewritableRelativeObjectKey(
    const PlainRewritableLayout & layout,
    const DirectoryRemoteInfo & directory_info,
    const std::string & file_name,
    const FileRemoteInfo & file_info);

/// Writes prefix.path for the directory and updates etag/mtime in `directory_info`.
void writePlainRewritablePrefixPath(
    IObjectStorage & object_storage,
    const PlainRewritableLayout & layout,
    const std::filesystem::path & logical_directory_path,
    DirectoryRemoteInfo & directory_info);

/// Converts an implicit directory to explicit form (in memory + on disk). No-op if already explicit.
void ensurePlainRewritableDirectoryExplicit(
    IObjectStorage & object_storage,
    InMemoryDirectoryTree & fs_tree,
    const PlainRewritableLayout & layout,
    const std::string & logical_directory_path);

/// Rewrites prefix.path from the current in-memory directory info (must already be explicit).
void rewritePlainRewritableExplicitPrefixPath(
    IObjectStorage & object_storage,
    InMemoryDirectoryTree & fs_tree,
    const PlainRewritableLayout & layout,
    const std::string & logical_directory_path);

}
