#pragma once

#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/Transforms/LazyMaterializingTransform.h>
#include <Storages/prepareReadingFromFormat.h>

namespace DB
{

class StorageFile;

/// Runtime registry of the local files read by the main branch of lazy materialization
/// (the counterpart of `LazyObjectStorageFileRegistry` for the `File` engine and the `file`
/// table function). The main branch's sources register each file when it produces its first
/// chunk and use the index to form the global row index; the lazy branch maps the indexes
/// back to the files. Each entry captures the generation token of the file (see
/// `computeFileCacheVersionToken`) so the lazy reread can prove it sees the same generation.
struct LazyFileRegistry
{
    /// The global row index of a row is (file_index << ROW_INDEX_BITS) | row_index_in_file.
    /// 2^40 rows per file and 2^24 files per query are both far beyond practical limits.
    /// (The layout mirrors `LazyObjectStorageFileRegistry`, but the two are independent:
    /// a query has a single reading step, so the indexes never mix.)
    static constexpr size_t ROW_INDEX_BITS = 40;
    static constexpr UInt64 MAX_ROWS_PER_FILE = 1ul << ROW_INDEX_BITS;
    static constexpr UInt64 ROW_INDEX_MASK = MAX_ROWS_PER_FILE - 1;
    static constexpr UInt64 MAX_FILES = 1ul << (64 - ROW_INDEX_BITS);

    struct FileEntry
    {
        String path;
        /// The generation of the file the main pass read: sub-second mtime + inode + size
        /// (see `computeFileCacheVersionToken`). The lazy pass fails close if a re-stat of
        /// the path produces a different token.
        String version_token;
    };

    std::mutex mutex;
    std::vector<FileEntry> files;

    UInt64 registerFile(const String & path, const String & version_token);
};

using LazyFileRegistryPtr = std::shared_ptr<LazyFileRegistry>;

/// Shared state between the two branches of lazy materialization for the `File` storage.
struct FileLazyMaterializingRows : public ILazyMaterializingRows
{
    struct FileRows
    {
        LazyFileRegistry::FileEntry file;
        std::shared_ptr<const PaddedPODArray<UInt64>> rows;
    };

    /// Filled by filterRangesAndFillRows: the files that contain surviving rows, in the order of
    /// their file index, with the sorted row indexes to read from each of them.
    std::vector<FileRows> rows_in_files;

    LazyFileRegistryPtr file_registry;

    explicit FileLazyMaterializingRows(LazyFileRegistryPtr file_registry_);

    void filterRangesAndFillRows(const PaddedPODArray<UInt64> & sorted_indexes) override;
};

using FileLazyMaterializingRowsPtr = std::shared_ptr<FileLazyMaterializingRows>;

/// The lazy branch of lazy materialization for the `File` storage (see optimizeLazyMaterialization2).
/// Reads the deferred columns for exactly the rows that survived the LIMIT, in the global row
/// index order: files in the order they were registered by the main branch, rows within a file
/// in ascending order (guaranteed by the Parquet reader with `preserve_order`).
class LazilyReadFromFile final : public ISourceStep
{
public:
    LazilyReadFromFile(
        SharedHeader header,
        std::shared_ptr<StorageFile> storage_,
        ReadFromFormatInfo info_,
        ContextPtr context_,
        size_t max_block_size_);

    void setLazyMaterializingRows(FileLazyMaterializingRowsPtr lazy_materializing_rows_);

    String getName() const override { return "LazilyReadFromFile"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

private:
    std::shared_ptr<StorageFile> storage;
    ReadFromFormatInfo info;
    ContextPtr context;
    size_t max_block_size;

    FileLazyMaterializingRowsPtr lazy_materializing_rows;
};

}
