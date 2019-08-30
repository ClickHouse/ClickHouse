#pragma once
#include <Processors/IAccumulatingTransform.h>
#include <Core/SortDescription.h>
#include <Poco/TemporaryFile.h>
#include <IO/ReadBufferFromFile.h>
#include <Compression/CompressedReadBuffer.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/NativeBlockInputStream.h>

#include <common/logger_useful.h>

#include <queue>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_ENOUGH_SPACE;
}
class MergeSorter;

class MergeSortingTransform : public IProcessor
{
public:
    /// limit - if not 0, allowed to return just first 'limit' rows in sorted order.
    MergeSortingTransform(const Block & header,
                          SortDescription & description_,
                          size_t max_merged_block_size_, UInt64 limit_,
                          size_t max_bytes_before_remerge_,
                          size_t max_bytes_before_external_sort_, const std::string & tmp_path_,
                          size_t min_free_disk_space_);

    ~MergeSortingTransform() override;

    String getName() const override { return "MergeSortingTransform"; }

protected:

    Status prepare() override;
    void work() override;
    Processors expandPipeline() override;

private:
    SortDescription description;
    size_t max_merged_block_size;
    UInt64 limit;

    size_t max_bytes_before_remerge;
    size_t max_bytes_before_external_sort;
    const std::string tmp_path;
    size_t min_free_disk_space;

    Logger * log = &Logger::get("MergeSortingBlockInputStream");

    Chunks chunks;
    size_t sum_rows_in_blocks = 0;
    size_t sum_bytes_in_blocks = 0;

    /// If remerge doesn't save memory at least several times, mark it as useless and don't do it anymore.
    bool remerge_is_useful = true;

    /// Before operation, will remove constant columns from blocks. And after, place constant columns back.
    /// (to avoid excessive virtual function calls and because constants cannot be serialized in Native format for temporary files)
    /// Save original block structure here.
    Block header_without_constants;
    /// Columns which were constant in header and we need to remove from chunks.
    std::vector<bool> const_columns_to_remove;

    /// Everything below is for external sorting.
    std::vector<std::unique_ptr<Poco::TemporaryFile>> temporary_files;

    /// Merge all accumulated blocks to keep no more than limit rows.
    void remerge();

    void removeConstColumns(Chunk & chunk);
    void enrichChunkWithConstants(Chunk & chunk);

    enum class Stage
    {
        Consume = 0,
        Generate,
        Serialize,
    };

    Stage stage = Stage::Consume;
    bool generated_prefix = false;
    Chunk current_chunk;
    Chunk generated_chunk;

    std::unique_ptr<MergeSorter> merge_sorter;
    ProcessorPtr external_merging_sorted;
    Processors processors;

    Status prepareConsume();
    Status prepareSerialize();
    Status prepareGenerate();

    void consume(Chunk chunk);
    void serialize();
    void generate();
};

}
