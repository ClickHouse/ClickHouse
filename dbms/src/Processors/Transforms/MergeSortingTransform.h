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

class MergeSortingTransform : public IAccumulatingTransform
{
public:
    /// limit - if not 0, allowed to return just first 'limit' rows in sorted order.
    MergeSortingTransform(const Block & header,
                          SortDescription & description_,
                          size_t max_merged_block_size_, UInt64 limit_,
                          size_t max_bytes_before_remerge_,
                          size_t max_bytes_before_external_sort_, const std::string & tmp_path_);

    String getName() const override { return "MergeSortingTransform"; }

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    SortDescription description;
    size_t max_merged_block_size;
    UInt64 limit;

    size_t max_bytes_before_remerge;
    size_t max_bytes_before_external_sort;
    const std::string tmp_path;

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

    /// For reading data from temporary file.
    struct TemporaryFileStream
    {
        ReadBufferFromFile file_in;
        CompressedReadBuffer compressed_in;
        BlockInputStreamPtr block_in;

        TemporaryFileStream(const std::string & path, const Block & header)
                : file_in(path), compressed_in(file_in), block_in(std::make_shared<NativeBlockInputStream>(compressed_in, header, 0)) {}
    };

    std::vector<std::unique_ptr<TemporaryFileStream>> temporary_inputs;

    BlockInputStreams inputs_to_merge;

    /// Merge all accumulated blocks to keep no more than limit rows.
    void remerge();

    void removeConstColumns(Chunk & chunk);
};

}
