#pragma once

#include <IO/ReadBufferFromMemory.h>
#include <Common/PODArray.h>
#include <generated/parquet_types.h>
#include <parquet/properties.h>
#include <parquet/column_page.h>

namespace DB
{

class LazyPageReader
{
public:
    LazyPageReader(
        std::unique_ptr<ReadBufferFromMemory> stream_, const parquet::ReaderProperties & properties_, parquet::Compression::type codec, size_t offsets_)
        : stream(std::move(stream_)), properties(properties_), offset_in_file(offsets_)
    {
        decompressor = parquet::GetCodec(codec);
    }
    void seekFileOffset(size_t offset);
    bool hasNext();
    const parquet::format::PageHeader& peekNextPageHeader();
    std::shared_ptr<parquet::Page> nextPage();
    void skipNextPage();
    size_t getOffsetInFile() const;

private:
    std::shared_ptr<arrow::Buffer> decompressIfNeeded(const uint8_t * data, size_t compressed_size, size_t uncompressed_size, size_t levels_byte_len = 0);

    std::unique_ptr<ReadBufferFromMemory> stream = nullptr;
    parquet::format::PageHeader current_page_header;
    std::shared_ptr<parquet::Page> current_page;
    const parquet::ReaderProperties properties;

    std::shared_ptr<arrow::util::Codec> decompressor;
    PaddedPODArray<UInt8> decompression_buffer;

    static const size_t max_page_header_size = 16 * 1024 * 1024;
    static const size_t DEFAULT_PAGE_HEADER_SIZE = 16 * 1024;

    const size_t offset_in_file = 0;
};
}

