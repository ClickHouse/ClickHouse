#include <IO/ReadBufferFromMemory.h>
#include <Common/PODArray.h>
#include <Processors/Formats/Impl/Parquet/generated/parquet_types.h>
#include <parquet/properties.h>
#include <parquet/column_page.h>


namespace DB
{


class LazyPageReader
{
public:
    LazyPageReader(
        const std::shared_ptr<ReadBufferFromMemory> & stream_, const parquet::ReaderProperties & properties_, size_t total_num_values_, parquet::Compression::type codec)
        : stream(stream_), properties(properties_), total_num_values(total_num_values_)
    {
        decompressor = parquet::GetCodec(codec);
    }
    bool hasNext();
    const parquet::format::PageHeader& peekNextPageHeader();
    std::shared_ptr<parquet::Page> nextPage();
    void skipNextPage();

private:
    std::shared_ptr<arrow::Buffer> decompressIfNeeded(const uint8_t * data, size_t compressed_size, size_t uncompressed_size);

    std::shared_ptr<ReadBufferFromMemory> stream;
    parquet::format::PageHeader current_page_header;
    std::shared_ptr<parquet::Page> current_page;
    const parquet::ReaderProperties properties;

    std::shared_ptr<arrow::util::Codec> decompressor;
    PaddedPODArray<UInt8> decompression_buffer;

    static const size_t max_page_header_size = 16 * 1024 * 1024;
    static const size_t DEFAULT_PAGE_HEADER_SIZE = 16 * 1024;

    size_t seen_num_values = 0;
    size_t total_num_values = 0;
};
}

