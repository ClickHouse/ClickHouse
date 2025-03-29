#include "Read.h"

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

namespace DB::Parquet
{

Reader::Reader(ReadBuffer * reader, const ReadOptions & options_, SharedParsingThreadPoolPtr thread_pool)
    : options(options_), prefetcher(reader, options_, thread_pool)
{
}

void Reader::readFileMetaData()
{
    /// Parquet file ends with:
    ///  * serialized FileMetaData struct,
    ///  * [4 bytes] size of serialized FileMetaData struct,
    ///  * "PAR1" magic bytes.

    size_t file_size = prefetcher.getFileSize();
    if (file_size <= 8)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Parquet file too short: {} bytes", file_size);

    /// Read the last 64 KiB in hopes that FileMetaData is smaller than that.
    /// For files smaller than a few hundred MB this is usually enough.
    size_t initial_read_size = std::min(file_size, 64ul << 10);
    PODArray<char> buf(initial_read_size);
    prefetcher.readSync(buf.data(), initial_read_size, file_size - initial_read_size);

    if (memcmp(buf.data() + initial_read_size - 4, "PAR1", 4) != 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Not a parquet file (wrong magic bytes at the end of file)");

    int32_t metadata_size_i32;
    memcpy(&metadata_size_i32, buf.data() + initial_read_size - 8, 4);
    if (metadata_size_i32 <= 0 || size_t(metadata_size_i32) + 8 > file_size)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Bad metadata size in parquet file: {} bytes", metadata_size_i32);

    size_t metadata_size = size_t(metadata_size_i32);
    size_t buf_offset = 0;
    if (metadata_size + 8 > initial_read_size)
    {
        size_t remaining_bytes_to_read = metadata_size + 8 - initial_read_size;
        buf.resize(metadata_size);
        memmove(buf.data() + remaining_bytes_to_read, buf.data(), initial_read_size - 8);
        prefetcher.readSync(buf.data(), remaining_bytes_to_read, file_size - metadata_size - 8);
    }
    else
    {
        buf_offset = initial_read_size - 8 - metadata_size;
    }

    file_metadata = {};
    deserializeThriftStruct(file_metadata, buf.data() + buf_offset, metadata_size);

    volatile bool t = true;
    if (t)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "asdqwe there are {} row groups", file_metadata.row_groups.size());
}

}
