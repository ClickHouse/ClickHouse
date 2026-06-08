#pragma once

#include <algorithm>
#include <cstddef>


namespace DB
{

/** Splits a positioned read of `size` bytes starting at `offset` into chunks no larger than
  * `max_chunk` bytes and reads them one by one.
  *
  * For each chunk it invokes `read_chunk(char * chunk_buffer, size_t chunk_size, size_t chunk_offset)`,
  * which performs the actual read and returns the number of bytes it managed to read. A chunk that
  * returns fewer bytes than requested ends the loop (end of file / short read). The total number of
  * bytes read is returned.
  *
  * The purpose of the cap is to keep every individual read within the `hdfsPread` 32-bit `tSize`
  * size argument, which would otherwise overflow for requests above `INT_MAX` (~2 GiB). Keeping the
  * splitting logic isolated and free of HDFS dependencies makes it unit-testable with an injectable
  * cap: a small `max_chunk` exercises the chunking path that a single >2 GiB read would otherwise
  * require.
  */
template <typename ReadChunk>
size_t chunkedPread(char * buffer, size_t size, size_t offset, size_t max_chunk, ReadChunk && read_chunk)
{
    size_t total_read = 0;

    while (total_read < size)
    {
        const size_t current_read_size = std::min(size - total_read, max_chunk);
        const size_t bytes_read = read_chunk(buffer + total_read, current_read_size, offset + total_read);

        total_read += bytes_read;
        if (bytes_read < current_read_size)
            break;
    }

    return total_read;
}

}
