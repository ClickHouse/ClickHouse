#include <Compression/CheckingCompressedReadBuffer.h>

namespace DB
{

bool CheckingCompressedReadBuffer::nextImpl()
{
    size_t size_decompressed;
    size_t size_compressed_without_checksum;
    size_t size_compressed = readCompressedData(size_decompressed, size_compressed_without_checksum, true);

    if (!size_compressed)
        return false;

    /// own_compressed_buffer also includes getAdditionalSizeAtTheEndOfBuffer()
    /// which should not be accounted here, so size_compressed is used.
    ///
    /// And BufferBase is used over ReadBuffer, since former reset the working_buffer.
    BufferBase::set(own_compressed_buffer.data(), size_compressed, 0);

    return true;
}

}
