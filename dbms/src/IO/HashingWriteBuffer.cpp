#include <IO/HashingWriteBuffer.h>
#include <iomanip>


namespace DB
{

/// computation of the hash depends on the partitioning of blocks
/// so you need to compute a hash of n complete pieces and one incomplete
template <class Buffer>
void IHashingBuffer<Buffer>::calculateHash(DB::BufferBase::Position data, size_t len)
{
    if (len)
    {
        /// if the data is less than `block_size`, then put them into buffer and calculate hash later
        if (block_pos + len < block_size)
        {
            memcpy(&BufferWithOwnMemory<Buffer>::memory[block_pos], data, len);
            block_pos += len;
        }
        else
        {
            /// if something is already written to the buffer, then we'll add it
            if (block_pos)
            {
                size_t n = block_size - block_pos;
                memcpy(&BufferWithOwnMemory<Buffer>::memory[block_pos], data, n);
                append(&BufferWithOwnMemory<Buffer>::memory[0]);
                len -= n;
                data += n;
                block_pos = 0;
            }

            while (len >= block_size)
            {
                append(data);
                len -= block_size;
                data += block_size;
            }

            /// write the remainder to its buffer
            if (len)
            {
                memcpy(&BufferWithOwnMemory<Buffer>::memory[0], data, len);
                block_pos = len;
            }
        }
    }
}

template class IHashingBuffer<DB::ReadBuffer>;
template class IHashingBuffer<DB::WriteBuffer>;

}
