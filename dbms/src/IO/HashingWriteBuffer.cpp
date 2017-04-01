#include <IO/HashingWriteBuffer.h>
#include <iomanip>
#include <city.h>

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

/// UInt64 is 39 characters in 10th number system
static const size_t UINT64_DECIMAL_SIZE = 39;
std::string uint128ToString(uint128 data)
{
    std::stringstream ss;
    ss << std::setw(UINT64_DECIMAL_SIZE) << std::setfill('0') << data.first << std::setw(UINT64_DECIMAL_SIZE) << std::setfill('0') << data.second;
    return ss.str();
}

std::ostream & operator<<(std::ostream & os, const uint128 & data)
{
    os << uint128ToString(data);
    return os;
}

std::istream & operator>>(std::istream & is, uint128 & data)
{
    std::vector<char> buffer(UINT64_DECIMAL_SIZE);
    is.read(buffer.data(), UINT64_DECIMAL_SIZE);
    data.first = DB::parse<UInt64>(buffer.data(), UINT64_DECIMAL_SIZE);

    if (!is)
        throw DB::Exception(std::string("Fail to parse uint128 from ") + buffer.data());

    is.read(buffer.data(), UINT64_DECIMAL_SIZE);
    data.first = DB::parse<UInt64>(buffer.data(), UINT64_DECIMAL_SIZE);

    if (!is)
        throw DB::Exception(std::string("Fail to parse uint128 from ") + buffer.data());

    return is;
}
