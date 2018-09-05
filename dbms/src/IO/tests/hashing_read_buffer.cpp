#include <IO/ReadBufferFromIStream.h>
#include <IO/HashingReadBuffer.h>
#include <IO/WriteBufferFromOStream.h>
#include "hashing_buffer.h"
#include <iostream>

void test(size_t data_size)
{
    std::vector<char> vec(data_size);
    char * data = vec.data();

    for (size_t i = 0; i < data_size; ++i)
        data[i] = rand() & 255;

    CityHash_v1_0_2::uint128 reference = referenceHash(data, data_size);

    std::vector<size_t> block_sizes = {56, 128, 513, 2048, 3055, 4097, 4096};
    for (size_t read_buffer_block_size : block_sizes)
    {
        std::cout  << "block size " << read_buffer_block_size << std::endl;
        std::stringstream io;
        DB::WriteBufferFromOStream out_(io);
        DB::HashingWriteBuffer out(out_);
        out.write(data, data_size);
        out.next();

        DB::ReadBufferFromIStream source(io, read_buffer_block_size);
        DB::HashingReadBuffer buf(source);

        std::vector<char> read_buf(data_size);
        buf.read(read_buf.data(), data_size);

        bool failed_to_read = false;
        for (size_t i = 0; i < data_size; ++i)
            if (read_buf[i] != vec[i])
            {
                failed_to_read = true;
            }

        if (failed_to_read)
        {
            std::cout.write(data, data_size);
            std::cout << std::endl;
            std::cout.write(read_buf.data(), data_size);
            std::cout << std::endl;
            FAIL("Fail to read data");
        }

        if (buf.getHash() != reference)
        {
            FAIL("failed on data size " << data_size << " reading by blocks of size " << read_buffer_block_size);
        }
        if (buf.getHash() != out.getHash())
            FAIL("Hash of HashingReadBuffer doesn't match with hash of HashingWriteBuffer on data size " << data_size << " reading by blocks of size " << read_buffer_block_size);
    }
}

int main()
{
    test(5);
    test(100);
    test(2048);
    test(2049);
    test(100000);
    test(1 << 17);

    return 0;
}
