#include <IO/HashingWriteBuffer.h>
#include <IO/WriteBufferFromFile.h>
#include <pcg_random.hpp>

#include "hashing_buffer.h"

static void test(size_t data_size)
{
    pcg64 rng;

    std::vector<char> vec(data_size);
    char * data = vec.data();

    for (size_t i = 0; i < data_size; ++i)
        data[i] = rng() & 255;

    CityHash_v1_0_2::uint128 reference = referenceHash(data, data_size);

    DB::WriteBufferFromFile sink("/dev/null", 1 << 16);

    {
        DB::HashingWriteBuffer buf(sink);

        for (size_t pos = 0; pos < data_size;)
        {
            size_t len = std::min(static_cast<size_t>(rng() % 10000 + 1), data_size - pos);
            buf.write(data + pos, len);
            buf.next();
            pos += len;
        }

        if (buf.getHash() != reference)
            FAIL("failed on data size " << data_size << " writing rngom chunks of up to 10000 bytes");
    }

    {
        DB::HashingWriteBuffer buf(sink);

        for (size_t pos = 0; pos < data_size;)
        {
            size_t len = std::min(static_cast<size_t>(rng() % 5 + 1), data_size - pos);
            buf.write(data + pos, len);
            buf.next();
            pos += len;
        }

        if (buf.getHash() != reference)
            FAIL("failed on data size " << data_size << " writing rngom chunks of up to 5 bytes");
    }

    {
        DB::HashingWriteBuffer buf(sink);

        for (size_t pos = 0; pos < data_size;)
        {
            size_t len = std::min(static_cast<size_t>(2048 + rng() % 3 - 1), data_size - pos);
            buf.write(data + pos, len);
            buf.next();
            pos += len;
        }

        if (buf.getHash() != reference)
            FAIL("failed on data size " << data_size << " writing rngom chunks of 2048 +-1 bytes");
    }

    {
        DB::HashingWriteBuffer buf(sink);

        buf.write(data, data_size);

        if (buf.getHash() != reference)
            FAIL("failed on data size " << data_size << " writing all at once");
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
