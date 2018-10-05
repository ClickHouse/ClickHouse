#include <iostream>
#include <iomanip>

#include <city.h>
#include <openssl/md5.h>

#include <Common/Stopwatch.h>

#include <Common/SipHash.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>


int main(int, char **)
{
    using Strings = std::vector<std::string>;
    using Hashes = std::vector<char>;
    Strings strings;
    size_t rows = 0;
    size_t bytes = 0;

    {
        Stopwatch watch;

        DB::ReadBufferFromFileDescriptor in(STDIN_FILENO);

        while (!in.eof())
        {
            strings.push_back(std::string());
            DB::readEscapedString(strings.back(), in);
            DB::assertChar('\n', in);
            bytes += strings.back().size() + 1;
        }

        watch.stop();
        rows = strings.size();
        std::cerr << std::fixed << std::setprecision(2)
            << "Read " << rows << " rows, " << bytes / 1000000.0 << " MB"
            << ", elapsed: " << watch.elapsedSeconds()
            << " (" << rows / watch.elapsedSeconds() << " rows/sec., " << bytes / 1000000.0 / watch.elapsedSeconds() << " MB/sec.)"
            << std::endl;
    }

    Hashes hashes(16 * rows);

    {
        Stopwatch watch;

        for (size_t i = 0; i < rows; ++i)
        {
            *reinterpret_cast<UInt64*>(&hashes[i * 16]) = CityHash_v1_0_2::CityHash64(strings[i].data(), strings[i].size());
        }

        watch.stop();

        UInt64 check = CityHash_v1_0_2::CityHash64(hashes.data(), hashes.size());

        std::cerr << std::fixed << std::setprecision(2)
            << "CityHash64 (check = " << check << ")"
            << ", elapsed: " << watch.elapsedSeconds()
            << " (" << rows / watch.elapsedSeconds() << " rows/sec., " << bytes / 1000000.0 / watch.elapsedSeconds() << " MB/sec.)"
            << std::endl;
    }

/*    {
        Stopwatch watch;

        std::vector<char> seed(16);

        for (size_t i = 0; i < rows; ++i)
        {
            sipHash(
                reinterpret_cast<unsigned char *>(&hashes[i * 16]),
                reinterpret_cast<const unsigned char *>(strings[i].data()),
                strings[i].size(),
                reinterpret_cast<const unsigned char *>(seed.data()));
        }

        watch.stop();

        UInt64 check = CityHash_v1_0_2::CityHash64(hashes.data(), hashes.size());

        std::cerr << std::fixed << std::setprecision(2)
            << "SipHash (check = " << check << ")"
            << ", elapsed: " << watch.elapsedSeconds()
            << " (" << rows / watch.elapsedSeconds() << " rows/sec., " << bytes / 1000000.0 / watch.elapsedSeconds() << " MB/sec.)"
            << std::endl;
    }*/

    {
        Stopwatch watch;

        for (size_t i = 0; i < rows; ++i)
        {
            SipHash hash;
            hash.update(strings[i].data(), strings[i].size());
            hash.get128(&hashes[i * 16]);
        }

        watch.stop();

        UInt64 check = CityHash_v1_0_2::CityHash64(hashes.data(), hashes.size());

        std::cerr << std::fixed << std::setprecision(2)
            << "SipHash, stream (check = " << check << ")"
            << ", elapsed: " << watch.elapsedSeconds()
            << " (" << rows / watch.elapsedSeconds() << " rows/sec., " << bytes / 1000000.0 / watch.elapsedSeconds() << " MB/sec.)"
            << std::endl;
    }

    {
        Stopwatch watch;

        for (size_t i = 0; i < rows; ++i)
        {
            MD5_CTX state;
            MD5_Init(&state);
            MD5_Update(&state, reinterpret_cast<const unsigned char *>(strings[i].data()), strings[i].size());
            MD5_Final(reinterpret_cast<unsigned char *>(&hashes[i * 16]), &state);
        }

        watch.stop();

        UInt64 check = CityHash_v1_0_2::CityHash64(hashes.data(), hashes.size());

        std::cerr << std::fixed << std::setprecision(2)
            << "MD5 (check = " << check << ")"
            << ", elapsed: " << watch.elapsedSeconds()
            << " (" << rows / watch.elapsedSeconds() << " rows/sec., " << bytes / 1000000.0 / watch.elapsedSeconds() << " MB/sec.)"
            << std::endl;
    }

    return 0;
}
