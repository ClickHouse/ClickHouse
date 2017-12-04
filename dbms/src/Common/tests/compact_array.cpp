/// Bug in GCC: https://gcc.gnu.org/bugzilla/show_bug.cgi?id=59124
#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Warray-bounds"
#endif

#include <Common/CompactArray.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>

#include <boost/filesystem.hpp>

#include <string>
#include <iostream>
#include <fstream>
#include <stdexcept>
#include <unistd.h>
#include <cstdlib>

namespace fs = boost::filesystem;

std::string createTmpPath(const std::string & filename)
{
    char pattern[] = "/tmp/fileXXXXXX";
    char * dir = mkdtemp(pattern);
    if (dir == nullptr)
        throw std::runtime_error("Could not create directory");

    return std::string(dir) + "/" + filename;
}

template <size_t width, size_t bucket_count, typename Generator>
struct Test
{
    static void perform()
    {
        bool ok = true;

        std::string filename;

        try
        {
            using Store = DB::CompactArray<UInt64, width, bucket_count>;

            Store store;

            for (size_t i = 0; i < bucket_count; ++i)
                store[i] = Generator::execute(i, width);

            filename = createTmpPath("compact_array.bin");

            {
                DB::WriteBufferFromFile wb(filename);
                wb.write(reinterpret_cast<const char *>(&store), sizeof(store));
            }

            {
                DB::ReadBufferFromFile rb(filename);
                typename Store::Reader reader(rb);
                while (reader.next())
                {
                    const auto & data = reader.get();
                    if (data.second != store[data.first])
                        throw std::runtime_error("Found discrepancy");
                }
            }
        }
        catch (const Poco::Exception & ex)
        {
            std::cout << "Test width=" << width << " bucket_count=" << bucket_count << " failed "
                << "(Error: " << ex.what() << ": " << ex.displayText() << ")\n";
            ok = false;
        }
        catch (const std::runtime_error & ex)
        {
            std::cout << "Test width=" << width << " bucket_count=" << bucket_count << " failed "
                << "(Error: " << ex.what() << ")\n";
            ok = false;
        }
        catch (...)
        {
            std::cout << "Test width=" << width << " bucket_count=" << bucket_count << " failed\n";
            ok = false;
        }

        fs::remove_all(fs::path(filename).parent_path().string());

        if (ok)
            std::cout << "Test width=" << width << " bucket_count=" << bucket_count << " passed\n";
    }
};


template <typename Generator>
struct TestSet
{
    static void execute()
    {
        Test<1, 1, Generator>::perform();
        Test<1, 2, Generator>::perform();
        Test<1, 3, Generator>::perform();
        Test<1, 4, Generator>::perform();
        Test<1, 5, Generator>::perform();
        Test<1, 6, Generator>::perform();
        Test<1, 7, Generator>::perform();
        Test<1, 8, Generator>::perform();
        Test<1, 9, Generator>::perform();
        Test<1, 10, Generator>::perform();
        Test<1, 16, Generator>::perform();
        Test<1, 32, Generator>::perform();
        Test<1, 64, Generator>::perform();
        Test<1, 128, Generator>::perform();
        Test<1, 256, Generator>::perform();
        Test<1, 512, Generator>::perform();
        Test<1, 1024, Generator>::perform();

        Test<2, 1, Generator>::perform();
        Test<2, 2, Generator>::perform();
        Test<2, 3, Generator>::perform();
        Test<2, 4, Generator>::perform();
        Test<2, 5, Generator>::perform();
        Test<2, 6, Generator>::perform();
        Test<2, 7, Generator>::perform();
        Test<2, 8, Generator>::perform();
        Test<2, 9, Generator>::perform();
        Test<2, 10, Generator>::perform();
        Test<2, 16, Generator>::perform();
        Test<2, 32, Generator>::perform();
        Test<2, 64, Generator>::perform();
        Test<2, 128, Generator>::perform();
        Test<2, 256, Generator>::perform();
        Test<2, 512, Generator>::perform();
        Test<2, 1024, Generator>::perform();

        Test<3, 1, Generator>::perform();
        Test<3, 2, Generator>::perform();
        Test<3, 3, Generator>::perform();
        Test<3, 4, Generator>::perform();
        Test<3, 5, Generator>::perform();
        Test<3, 6, Generator>::perform();
        Test<3, 7, Generator>::perform();
        Test<3, 8, Generator>::perform();
        Test<3, 9, Generator>::perform();
        Test<3, 10, Generator>::perform();
        Test<3, 16, Generator>::perform();
        Test<3, 32, Generator>::perform();
        Test<3, 64, Generator>::perform();
        Test<3, 128, Generator>::perform();
        Test<3, 256, Generator>::perform();
        Test<3, 512, Generator>::perform();
        Test<3, 1024, Generator>::perform();

        Test<4, 1, Generator>::perform();
        Test<4, 2, Generator>::perform();
        Test<4, 3, Generator>::perform();
        Test<4, 4, Generator>::perform();
        Test<4, 5, Generator>::perform();
        Test<4, 6, Generator>::perform();
        Test<4, 7, Generator>::perform();
        Test<4, 8, Generator>::perform();
        Test<4, 9, Generator>::perform();
        Test<4, 10, Generator>::perform();
        Test<4, 16, Generator>::perform();
        Test<4, 32, Generator>::perform();
        Test<4, 64, Generator>::perform();
        Test<4, 128, Generator>::perform();
        Test<4, 256, Generator>::perform();
        Test<4, 512, Generator>::perform();
        Test<4, 1024, Generator>::perform();

        Test<5, 1, Generator>::perform();
        Test<5, 2, Generator>::perform();
        Test<5, 3, Generator>::perform();
        Test<5, 4, Generator>::perform();
        Test<5, 5, Generator>::perform();
        Test<5, 6, Generator>::perform();
        Test<5, 7, Generator>::perform();
        Test<5, 8, Generator>::perform();
        Test<5, 9, Generator>::perform();
        Test<5, 10, Generator>::perform();
        Test<5, 16, Generator>::perform();
        Test<5, 32, Generator>::perform();
        Test<5, 64, Generator>::perform();
        Test<5, 128, Generator>::perform();
        Test<5, 256, Generator>::perform();
        Test<5, 512, Generator>::perform();
        Test<5, 1024, Generator>::perform();

        Test<6, 1, Generator>::perform();
        Test<6, 2, Generator>::perform();
        Test<6, 3, Generator>::perform();
        Test<6, 4, Generator>::perform();
        Test<6, 5, Generator>::perform();
        Test<6, 6, Generator>::perform();
        Test<6, 7, Generator>::perform();
        Test<6, 8, Generator>::perform();
        Test<6, 9, Generator>::perform();
        Test<6, 10, Generator>::perform();
        Test<6, 16, Generator>::perform();
        Test<6, 32, Generator>::perform();
        Test<6, 64, Generator>::perform();
        Test<6, 128, Generator>::perform();
        Test<6, 256, Generator>::perform();
        Test<6, 512, Generator>::perform();
        Test<6, 1024, Generator>::perform();

        Test<7, 1, Generator>::perform();
        Test<7, 2, Generator>::perform();
        Test<7, 3, Generator>::perform();
        Test<7, 4, Generator>::perform();
        Test<7, 5, Generator>::perform();
        Test<7, 6, Generator>::perform();
        Test<7, 7, Generator>::perform();
        Test<7, 8, Generator>::perform();
        Test<7, 9, Generator>::perform();
        Test<7, 10, Generator>::perform();
        Test<7, 16, Generator>::perform();
        Test<7, 32, Generator>::perform();
        Test<7, 64, Generator>::perform();
        Test<7, 128, Generator>::perform();
        Test<7, 256, Generator>::perform();
        Test<7, 512, Generator>::perform();
        Test<7, 1024, Generator>::perform();
    }
};

struct Generator1
{
    static UInt8 execute(size_t, size_t width)
    {
        return (1 << width) - 1;
    }
};

struct Generator2
{
    static UInt8 execute(size_t i, size_t width)
    {
        return (i >> 1) & ((1 << width) - 1);
    }
};

struct Generator3
{
    static UInt8 execute(size_t i, size_t width)
    {
        return (i * 17 + 31) % (1ULL << width);
    }
};

void runTests()
{
    std::cout << "Test set 1\n";
    TestSet<Generator1>::execute();
    std::cout << "Test set 2\n";
    TestSet<Generator2>::execute();
    std::cout << "Test set 3\n";
    TestSet<Generator3>::execute();
}

int main()
{
    runTests();
    return 0;
}

#if !__clang__
#pragma GCC diagnostic pop
#endif
