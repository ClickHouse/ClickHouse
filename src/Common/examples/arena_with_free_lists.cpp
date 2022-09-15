/** Run this (example)
  * ./arena_with_free_lists 5000000 < ../../Server/data/test/hits/20140317_20140323_2_178_4/Title.bin
  */

#define USE_BAD_ARENA 0

#if !USE_BAD_ARENA
    #include <Common/ArenaWithFreeLists.h>
#endif

#include <variant>
#include <memory>
#include <array>
#include <sys/resource.h>
#include <base/bit_cast.h>

#include <base/StringRef.h>
#include <base/arraySize.h>
#include <Common/Arena.h>
#include <Core/Field.h>
#include <Common/Stopwatch.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <Compression/CompressedReadBuffer.h>
#include <IO/ReadHelpers.h>

using namespace DB;

namespace DB
{
    namespace ErrorCodes
    {
        extern const int SYSTEM_ERROR;
    }
}


/// Implementation of ArenaWithFreeLists, which contains a bug. Used to reproduce the bug.
#if USE_BAD_ARENA

class ArenaWithFreeLists : private Allocator<false>
{
private:
    struct Block { Block * next; };

    static const std::array<size_t, 14> & getSizes()
    {
        static constexpr std::array<size_t, 14> sizes{
            8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536
        };

        static_assert(sizes.front() >= sizeof(Block), "Can't make allocations smaller than sizeof(Block)");

        return sizes;
    }

    static auto sizeToPreviousPowerOfTwo(const int size)
    {
        return _bit_scan_reverse(size - 1);
        /// The bug is located in the line above. If you change to the next line, then the bug is fixed.
        //return size <= 1 ? 0 : _bit_scan_reverse(size - 1);
    }

    static auto getMinBucketNum()
    {
        static const auto val = sizeToPreviousPowerOfTwo(getSizes().front());
        return val;
    }
    static auto getMaxFixedBlockSize() { return getSizes().back(); }

    Arena pool;
    const std::unique_ptr<Block * []> free_lists = std::make_unique<Block * []>(arraySize(getSizes()));

    static size_t findFreeListIndex(const size_t size)
    {
        /// shift powers of two into previous bucket by subtracting 1
        const auto bucket_num = sizeToPreviousPowerOfTwo(size);

        return std::max(bucket_num, getMinBucketNum()) - getMinBucketNum();
    }

public:
    ArenaWithFreeLists(
        const size_t initial_size = 4096, const size_t growth_factor = 2,
        const size_t linear_growth_threshold = 128 * 1024 * 1024)
        : pool{initial_size, growth_factor, linear_growth_threshold}
    {
    }

    char * alloc(const size_t size)
    {
        if (size > getMaxFixedBlockSize())
            return static_cast<char *>(Allocator::alloc(size));

        /// find list of required size
        const auto list_idx = findFreeListIndex(size);

        if (auto & block = free_lists[list_idx])
        {
            const auto res = bit_cast<char *>(block);
            block = block->next;
            return res;
        }

        /// no block of corresponding size, allocate a new one
        return pool.alloc(getSizes()[list_idx]);
    }

    void free(const void * ptr, const size_t size)
    {
        if (size > getMaxFixedBlockSize())
            return Allocator::free(const_cast<void *>(ptr), size);

        /// find list of required size
        const auto list_idx = findFreeListIndex(size);

        auto & block = free_lists[list_idx];
        const auto old = block;
        block = bit_cast<Block *>(ptr);
        block->next = old;
    }

    /// Size of the allocated pool in bytes
    size_t size() const
    {
        return pool.size();
    }
};

#endif


/// A small piece copied from the CacheDictionary. It is used only to demonstrate the problem.
struct Dictionary
{
    template <typename Value> using ContainerType = Value[];
    template <typename Value> using ContainerPtrType = std::unique_ptr<ContainerType<Value>>;

    enum class AttributeUnderlyingTypeTest
    {
        UInt8,
        UInt16,
        UInt32,
        UInt64,
        Int8,
        Int16,
        Int32,
        Int64,
        Float32,
        Float64,
        String
    };

    struct Attribute final
    {
        AttributeUnderlyingTypeTest type;
        std::variant<
            UInt8, UInt16, UInt32, UInt64,
            Int8, Int16, Int32, Int64,
            Float32, Float64,
            String> null_values;
        std::variant<
            ContainerPtrType<UInt8>, ContainerPtrType<UInt16>, ContainerPtrType<UInt32>, ContainerPtrType<UInt64>,
            ContainerPtrType<Int8>, ContainerPtrType<Int16>, ContainerPtrType<Int32>, ContainerPtrType<Int64>,
            ContainerPtrType<Float32>, ContainerPtrType<Float64>,
            ContainerPtrType<StringRef>> arrays;
    };

    std::unique_ptr<ArenaWithFreeLists> string_arena;

    /// This function is compiled into exactly the same machine code as in production, when there was a bug.
    void NO_INLINE setAttributeValue(Attribute & attribute, const UInt64 idx, const Field & value) const
    {
        switch (attribute.type)
        {
            case AttributeUnderlyingTypeTest::UInt8: std::get<ContainerPtrType<UInt8>>(attribute.arrays)[idx] = value.get<UInt64>(); break;
            case AttributeUnderlyingTypeTest::UInt16: std::get<ContainerPtrType<UInt16>>(attribute.arrays)[idx] = value.get<UInt64>(); break;
            case AttributeUnderlyingTypeTest::UInt32: std::get<ContainerPtrType<UInt32>>(attribute.arrays)[idx] = value.get<UInt64>(); break;
            case AttributeUnderlyingTypeTest::UInt64: std::get<ContainerPtrType<UInt64>>(attribute.arrays)[idx] = value.get<UInt64>(); break;
            case AttributeUnderlyingTypeTest::Int8: std::get<ContainerPtrType<Int8>>(attribute.arrays)[idx] = value.get<Int64>(); break;
            case AttributeUnderlyingTypeTest::Int16: std::get<ContainerPtrType<Int16>>(attribute.arrays)[idx] = value.get<Int64>(); break;
            case AttributeUnderlyingTypeTest::Int32: std::get<ContainerPtrType<Int32>>(attribute.arrays)[idx] = value.get<Int64>(); break;
            case AttributeUnderlyingTypeTest::Int64: std::get<ContainerPtrType<Int64>>(attribute.arrays)[idx] = value.get<Int64>(); break;
            case AttributeUnderlyingTypeTest::Float32: std::get<ContainerPtrType<Float32>>(attribute.arrays)[idx] = value.get<Float64>(); break;
            case AttributeUnderlyingTypeTest::Float64: std::get<ContainerPtrType<Float64>>(attribute.arrays)[idx] = value.get<Float64>(); break;
            case AttributeUnderlyingTypeTest::String:
            {
                const auto & string = value.get<String>();
                auto & string_ref = std::get<ContainerPtrType<StringRef>>(attribute.arrays)[idx];
                const auto & null_value_ref = std::get<String>(attribute.null_values);

                /// free memory unless it points to a null_value
                if (string_ref.data && string_ref.data != null_value_ref.data())
                    string_arena->free(const_cast<char *>(string_ref.data), string_ref.size);

                const auto size = string.size();
                if (size != 0)
                {
                    auto * string_ptr = string_arena->alloc(size + 1);
                    std::copy(string.data(), string.data() + size + 1, string_ptr);
                    string_ref = StringRef{string_ptr, size};
                }
                else
                    string_ref = {};

                break;
            }
        }
    }
};


int main(int argc, char ** argv)
{
    if (argc < 2)
    {
        std::cerr << "Usage: program n\n";
        return 1;
    }

    std::cerr << std::fixed << std::setprecision(2);

    size_t n = parse<size_t>(argv[1]);
    std::vector<std::string> data;
    size_t sum_strings_size = 0;

    {
        Stopwatch watch;
        DB::ReadBufferFromFileDescriptor in1(STDIN_FILENO);
        DB::CompressedReadBuffer in2(in1);

        for (size_t i = 0; i < n && !in2.eof(); ++i)
        {
            data.emplace_back();
            readStringBinary(data.back(), in2);
            sum_strings_size += data.back().size() + 1;
        }

        watch.stop();
        std::cerr
            << "Read. Elements: " << data.size() << ", bytes: " << sum_strings_size
            << ", elapsed: " << watch.elapsedSeconds()
            << " (" << data.size() / watch.elapsedSeconds() << " elem/sec.,"
            << " " << sum_strings_size / 1048576.0 / watch.elapsedSeconds() << " MiB/sec.)"
            << std::endl;

        rusage resource_usage;
        if (0 != getrusage(RUSAGE_SELF, &resource_usage))
            throwFromErrno("Cannot getrusage", ErrorCodes::SYSTEM_ERROR);

        size_t allocated_bytes = resource_usage.ru_maxrss * 1024;
        std::cerr << "Current memory usage: " << allocated_bytes << " bytes.\n";
    }

    ArenaWithFreeLists arena;
    std::vector<StringRef> refs;
    refs.reserve(data.size());

    {
        Stopwatch watch;

        for (const auto & s : data)
        {
            auto * ptr = arena.alloc(s.size() + 1);
            memcpy(ptr, s.data(), s.size() + 1);
            refs.emplace_back(ptr, s.size() + 1);
        }

        watch.stop();
        std::cerr
            << "Insert info arena. Bytes: " << arena.size()
            << ", elapsed: " << watch.elapsedSeconds()
            << " (" << data.size() / watch.elapsedSeconds() << " elem/sec.,"
            << " " << sum_strings_size / 1048576.0 / watch.elapsedSeconds() << " MiB/sec.)"
            << std::endl;
    }

    //while (true)
    {
        Stopwatch watch;

        size_t bytes = 0;
        for (size_t i = 0, size = data.size(); i < size; ++i)
        {
            size_t index_from = lrand48() % size;
            size_t index_to = lrand48() % size;

            arena.free(const_cast<char *>(refs[index_to].data), refs[index_to].size);
            const auto & s = data[index_from];
            auto * ptr = arena.alloc(s.size() + 1);
            memcpy(ptr, s.data(), s.size() + 1);
            bytes += s.size() + 1;

            refs[index_to] = {ptr, s.size() + 1};
        }

        watch.stop();
        std::cerr
            << "Randomly remove and insert elements. Bytes: " << arena.size()
            << ", elapsed: " << watch.elapsedSeconds()
            << " (" << data.size() / watch.elapsedSeconds() << " elem/sec.,"
            << " " << bytes / 1048576.0 / watch.elapsedSeconds() << " MiB/sec.)"
            << std::endl;
    }

    Dictionary dictionary;
    dictionary.string_arena = std::make_unique<ArenaWithFreeLists>();

    constexpr size_t cache_size = 1024;

    Dictionary::Attribute attr;
    attr.type = Dictionary::AttributeUnderlyingTypeTest::String;
    std::get<Dictionary::ContainerPtrType<StringRef>>(attr.arrays).reset(new StringRef[cache_size]{});  // NOLINT

    while (true)
    {
        Stopwatch watch;

        size_t bytes = 0;
        for (size_t i = 0, size = data.size(); i < size; ++i)
        {
            size_t index_from = lrand48() % size;
            size_t index_to = lrand48() % cache_size;

            dictionary.setAttributeValue(attr, index_to, data[index_from]);

            bytes += data[index_from].size() + 1;
        }

        watch.stop();
        std::cerr
            << "Filling cache. Bytes: " << arena.size()
            << ", elapsed: " << watch.elapsedSeconds()
            << " (" << data.size() / watch.elapsedSeconds() << " elem/sec.,"
            << " " << bytes / 1048576.0 / watch.elapsedSeconds() << " MiB/sec.)"
            << std::endl;
    }
}
