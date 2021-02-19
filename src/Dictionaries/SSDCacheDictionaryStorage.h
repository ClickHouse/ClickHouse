#pragma once

#include <chrono>

#include <pcg_random.hpp>
#include <filesystem>
#include <city.h>
#include <fcntl.h>

#include <common/unaligned.h>
#include <Common/randomSeed.h>
#include <Common/Arena.h>
#include <Common/ArenaWithFreeLists.h>
#include <Common/MemorySanitizer.h>
#include <Common/HashTable/LRUHashMap.h>
#include <IO/AIO.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/ICacheDictionaryStorage.h>
#include <Dictionaries/DictionaryHelpers.h>


namespace ProfileEvents
{
    extern const Event FileOpen;
    extern const Event WriteBufferAIOWrite;
    extern const Event WriteBufferAIOWriteBytes;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int AIO_READ_ERROR;
    extern const int AIO_WRITE_ERROR;
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int CANNOT_CREATE_DIRECTORY;
    extern const int CANNOT_FSYNC;
    extern const int CANNOT_IO_GETEVENTS;
    extern const int CANNOT_IO_SUBMIT;
    extern const int CANNOT_OPEN_FILE;
    extern const int CORRUPTED_DATA;
    extern const int FILE_DOESNT_EXIST;
    extern const int UNSUPPORTED_METHOD;
    extern const int NOT_IMPLEMENTED;
}

struct SSDCacheDictionaryStorageConfiguration
{
    const size_t strict_max_lifetime_seconds;
    const DictionaryLifetime lifetime;

    const std::string file_path;
    const size_t max_partitions_count;
    const size_t max_stored_keys;
    const size_t block_size;
    const size_t file_blocks_size;
    const size_t read_buffer_blocks_size;
    const size_t write_buffer_blocks_size;
};


/** Simple Key is serialized in block with following structure
    key     | data_size | data
    8 bytes | 8 bytes   | data_size bytes

    Complex Key is serialized in block with following structure
    key_size     | key_data       | data_size | data
    8 bytes      | key_size bytes | 8 bytes   | data_size bytes
*/
template <typename TKeyType>
struct SSDCacheKey final
{
    using KeyType = TKeyType;

    SSDCacheKey(KeyType key_, size_t size_, const char * data_)
        : key(key_)
        , size(size_)
        , data(data_)
    {}

    KeyType key;
    size_t size;
    const char * data;
};

using SSDCacheSimpleKey = SSDCacheKey<UInt64>;
using SSDCacheComplexKey = SSDCacheKey<StringRef>;

/** Block is serialized with following structure
    check_sum | keys_size | [keys]
    8 bytes   | 8 bytes   |
*/
class SSDCacheBlock final
{
    static constexpr size_t block_header_check_sum_size = sizeof(size_t);
    static constexpr size_t block_header_keys_size = sizeof(size_t);
public:

    /// Block header size
    static constexpr size_t block_header_size = block_header_check_sum_size + block_header_keys_size;

    explicit SSDCacheBlock(size_t block_size_)
        : block_size(block_size_)
    {}

    /// Checks if size can be written in empty block with block_size
    static bool canBeWrittenInEmptyBlock(SSDCacheSimpleKey & simple_key, size_t block_size)
    {
        static constexpr size_t simple_key_size = sizeof(simple_key.key);

        return (block_header_size + simple_key_size + sizeof(simple_key.size) + simple_key.size) <= block_size;
    }

    static bool canBeWrittenInEmptyBlock(SSDCacheComplexKey & complex_key, size_t block_size)
    {
        StringRef & key = complex_key.key;
        size_t complex_key_size = sizeof(key.size) + key.size;

        return (block_header_size + complex_key_size + sizeof(complex_key.size) + complex_key.size) <= block_size;
    }

    /// Reset block with new block_data
    /// block_data must be filled with zeroes if it is new block
    void reset(char * new_block_data)
    {
        block_data = new_block_data;
        current_block_offset = block_header_size;
        keys_size = unalignedLoad<size_t>(new_block_data + block_header_check_sum_size);
    }

    /// Check if it is enough place to write key in block
    bool enoughtPlaceToWriteKey(const SSDCacheSimpleKey & cache_key) const
    {
        return (current_block_offset + (sizeof(cache_key.key) + sizeof(cache_key.size) + cache_key.size)) <= block_size;
    }

    bool enoughtPlaceToWriteKey(const SSDCacheComplexKey & cache_key) const
    {
        const StringRef & key = cache_key.key;
        size_t complex_key_size = sizeof(key.size) + key.size;

        return (current_block_offset + (complex_key_size + sizeof(cache_key.size) + cache_key.size)) <= block_size;
    }

    /// Write key and returns offset in ssd cache block where data is written
    /// It is client responsibility to check if there is enough place in block to write key
    /// Returns true if key was written and false if there was not enough place to write key
    bool writeKey(const SSDCacheSimpleKey & cache_key, size_t & offset_in_block)
    {
        assert(cache_key.size > 0);

        if (!enoughtPlaceToWriteKey(cache_key))
            return false;

        char * current_block_offset_data = block_data + current_block_offset;

        /// Write simple key
        memcpy(reinterpret_cast<void *>(current_block_offset_data), reinterpret_cast<const void *>(&cache_key.key), sizeof(cache_key.key));
        current_block_offset_data += sizeof(cache_key.key);
        current_block_offset += sizeof(cache_key.key);

        /// Write serialized columns size
        memcpy(reinterpret_cast<void *>(current_block_offset_data), reinterpret_cast<const void *>(&cache_key.size), sizeof(cache_key.size));
        current_block_offset_data += sizeof(cache_key.size);
        current_block_offset += sizeof(cache_key.size);

        offset_in_block = current_block_offset;

        memcpy(reinterpret_cast<void *>(current_block_offset_data), reinterpret_cast<const void *>(cache_key.data), cache_key.size);
        current_block_offset += cache_key.size;

        ++keys_size;

        return true;
    }

    bool writeKey(const SSDCacheComplexKey & cache_key, size_t & offset_in_block)
    {
        assert(cache_key.size > 0);

        if (!enoughtPlaceToWriteKey(cache_key))
            return false;

        char * current_block_offset_data = block_data + current_block_offset;

        const StringRef & key = cache_key.key;

        /// Write complex key
        memcpy(reinterpret_cast<void *>(current_block_offset_data), reinterpret_cast<const void *>(&key.size), sizeof(key.size));
        current_block_offset_data += sizeof(key.size);
        current_block_offset += sizeof(key.size);

        memcpy(reinterpret_cast<void *>(current_block_offset_data), reinterpret_cast<const void *>(key.data), key.size);
        current_block_offset_data += key.size;
        current_block_offset += key.size;

        /// Write serialized columns size
        memcpy(reinterpret_cast<void *>(current_block_offset_data), reinterpret_cast<const void *>(&cache_key.size), sizeof(cache_key.size));
        current_block_offset_data += sizeof(cache_key.size);
        current_block_offset += sizeof(cache_key.size);

        offset_in_block = current_block_offset;

        memcpy(reinterpret_cast<void *>(current_block_offset_data), reinterpret_cast<const void *>(cache_key.data), cache_key.size);
        current_block_offset += cache_key.size;

        ++keys_size;

        return true;
    }

    inline size_t getKeysSize() const { return keys_size; }

    /// Write keys size into block header
    inline void writeKeysSize()
    {
        char * keys_size_offset_data = block_data + block_header_check_sum_size;
        std::memcpy(keys_size_offset_data, &keys_size, sizeof(size_t));
    }

    /// Get check sum from block header
    inline size_t getCheckSum() const { return unalignedLoad<size_t>(block_data); }

    /// Calculate check sum in block
    inline size_t calculateCheckSum() const
    {
        size_t calculated_check_sum = static_cast<size_t>(CityHash_v1_0_2::CityHash64(block_data + block_header_check_sum_size, block_size - block_header_check_sum_size));

        return calculated_check_sum;
    }

    /// Check if check sum from block header matched calculated check sum in block
    inline bool checkCheckSum() const
    {
        size_t calculated_check_sum = calculateCheckSum();
        size_t check_sum = getCheckSum();

        return calculated_check_sum == check_sum;
    }

    /// Write check sum in block header
    inline void writeCheckSum()
    {
        size_t check_sum = static_cast<size_t>(CityHash_v1_0_2::CityHash64(block_data + block_header_check_sum_size, block_size - block_header_check_sum_size));
        std::memcpy(block_data, &check_sum, sizeof(size_t));
    }

    inline size_t getBlockSize() const { return block_size; }

    /// Returns block data
    inline char * getBlockData() const { return block_data; }

    /// Read keys that were serialized in block
    /// It is client responsibility to ensure that simple or complex keys were written in block
    void readSimpleKeys(PaddedPODArray<UInt64> & simple_keys) const
    {
        char * block_start = block_data + block_header_size;
        char * block_end = block_data + block_size;

        static constexpr size_t key_prefix_size = sizeof(UInt64) + sizeof(size_t);

        while (block_start + key_prefix_size < block_end)
        {
            UInt64 key = unalignedLoad<UInt64>(block_start);
            block_start += sizeof(UInt64);

            size_t allocated_size = unalignedLoad<size_t>(block_start);
            block_start += sizeof(size_t);

            /// If we read empty allocated size that means it is end of block
            if (allocated_size == 0)
                break;

            simple_keys.emplace_back(key);
            block_start += allocated_size;
        }
    }

    void readComplexKeys(PaddedPODArray<StringRef> & complex_keys) const
    {
        char * block_start = block_data + block_header_size;
        char * block_end = block_data + block_size;

        static constexpr size_t key_prefix_size = sizeof(size_t) + sizeof(size_t);

        while (block_start + key_prefix_size < block_end)
        {
            size_t key_size = unalignedLoad<size_t>(block_start);
            block_start += sizeof(key_size);

            StringRef complex_key (block_start, key_size);

            block_start += key_size;

            size_t allocated_size = unalignedLoad<size_t>(block_start);
            block_start += sizeof(size_t);

            /// If we read empty allocated size that means it is end of block
            if (allocated_size == 0)
                break;

            complex_keys.emplace_back(complex_key);
            block_start += allocated_size;
        }
    }

private:
    char * block_data;
    size_t block_size;

    size_t current_block_offset = block_header_size;
    /// TODO: Probably can be removed
    size_t keys_size = 0;
};

struct SSDCacheIndex
{
    SSDCacheIndex(size_t block_index_, size_t offset_in_block_)
        : block_index(block_index_)
        , offset_in_block(offset_in_block_)
    {}

    SSDCacheIndex() = default;

    size_t block_index = 0;
    size_t offset_in_block = 0;
};

inline bool operator==(const SSDCacheIndex & lhs, const SSDCacheIndex & rhs)
{
    return lhs.block_index == rhs.block_index && lhs.offset_in_block == rhs.offset_in_block;
}

template <typename SSDCacheKeyType>
class SSDCacheMemoryBufferPartition
{
public:
    using KeyType = typename SSDCacheKeyType::KeyType;

    explicit SSDCacheMemoryBufferPartition(size_t block_size_, size_t partition_blocks_size_)
        : block_size(block_size_)
        , partition_blocks_size(partition_blocks_size_)
        , buffer(block_size, 4096)
        , current_write_block(block_size)
    {
        current_write_block.reset(buffer.m_data);
    }

    bool writeKey(const SSDCacheKeyType & key, SSDCacheIndex & index)
    {
        if (current_block_index == partition_blocks_size)
            return false;

        size_t block_offset;
        bool write_in_current_block = current_write_block.writeKey(key, block_offset);

        if (write_in_current_block)
        {
            index.block_index = current_block_index;
            index.offset_in_block = block_offset;
            return true;
        }

        current_write_block.writeKeysSize();
        current_write_block.writeCheckSum();

        ++current_block_index;

        if (current_block_index == partition_blocks_size)
            return false;

        current_write_block.reset(buffer.m_data + (block_size * current_block_index));

        write_in_current_block = current_write_block.writeKey(key, block_offset);
        assert(write_in_current_block);

        index.block_index = current_block_index;
        index.offset_in_block = block_offset;

        return write_in_current_block;
    }

    void writeKeysSizeAndCheckSumForCurrentWriteBlock()
    {
        current_write_block.writeKeysSize();
        current_write_block.writeCheckSum();
    }

    inline char * getPlace(SSDCacheIndex index) const
    {
        return buffer.m_data + index.block_index * block_size + index.offset_in_block;
    }

    inline size_t getCurrentBlockIndex() const { return current_block_index; }

    inline const char * getData() const { return buffer.m_data; }

    inline size_t getSizeInBytes() const { return block_size * partition_blocks_size; }

    void readKeys(PaddedPODArray<KeyType> & keys) const
    {
        SSDCacheBlock block(block_size);

        for (size_t block_index = 0; block_index < partition_blocks_size; ++block_index)
        {
            block.reset(buffer.m_data + (block_index * block_size));

            if constexpr (std::is_same_v<KeyType, UInt64>)
                block.readSimpleKeys(keys);
            else
                block.readComplexKeys(keys);
        }
    }

    inline void resetToPartitionStart()
    {
        current_block_index = 0;
        current_write_block.reset(buffer.m_data);
    }

    const size_t block_size;

    const size_t partition_blocks_size;

private:
    Memory<Allocator<true>> buffer;

    SSDCacheBlock current_write_block;

    size_t current_block_index = 0;
};

template <typename SSDCacheKeyType>
class SSDCacheFileBuffer : private boost::noncopyable
{
    static constexpr auto BIN_FILE_EXT = ".bin";

public:

    using KeyType = typename SSDCacheKeyType::KeyType;

    explicit SSDCacheFileBuffer(
        const std::string & file_path_,
        size_t block_size_,
        size_t file_blocks_size_,
        size_t read_from_file_buffer_blocks_size_)
        : file_path(file_path_ + BIN_FILE_EXT)
        , block_size(block_size_)
        , file_blocks_size(file_blocks_size_)
        , read_from_file_buffer_blocks_size(read_from_file_buffer_blocks_size_)
    {
        auto path = std::filesystem::path{file_path};
        auto parent_path_directory = path.parent_path();

        /// If cache file is in directory that does not exists create it
        if (!std::filesystem::exists(parent_path_directory))
            if (!std::filesystem::create_directories(parent_path_directory))
                throw Exception{"Failed to create directories.", ErrorCodes::CANNOT_CREATE_DIRECTORY};

        ProfileEvents::increment(ProfileEvents::FileOpen);

        file.fd = ::open(file_path.c_str(), O_RDWR | O_CREAT | O_TRUNC | O_DIRECT, 0666);
        if (file.fd == -1)
        {
            auto error_code = (errno == ENOENT) ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE;
            throwFromErrnoWithPath("Cannot open file " + file_path, file_path, error_code);
        }

        allocateSizeForNextPartition();
    }

    void allocateSizeForNextPartition()
    {
        if (preallocateDiskSpace(file.fd, current_blocks_size * block_size, block_size * file_blocks_size) < 0)
            throwFromErrnoWithPath("Cannot preallocate space for the file " + file_path, file_path, ErrorCodes::CANNOT_ALLOCATE_MEMORY);

        current_blocks_size += file_blocks_size;
    }

    bool writeBuffer(const char * buffer, size_t buffer_size_in_blocks)
    {
        if (current_block_index + buffer_size_in_blocks > current_blocks_size)
            return false;

        AIOContext aio_context{1};

        iocb write_request{};
        iocb * write_request_ptr{&write_request};

        #if defined(__FreeBSD__)
        write_request.aio.aio_lio_opcode = LIO_WRITE;
        write_request.aio.aio_fildes = file_handler.fd;
        write_request.aio.aio_buf = reinterpret_cast<volatile void *>(buffer);
        write_request.aio.aio_nbytes = block_size * buffer_size_in_blocks;
        write_request.aio.aio_offset = current_block_index * block_size;
        #else
        write_request.aio_lio_opcode = IOCB_CMD_PWRITE;
        write_request.aio_fildes = file.fd;
        write_request.aio_buf = reinterpret_cast<UInt64>(buffer);
        write_request.aio_nbytes = block_size * buffer_size_in_blocks;
        write_request.aio_offset = current_block_index * block_size;
        #endif

        while (io_submit(aio_context.ctx, 1, &write_request_ptr) < 0)
        {
            if (errno != EINTR)
                throw Exception("Cannot submit request for asynchronous IO on file " + file_path, ErrorCodes::CANNOT_IO_SUBMIT);
        }

        // CurrentMetrics::Increment metric_increment_write{CurrentMetrics::Write};

        io_event event;

        while (io_getevents(aio_context.ctx, 1, 1, &event, nullptr) < 0)
        {
            if (errno != EINTR)
                throw Exception("Failed to wait for asynchronous IO completion on file " + file_path, ErrorCodes::CANNOT_IO_GETEVENTS);
        }

        // Unpoison the memory returned from an uninstrumented system function.
        __msan_unpoison(&event, sizeof(event));

        auto bytes_written = eventResult(event);

        ProfileEvents::increment(ProfileEvents::WriteBufferAIOWrite);
        ProfileEvents::increment(ProfileEvents::WriteBufferAIOWriteBytes, bytes_written);

        if (bytes_written != static_cast<decltype(bytes_written)>(block_size * buffer_size_in_blocks))
            throwFromErrno("Not all data was written for asynchronous IO on file " + file_path + ". returned: " + std::to_string(bytes_written), ErrorCodes::AIO_WRITE_ERROR, -event.res);

        if (::fsync(file.fd) < 0)
            throwFromErrnoWithPath("Cannot fsync " + file_path, file_path, ErrorCodes::CANNOT_FSYNC);

        current_block_index += buffer_size_in_blocks;

        return true;
    }

    bool readKeys(size_t block_start, size_t blocks_length, PaddedPODArray<KeyType> & out) const
    {
        if (block_start + blocks_length > current_blocks_size)
            return false;

        size_t buffer_size_in_bytes = blocks_length * block_size;

        Memory read_buffer_memory(block_size * blocks_length, block_size);

        iocb request{};
        iocb * request_ptr = &request;

        #if defined(__FreeBSD__)
        request.aio.aio_lio_opcode = LIO_READ;
        request.aio.aio_fildes = file_handler.fd;
        request.aio.aio_buf = reinterpret_cast<volatile void *>(reinterpret_cast<UInt64>(read_buffer_memory.data()));
        request.aio.aio_nbytes = buffer_size_in_bytes;
        request.aio.aio_offset = block_start * block_size;
        request.aio_data = 0;
        #else
        request.aio_lio_opcode = IOCB_CMD_PREAD;
        request.aio_fildes = file.fd;
        request.aio_buf = reinterpret_cast<UInt64>(read_buffer_memory.data());
        request.aio_nbytes = buffer_size_in_bytes;
        request.aio_offset = block_start * block_size;
        request.aio_data = 0;
        #endif

        io_event event{};
        AIOContext aio_context(1);

        while (io_submit(aio_context.ctx, 1, &request_ptr) != 1)
        {
            if (errno != EINTR)
                throwFromErrno("io_submit: Failed to submit a request for asynchronous IO", ErrorCodes::CANNOT_IO_SUBMIT);
        }

        while (io_getevents(aio_context.ctx, 1, 1, &event, nullptr) != 1)
        {
            if (errno != EINTR)
                throwFromErrno("io_getevents: Failed to get an event for asynchronous IO", ErrorCodes::CANNOT_IO_GETEVENTS);
        }

        auto read_bytes = eventResult(event);

        if (read_bytes != static_cast<ssize_t>(buffer_size_in_bytes))
            throw Exception("GC: AIO failed to read file " + file_path, ErrorCodes::AIO_READ_ERROR);

        SSDCacheBlock block(block_size);

        for (size_t i = 0; i < blocks_length; ++i)
        {
            block.reset(read_buffer_memory.data() + (i * block_size));

            if constexpr (std::is_same_v<SSDCacheKeyType, SSDCacheSimpleKey>)
                block.readSimpleKeys(out);
            else
                block.readComplexKeys(out);
        }

        return true;
    }

    template <typename FetchBlockFunc>
    void fetchBlocks(const PaddedPODArray<size_t> & blocks_to_fetch, FetchBlockFunc && func) const
    {
        if (blocks_to_fetch.empty())
            return;

        /// TODO: Probably make sense to pass it as parameter
        Memory read_buffer(block_size * read_from_file_buffer_blocks_size, 4096);

        PaddedPODArray<iocb> requests;
        PaddedPODArray<iocb *> pointers;

        requests.reserve(blocks_to_fetch.size());
        pointers.reserve(blocks_to_fetch.size());

        for (size_t block_to_fetch_index = 0; block_to_fetch_index < blocks_to_fetch.size(); ++block_to_fetch_index)
        {
            iocb request{};

            char * buffer_place = read_buffer.data() + block_size * (block_to_fetch_index % read_from_file_buffer_blocks_size);

            #if defined(__FreeBSD__)
            request.aio.aio_lio_opcode = LIO_READ;
            request.aio.aio_fildes = file.fd;
            request.aio.aio_buf = reinterpret_cast<volatile void *>(reinterpret_cast<UInt64>(buffer_place));
            request.aio.aio_nbytes = block_size;
            request.aio.aio_offset = block_size * blocks_to_fetch[block_to_fetch_index];
            request.aio_data = block_to_fetch_index;
            #else
            request.aio_lio_opcode = IOCB_CMD_PREAD;
            request.aio_fildes = file.fd;
            request.aio_buf = reinterpret_cast<UInt64>(buffer_place);
            request.aio_nbytes = block_size;
            request.aio_offset = block_size * blocks_to_fetch[block_to_fetch_index];
            request.aio_data = block_to_fetch_index;
            #endif

            requests.push_back(request);
            pointers.push_back(&requests.back());
        }

        AIOContext aio_context(read_from_file_buffer_blocks_size);

        PaddedPODArray<bool> processed(requests.size(), false);
        PaddedPODArray<io_event> events(requests.size());

        size_t to_push = 0;
        size_t to_pop = 0;

        while (to_pop < requests.size())
        {
            int popped = 0;

            while (to_pop < to_push && (popped = io_getevents(aio_context.ctx, to_push - to_pop, to_push - to_pop, &events[to_pop], nullptr)) <= 0)
            {
                if (errno != EINTR)
                    throwFromErrno("io_getevents: Failed to get an event for asynchronous IO", ErrorCodes::CANNOT_IO_GETEVENTS);
            }

            for (size_t i = to_pop; i < to_pop + popped; ++i)
            {
                size_t block_to_fetch_index = events[i].data;
                const auto & request = requests[block_to_fetch_index];

                const ssize_t read_bytes = eventResult(events[i]);

                if (read_bytes != static_cast<ssize_t>(block_size))
                    throw Exception("AIO failed to read file " + file_path + ".", ErrorCodes::AIO_READ_ERROR);

                char * request_buffer = getRequestBuffer(request);

                // Unpoison the memory returned from an uninstrumented system function.
                __msan_unpoison(request_buffer, block_size);

                SSDCacheBlock block(block_size);
                block.reset(request_buffer);

                if (!block.checkCheckSum())
                {
                    std::string calculated_check_sum = std::to_string(block.calculateCheckSum());
                    std::string check_sum = std::to_string(block.getCheckSum());
                    throw Exception("Cache data corrupted. Checksum validation failed. Calculated " +  calculated_check_sum + " in block " + check_sum, ErrorCodes::CORRUPTED_DATA);
                }

                std::forward<FetchBlockFunc>(func)(blocks_to_fetch[block_to_fetch_index], block.getBlockData());

                processed[block_to_fetch_index] = true;
            }

            while (to_pop < requests.size() && processed[to_pop])
                ++to_pop;

            /// add new io tasks
            const int new_tasks_count = std::min(read_from_file_buffer_blocks_size - (to_push - to_pop), requests.size() - to_push);

            int pushed = 0;
            while (new_tasks_count > 0 && (pushed = io_submit(aio_context.ctx, new_tasks_count, &pointers[to_push])) <= 0)
            {
                if (errno != EINTR)
                    throwFromErrno("io_submit: Failed to submit a request for asynchronous IO", ErrorCodes::CANNOT_IO_SUBMIT);
            }

            to_push += pushed;
        }
    }

    inline size_t getCurrentBlockIndex() const { return current_block_index; }

    inline void reset()
    {
        current_block_index = 0;
    }
private:
    struct FileDescriptor : private boost::noncopyable
    {

        FileDescriptor() = default;

        FileDescriptor(FileDescriptor && rhs) : fd(rhs.fd) { rhs.fd = -1; }

        FileDescriptor & operator=(FileDescriptor && rhs)
        {
            close(fd);

            fd = rhs.fd;
            rhs.fd = -1;
        }

        ~FileDescriptor()
        {
            if (fd != -1)
                close(fd);
        }

        int fd = -1;
    };

    static int preallocateDiskSpace(int fd, size_t offset, size_t len)
    {
        #if defined(__FreeBSD__)
            return posix_fallocate(fd, offset, len);
        #else
            return fallocate(fd, 0, offset, len);
        #endif
    }

    static char * getRequestBuffer(const iocb & request)
    {
        char * result = nullptr;
        #if defined(__FreeBSD__)
        result = reinterpret_cast<char *>(reinterpret_cast<UInt64>(request.aio.aio_buf));
        #else
        result = reinterpret_cast<char *>(request.aio_buf);
        #endif

        return result;
    }

    static ssize_t eventResult(io_event & event)
    {
        ssize_t  bytes_written;

        #if defined(__FreeBSD__)
            bytes_written = aio_return(reinterpret_cast<struct aiocb *>(event.udata));
        #else
            bytes_written = event.res;
        #endif

        return bytes_written;
    }

    std::string file_path;
    size_t block_size;
    size_t file_blocks_size;
    size_t read_from_file_buffer_blocks_size;
    FileDescriptor file;

    size_t current_block_index = 0;
    size_t current_blocks_size = 0;
};

template <typename CacheDictionaryStorage>
class ArenaCellKeyDisposer
{
public:
    CacheDictionaryStorage & storage;

    template <typename Key, typename Value>
    void operator()(const Key & key, const Value &) const
    {
        /// In case of complex key we keep it in arena
        if constexpr (std::is_same_v<Key, StringRef>)
            storage.complex_key_arena.free(const_cast<char *>(key.data), key.size);
    }
};

template <DictionaryKeyType dictionary_key_type>
class SSDCacheDictionaryStorage final : public ICacheDictionaryStorage
{
public:
    using SSDCacheKeyType = std::conditional_t<dictionary_key_type == DictionaryKeyType::simple, SSDCacheSimpleKey, SSDCacheComplexKey>;
    using KeyType = std::conditional_t<dictionary_key_type == DictionaryKeyType::simple, UInt64, StringRef>;

    explicit SSDCacheDictionaryStorage(const SSDCacheDictionaryStorageConfiguration & configuration_)
        : configuration(configuration_)
        , file_buffer(configuration_.file_path, configuration.block_size, configuration.file_blocks_size, configuration.read_buffer_blocks_size)
        , rnd_engine(randomSeed())
        , index(configuration.max_stored_keys, false, ArenaCellKeyDisposer<SSDCacheDictionaryStorage<dictionary_key_type>> { *this })
    {
        memory_buffer_partitions.emplace_back(configuration.block_size, configuration.write_buffer_blocks_size);
    }

    bool returnsFetchedColumnsInOrderOfRequestedKeys() const override { return false; }

    bool supportsSimpleKeys() const override { return dictionary_key_type == DictionaryKeyType::simple; }

    SimpleKeysStorageFetchResult fetchColumnsForKeys(
        const PaddedPODArray<UInt64> & keys,
        const DictionaryStorageFetchRequest & fetch_request) override
    {
        if constexpr (dictionary_key_type == DictionaryKeyType::simple)
        {
            SimpleKeysStorageFetchResult result;
            fetchColumnsForKeysImpl(keys, fetch_request, result);
            return result;
        }
        else
            throw Exception("Method insertColumnsForKeys is not supported for complex key storage", ErrorCodes::NOT_IMPLEMENTED);
    }

    void insertColumnsForKeys(const PaddedPODArray<UInt64> & keys, Columns columns) override
    {
        if constexpr (dictionary_key_type == DictionaryKeyType::simple)
            insertColumnsForKeysImpl(keys, columns);
        else
            throw Exception("Method insertColumnsForKeys is not supported for complex key storage", ErrorCodes::NOT_IMPLEMENTED);
    }

    PaddedPODArray<UInt64> getCachedSimpleKeys() const override
    {
        if constexpr (dictionary_key_type == DictionaryKeyType::simple)
            return getCachedKeysImpl();
        else
            throw Exception("Method getCachedSimpleKeys is not supported for complex key storage", ErrorCodes::NOT_IMPLEMENTED);
    }

    bool supportsComplexKeys() const override { return dictionary_key_type == DictionaryKeyType::complex; }

    ComplexKeysStorageFetchResult fetchColumnsForKeys(
        const PaddedPODArray<StringRef> & keys,
        const DictionaryStorageFetchRequest & column_fetch_requests) override
    {
        if constexpr (dictionary_key_type == DictionaryKeyType::complex)
        {
            ComplexKeysStorageFetchResult result;
            fetchColumnsForKeysImpl(keys, column_fetch_requests, result);
            return result;
        }
        else
            throw Exception("Method fetchColumnsForKeys is not supported for simple key storage", ErrorCodes::NOT_IMPLEMENTED);
    }

    void insertColumnsForKeys(const PaddedPODArray<StringRef> & keys, Columns columns) override
    {
        if constexpr (dictionary_key_type == DictionaryKeyType::complex)
            insertColumnsForKeysImpl(keys, columns);
        else
            throw Exception("Method insertColumnsForKeys is not supported for simple key storage", ErrorCodes::NOT_IMPLEMENTED);
    }

    PaddedPODArray<StringRef> getCachedComplexKeys() const override
    {
        if constexpr (dictionary_key_type == DictionaryKeyType::complex)
            return getCachedKeysImpl();
        else
            throw Exception("Method getCachedSimpleKeys is not supported for simple key storage", ErrorCodes::NOT_IMPLEMENTED);
    }

    size_t getSize() const override { return index.size(); }

    size_t getMaxSize() const override {return index.getMaxSize(); }

    size_t getBytesAllocated() const override
    {
        size_t memory_partitions_bytes_size = memory_buffer_partitions.size() * configuration.write_buffer_blocks_size * configuration.block_size;
        size_t file_partitions_bytes_size = memory_buffer_partitions.size() * configuration.file_blocks_size * configuration.block_size;

        return index.getSizeInBytes() + memory_partitions_bytes_size + file_partitions_bytes_size;
    }

private:

    using TimePoint = std::chrono::system_clock::time_point;

    struct Cell
    {
        TimePoint deadline;

        SSDCacheIndex index;
        bool in_memory;
        size_t in_memory_partition_index;
    };

    template <typename Result>
    void fetchColumnsForKeysImpl(
        const PaddedPODArray<KeyType> & keys,
        const DictionaryStorageFetchRequest & fetch_request,
        Result & result) const
    {
        result.fetched_columns = fetch_request.makeAttributesResultColumns();

        const auto now = std::chrono::system_clock::now();

        size_t fetched_columns_index = 0;

        struct KeyToBlockOffset
        {
            KeyToBlockOffset(size_t key_index_, size_t offset_in_block_, bool is_expired_)
                : key_index(key_index_)
                , offset_in_block(offset_in_block_)
                , is_expired(is_expired_)
            {}

            size_t key_index;
            size_t offset_in_block;
            bool is_expired;
        };

        using BlockIndexToKeysMap = std::unordered_map<size_t, PaddedPODArray<KeyToBlockOffset>>;

        BlockIndexToKeysMap block_to_keys_map;
        std::unordered_set<size_t> unique_blocks_to_request;

        for (size_t key_index = 0; key_index < keys.size(); ++key_index)
        {
            auto key = keys[key_index];

            const auto * it = index.find(key);

            if (it)
            {
                const auto & cell = it->getMapped();

                bool has_deadline = cellHasDeadline(cell);

                if (has_deadline && now > cell.deadline + std::chrono::seconds(configuration.strict_max_lifetime_seconds))
                {
                    result.not_found_or_expired_keys.emplace_back(key);
                    result.not_found_or_expired_keys_indexes.emplace_back(key_index);
                    continue;
                }
                else if (has_deadline && now > cell.deadline)
                {
                    if (cell.in_memory)
                    {
                        const auto & partition = memory_buffer_partitions[cell.in_memory_partition_index];
                        char * serialized_columns_place = partition.getPlace(cell.index);
                        deserializeAndInsertIntoColumns(result.fetched_columns, fetch_request, serialized_columns_place);
                        result.expired_keys_to_fetched_columns_index[key] = fetched_columns_index;
                        result.not_found_or_expired_keys.emplace_back(key);
                        result.not_found_or_expired_keys_indexes.emplace_back(key_index);
                        ++fetched_columns_index;
                    }
                    else
                    {
                        block_to_keys_map[cell.index.block_index].emplace_back(key_index, cell.index.offset_in_block, true);
                        unique_blocks_to_request.insert(cell.index.block_index);
                    }
                }
                else
                {
                    if (cell.in_memory)
                    {
                        const auto & partition = memory_buffer_partitions[cell.in_memory_partition_index];
                        char * serialized_columns_place = partition.getPlace(cell.index);
                        deserializeAndInsertIntoColumns(result.fetched_columns, fetch_request, serialized_columns_place);
                        result.found_keys_to_fetched_columns_index[key] = fetched_columns_index;
                        ++fetched_columns_index;
                    }
                    else
                    {
                        block_to_keys_map[cell.index.block_index].emplace_back(key_index, cell.index.offset_in_block, false);
                        unique_blocks_to_request.insert(cell.index.block_index);
                    }
                }
            }
            else
            {
                result.not_found_or_expired_keys.emplace_back(key);
                result.not_found_or_expired_keys_indexes.emplace_back(key_index);
            }
        }

        PaddedPODArray<size_t> blocks_to_request;
        blocks_to_request.reserve(unique_blocks_to_request.size());

        for (auto block : unique_blocks_to_request)
            blocks_to_request.emplace_back(block);

        /// Sort blocks by offset before start async io requests
        std::sort(blocks_to_request.begin(), blocks_to_request.end());

        file_buffer.fetchBlocks(blocks_to_request, [&](size_t block_index, char * block_data)
        {
            PaddedPODArray<KeyToBlockOffset> & keys_in_block = block_to_keys_map[block_index];

            for (auto & key_in_block : keys_in_block)
            {
                auto key = keys[key_in_block.key_index];

                char * key_data = block_data + key_in_block.offset_in_block;
                deserializeAndInsertIntoColumns(result.fetched_columns, fetch_request, key_data);

                auto & found_key_hash_map
                    = key_in_block.is_expired ? result.expired_keys_to_fetched_columns_index : result.found_keys_to_fetched_columns_index;

                found_key_hash_map[key] = fetched_columns_index;
                ++fetched_columns_index;

                if (key_in_block.is_expired)
                {
                    result.not_found_or_expired_keys.emplace_back(key);
                    result.not_found_or_expired_keys_indexes.emplace_back(key_in_block.key_index);
                }
            }
        });
    }

    void insertColumnsForKeysImpl(const PaddedPODArray<KeyType> & keys, Columns columns)
    {
        size_t columns_to_serialize_size = columns.size();
        PaddedPODArray<StringRef> temporary_column_data(columns_to_serialize_size);

        Arena temporary_values_pool;

        const auto now = std::chrono::system_clock::now();

        for (size_t key_index = 0; key_index < keys.size(); ++key_index)
        {
            size_t allocated_size_for_columns = 0;
            const char * block_start = nullptr;

            auto key = keys[key_index];

            for (size_t column_index = 0; column_index < columns_to_serialize_size; ++column_index)
            {
                auto & column = columns[column_index];
                temporary_column_data[column_index] = column->serializeValueIntoArena(key_index, temporary_values_pool, block_start);
                allocated_size_for_columns += temporary_column_data[column_index].size;
            }

            SSDCacheKeyType ssd_cache_key { key, allocated_size_for_columns, block_start };

            if (!SSDCacheBlock::canBeWrittenInEmptyBlock(ssd_cache_key, configuration.block_size))
                throw Exception("Serialized columns size is greater than allowed block size and metadata", ErrorCodes::UNSUPPORTED_METHOD);

            /// We cannot reuse place that is already allocated in file or memory cache so we erase key from index
            if (index.contains(key))
                index.erase(key);

            Cell cell;
            setCellDeadline(cell, now);

            if constexpr (dictionary_key_type == DictionaryKeyType::complex)
            {
                /// Copy complex key into arena and put in cache
                size_t key_size = key.size;
                char * place_for_key = complex_key_arena.alloc(key_size);
                memcpy(reinterpret_cast<void *>(place_for_key), reinterpret_cast<const void *>(key.data), key_size);
                KeyType updated_key{place_for_key, key_size};
                ssd_cache_key.key = updated_key;
            }

            insertCell(ssd_cache_key, cell);

            temporary_values_pool.rollback(allocated_size_for_columns);
        }
    }

    PaddedPODArray<KeyType> getCachedKeysImpl() const
    {
        PaddedPODArray<KeyType> result;
        result.reserve(index.size());

        for (auto & node : index)
            result.emplace_back(node.getKey());

        return result;
    }

    void insertCell(SSDCacheKeyType & ssd_cache_key, Cell & cell)
    {
        SSDCacheIndex cache_index;

        size_t loop_count = 0;

        while (true)
        {
            bool started_reusing_old_partitions = memory_buffer_partitions.size() == configuration.max_partitions_count;

            auto & current_memory_buffer_partition = memory_buffer_partitions[current_partition_index];

            bool write_into_memory_buffer_result = current_memory_buffer_partition.writeKey(ssd_cache_key, cache_index);

            if (write_into_memory_buffer_result)
            {
                cell.in_memory = true;
                cell.index = cache_index;
                cell.in_memory_partition_index = current_partition_index;

                index.insert(ssd_cache_key.key, cell);
                break;
            }
            else
            {
                /// Partition memory write buffer if full flush it to disk and retry
                size_t block_index_in_file_before_write = file_buffer.getCurrentBlockIndex();

                if (started_reusing_old_partitions)
                {
                    /// If we start reusing old partitions we need to remove old keys on disk from index before writing buffer
                    PaddedPODArray<KeyType> old_keys;
                    file_buffer.readKeys(block_index_in_file_before_write, configuration.write_buffer_blocks_size, old_keys);

                    for (auto old_key : old_keys)
                        index.erase(old_key);
                }

                const char * partition_data = current_memory_buffer_partition.getData();

                bool flush_to_file_result = file_buffer.writeBuffer(partition_data, configuration.write_buffer_blocks_size);
                std::cerr << "Flushed to file " << flush_to_file_result << std::endl;

                if (flush_to_file_result)
                {
                    /// Update index cells keys offset and block index
                    PaddedPODArray<KeyType> keys_to_update;
                    current_memory_buffer_partition.readKeys(keys_to_update);


                    for (auto key_to_update : keys_to_update)
                    {
                        auto * it = index.find(key_to_update);

                        /// If multiple keys with different sizes are inserted we can have duplicates
                        if (!it)
                            continue;

                        Cell & cell_to_update = it->getMapped();

                        cell_to_update.in_memory = false;
                        cell_to_update.index.block_index = block_index_in_file_before_write;
                    }

                    /// Memory buffer partition flushed to disk start reusing it
                    current_memory_buffer_partition.resetToPartitionStart();
                    memset(const_cast<char *>(current_memory_buffer_partition.getData()), 0, current_memory_buffer_partition.getSizeInBytes());

                    write_into_memory_buffer_result = current_memory_buffer_partition.writeKey(ssd_cache_key, cache_index);
                    assert(write_into_memory_buffer_result);

                    cell.in_memory = true;
                    cell.index = cache_index;
                    cell.in_memory_partition_index = current_partition_index;

                    index.insert(ssd_cache_key.key, cell);
                    break;
                }
                else
                {
                    /// Partition is full need to try next partition

                    if (memory_buffer_partitions.size() < configuration.max_partitions_count)
                    {
                        /// Try tro create next partition without reusing old partitions
                        ++current_partition_index;

                        file_buffer.allocateSizeForNextPartition();

                        memory_buffer_partitions.emplace_back(configuration.block_size, configuration.write_buffer_blocks_size);
                    }
                    else
                    {
                        /// Start reusing old partitions
                        current_partition_index = (current_partition_index + 1) % memory_buffer_partitions.size();
                        file_buffer.reset();
                    }
                }

                ++loop_count;
            }
        }
    }

    inline static bool cellHasDeadline(const Cell & cell)
    {
        return cell.deadline != std::chrono::system_clock::from_time_t(0);
    }

    inline void setCellDeadline(Cell & cell, TimePoint now)
    {
        if (configuration.lifetime.min_sec == 0 && configuration.lifetime.max_sec == 0)
            cell.deadline = std::chrono::system_clock::from_time_t(0);

        size_t min_sec_lifetime = configuration.lifetime.min_sec;
        size_t max_sec_lifetime = configuration.lifetime.max_sec;

        std::uniform_int_distribution<UInt64> distribution{min_sec_lifetime, max_sec_lifetime};
        cell.deadline = now + std::chrono::seconds{distribution(rnd_engine)};
    }

    template <typename>
    friend class ArenaCellKeyDisposer;

    SSDCacheDictionaryStorageConfiguration configuration;

    SSDCacheFileBuffer<SSDCacheKeyType> file_buffer;

    std::vector<SSDCacheMemoryBufferPartition<SSDCacheKeyType>> memory_buffer_partitions;

    pcg64 rnd_engine;

    using SimpleKeyLRUHashMap = LRUHashMap<UInt64, Cell, ArenaCellKeyDisposer<SSDCacheDictionaryStorage<dictionary_key_type>>>;
    using ComplexKeyLRUHashMap = LRUHashMapWithSavedHash<StringRef, Cell, ArenaCellKeyDisposer<SSDCacheDictionaryStorage<dictionary_key_type>>>;

    using CacheLRUHashMap = std::conditional_t<
        dictionary_key_type == DictionaryKeyType::simple,
        SimpleKeyLRUHashMap,
        ComplexKeyLRUHashMap>;

    ArenaWithFreeLists complex_key_arena;

    CacheLRUHashMap index;

    size_t current_partition_index = 0;

};

}
