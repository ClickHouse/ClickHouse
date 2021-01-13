#if defined(OS_LINUX) || defined(__FreeBSD__)

#include "SSDComplexKeyCacheDictionary.h"

#include <algorithm>
#include <Columns/ColumnsNumber.h>
#include <Common/typeid_cast.h>
#include <Common/ProfileEvents.h>
#include <Common/ProfilingScopedRWLock.h>
#include <Common/MemorySanitizer.h>
#include <DataStreams/IBlockInputStream.h>
#include "DictionaryBlockInputStream.h"
#include "DictionaryFactory.h"
#include <IO/AIO.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <ext/chrono_io.h>
#include <ext/map.h>
#include <ext/range.h>
#include <ext/size.h>
#include <ext/bit_cast.h>
#include <numeric>
#include <filesystem>
#include <city.h>

namespace ProfileEvents
{
    extern const Event DictCacheKeysRequested;
    extern const Event DictCacheKeysRequestedMiss;
    extern const Event DictCacheKeysRequestedFound;
    extern const Event DictCacheKeysExpired;
    extern const Event DictCacheKeysNotFound;
    extern const Event DictCacheKeysHit;
    extern const Event DictCacheRequestTimeNs;
    extern const Event DictCacheRequests;
    extern const Event DictCacheLockWriteNs;
    extern const Event DictCacheLockReadNs;
    extern const Event FileOpen;
    extern const Event WriteBufferAIOWrite;
    extern const Event WriteBufferAIOWriteBytes;
}

namespace CurrentMetrics
{
    extern const Metric DictCacheRequests;
    extern const Metric Write;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int AIO_READ_ERROR;
    extern const int AIO_WRITE_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int CACHE_DICTIONARY_UPDATE_FAIL;
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int CANNOT_CREATE_DIRECTORY;
    extern const int CANNOT_FSYNC;
    extern const int CANNOT_IO_GETEVENTS;
    extern const int CANNOT_IO_SUBMIT;
    extern const int CANNOT_OPEN_FILE;
    extern const int CORRUPTED_DATA;
    extern const int FILE_DOESNT_EXIST;
    extern const int NOT_IMPLEMENTED;
    extern const int TYPE_MISMATCH;
    extern const int UNSUPPORTED_METHOD;
}

namespace
{
    constexpr size_t DEFAULT_SSD_BLOCK_SIZE_BYTES = DEFAULT_AIO_FILE_BLOCK_SIZE;
    constexpr size_t DEFAULT_FILE_SIZE_BYTES = 4 * 1024 * 1024 * 1024ULL;
    constexpr size_t DEFAULT_PARTITIONS_COUNT = 16;
    constexpr size_t DEFAULT_READ_BUFFER_SIZE_BYTES = 16 * DEFAULT_SSD_BLOCK_SIZE_BYTES;
    constexpr size_t DEFAULT_WRITE_BUFFER_SIZE_BYTES = DEFAULT_SSD_BLOCK_SIZE_BYTES;

    constexpr size_t DEFAULT_MAX_STORED_KEYS = 100000;

    constexpr size_t BUFFER_ALIGNMENT = DEFAULT_AIO_FILE_BLOCK_SIZE;
    constexpr size_t BLOCK_CHECKSUM_SIZE_BYTES = 8;
    constexpr size_t BLOCK_SPECIAL_FIELDS_SIZE_BYTES = 4;

    constexpr UInt64 KEY_METADATA_EXPIRES_AT_MASK = std::numeric_limits<std::chrono::system_clock::time_point::rep>::max();
    constexpr UInt64 KEY_METADATA_IS_DEFAULT_MASK = ~KEY_METADATA_EXPIRES_AT_MASK;

    constexpr size_t KEY_IN_MEMORY_BIT = 63;
    constexpr size_t KEY_IN_MEMORY = (1ULL << KEY_IN_MEMORY_BIT);
    constexpr size_t BLOCK_INDEX_BITS = 32;
    constexpr size_t INDEX_IN_BLOCK_BITS = 16;
    constexpr size_t INDEX_IN_BLOCK_MASK = (1ULL << INDEX_IN_BLOCK_BITS) - 1;
    constexpr size_t BLOCK_INDEX_MASK = ((1ULL << (BLOCK_INDEX_BITS + INDEX_IN_BLOCK_BITS)) - 1) ^ INDEX_IN_BLOCK_MASK;

    constexpr size_t NOT_EXISTS = -1;

    constexpr UInt8 HAS_NOT_FOUND = 2;

    const std::string BIN_FILE_EXT = ".bin";
    const std::string IND_FILE_EXT = ".idx";

    int preallocateDiskSpace(int fd, size_t len)
    {
        #if defined(__FreeBSD__)
            return posix_fallocate(fd, 0, len);
        #else
            return fallocate(fd, 0, 0, len);
        #endif
    }
}

SSDComplexKeyCachePartition::Metadata::time_point_t SSDComplexKeyCachePartition::Metadata::expiresAt() const
{
    return ext::safe_bit_cast<time_point_t>(data & KEY_METADATA_EXPIRES_AT_MASK);
}
void SSDComplexKeyCachePartition::Metadata::setExpiresAt(const time_point_t & t)
{
    data = ext::safe_bit_cast<time_point_urep_t>(t);
}

bool SSDComplexKeyCachePartition::Metadata::isDefault() const
{
    return (data & KEY_METADATA_IS_DEFAULT_MASK) == KEY_METADATA_IS_DEFAULT_MASK;
}
void SSDComplexKeyCachePartition::Metadata::setDefault()
{
    data |= KEY_METADATA_IS_DEFAULT_MASK;
}

bool SSDComplexKeyCachePartition::Index::inMemory() const
{
    return (index & KEY_IN_MEMORY) == KEY_IN_MEMORY;
}

bool SSDComplexKeyCachePartition::Index::exists() const
{
    return index != NOT_EXISTS;
}

void SSDComplexKeyCachePartition::Index::setNotExists()
{
    index = NOT_EXISTS;
}

void SSDComplexKeyCachePartition::Index::setInMemory(const bool in_memory)
{
    index = (index & ~KEY_IN_MEMORY) | (static_cast<size_t>(in_memory) << KEY_IN_MEMORY_BIT);
}

size_t SSDComplexKeyCachePartition::Index::getAddressInBlock() const
{
    return index & INDEX_IN_BLOCK_MASK;
}

void SSDComplexKeyCachePartition::Index::setAddressInBlock(const size_t address_in_block)
{
    index = (index & ~INDEX_IN_BLOCK_MASK) | address_in_block;
}

size_t SSDComplexKeyCachePartition::Index::getBlockId() const
{
    return (index & BLOCK_INDEX_MASK) >> INDEX_IN_BLOCK_BITS;
}

void SSDComplexKeyCachePartition::Index::setBlockId(const size_t block_id)
{
    index = (index & ~BLOCK_INDEX_MASK) | (block_id << INDEX_IN_BLOCK_BITS);
}

SSDComplexKeyCachePartition::SSDComplexKeyCachePartition(
        const AttributeUnderlyingType & /* key_structure */,
        const std::vector<AttributeUnderlyingType> & attributes_structure_,
        const std::string & dir_path,
        const size_t file_id_,
        const size_t max_size_,
        const size_t block_size_,
        const size_t read_buffer_size_,
        const size_t write_buffer_size_,
        const size_t max_stored_keys_)
    : file_id(file_id_)
    , max_size(max_size_)
    , block_size(block_size_)
    , read_buffer_size(read_buffer_size_)
    , write_buffer_size(write_buffer_size_)
    , max_stored_keys(max_stored_keys_)
    , path(dir_path + "/" + std::to_string(file_id))
    , key_to_index(max_stored_keys, KeyDeleter(keys_pool))
    , attributes_structure(attributes_structure_)
{
    if (!std::filesystem::create_directories(std::filesystem::path{dir_path}))
    {
        if (std::filesystem::exists(std::filesystem::path{dir_path}))
            LOG_INFO(&Poco::Logger::get("SSDComplexKeyCachePartition::Constructor"), "Using existing directory '{}' for cache-partition", dir_path);
        else
            throw Exception{"Failed to create directories.", ErrorCodes::CANNOT_CREATE_DIRECTORY};
    }

    {
        ProfileEvents::increment(ProfileEvents::FileOpen);

        const std::string filename = path + BIN_FILE_EXT;
        fd = ::open(filename.c_str(), O_RDWR | O_CREAT | O_TRUNC | O_DIRECT, 0666);
        if (fd == -1)
        {
            auto error_code = (errno == ENOENT) ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE;
            throwFromErrnoWithPath("Cannot open file " + filename, filename, error_code);
        }

        if (preallocateDiskSpace(fd, max_size * block_size) < 0)
            throwFromErrnoWithPath("Cannot preallocate space for the file " + filename, filename, ErrorCodes::CANNOT_ALLOCATE_MEMORY);
    }
}

SSDComplexKeyCachePartition::~SSDComplexKeyCachePartition()
{
    std::unique_lock lock(rw_lock);
    ::close(fd);
}

size_t SSDComplexKeyCachePartition::appendDefaults(
    const KeyRefs & keys_in,
    const PaddedPODArray<Metadata> & metadata,
    const size_t begin)
{
    std::unique_lock lock(rw_lock);
    KeyRefs keys(keys_in.size());
    for (size_t i = 0; i < keys_in.size(); ++i)
        keys[i] = keys_pool.copyKeyFrom(keys_in[i]);

    return append(keys, Attributes{}, metadata, begin);
}

size_t SSDComplexKeyCachePartition::appendBlock(
    const Columns & key_columns, const DataTypes & /* key_types */,
    const Attributes & new_attributes, const PaddedPODArray<Metadata> & metadata, const size_t begin)
{
    std::unique_lock lock(rw_lock);
    if (!new_attributes.empty() && new_attributes.size() != attributes_structure.size())
        throw Exception{"Wrong columns number in block.", ErrorCodes::BAD_ARGUMENTS};

    const auto keys_size = key_columns.size();
    KeyRefs keys(key_columns.front()->size());
    {
        StringRefs tmp_keys_refs(keys_size);
        for (size_t i = 0; i < key_columns.front()->size(); ++i)
            keys[i] = keys_pool.allocKey(i, key_columns, tmp_keys_refs);
    }

    return append(keys, new_attributes, metadata, begin);
}

size_t SSDComplexKeyCachePartition::append(
    const KeyRefs & keys,
    const Attributes & new_attributes,
    const PaddedPODArray<Metadata> & metadata,
    const size_t begin)
{
    if (!memory)
        memory.emplace(block_size * write_buffer_size, BUFFER_ALIGNMENT);

    auto init_write_buffer = [&]()
    {
        write_buffer.emplace(memory->data() + current_memory_block_id * block_size, block_size);
        uint64_t tmp = 0;
        write_buffer->write(reinterpret_cast<char*>(&tmp), BLOCK_CHECKSUM_SIZE_BYTES);
        write_buffer->write(reinterpret_cast<char*>(&tmp), BLOCK_SPECIAL_FIELDS_SIZE_BYTES);
        keys_in_block = 0;
    };

    if (!write_buffer)
        init_write_buffer();
    if (!keys_buffer_pool)
        keys_buffer_pool.emplace();

    bool flushed = false;
    auto finish_block = [&]()
    {
        write_buffer.reset();
        std::memcpy(memory->data() + block_size * current_memory_block_id + BLOCK_CHECKSUM_SIZE_BYTES, &keys_in_block, sizeof(keys_in_block)); // set count
        uint64_t checksum = CityHash_v1_0_2::CityHash64(memory->data() + block_size * current_memory_block_id + BLOCK_CHECKSUM_SIZE_BYTES, block_size - BLOCK_CHECKSUM_SIZE_BYTES); // checksum
        std::memcpy(memory->data() + block_size * current_memory_block_id, &checksum, sizeof(checksum));
        if (++current_memory_block_id == write_buffer_size)
            flush();
        flushed = true;
    };

    for (size_t index = begin; index < keys.size();)
    {
        Index cache_index;
        cache_index.setInMemory(true);
        cache_index.setBlockId(current_memory_block_id);
        cache_index.setAddressInBlock(write_buffer->offset());

        flushed = false;
        if (keys[index].fullSize() + sizeof(UInt64) > write_buffer->available()) // place for key and metadata
        {
            finish_block();
        }
        else
        {
            keys_pool.writeKey(keys[index], *write_buffer);
            writeBinary(metadata[index].data, *write_buffer);
        }

        for (const auto & attribute : new_attributes)
        {
            if (flushed)
                break;
            switch (attribute.type)
            {
#define DISPATCH(TYPE) \
            case AttributeUnderlyingType::ut##TYPE: \
                { \
                    if (sizeof(TYPE) > write_buffer->available()) \
                    { \
                        finish_block(); \
                        continue; \
                    } \
                    else \
                    { \
                        const auto & values = std::get<Attribute::Container<TYPE>>(attribute.values); /* NOLINT */ \
                        writeBinary(values[index], *write_buffer); \
                    } \
                } \
                break;

                DISPATCH(UInt8)
                DISPATCH(UInt16)
                DISPATCH(UInt32)
                DISPATCH(UInt64)
                DISPATCH(UInt128)
                DISPATCH(Int8)
                DISPATCH(Int16)
                DISPATCH(Int32)
                DISPATCH(Int64)
                DISPATCH(Decimal32)
                DISPATCH(Decimal64)
                DISPATCH(Decimal128)
                DISPATCH(Float32)
                DISPATCH(Float64)
#undef DISPATCH

            case AttributeUnderlyingType::utString:
                {
                    const auto & value = std::get<Attribute::Container<String>>(attribute.values)[index];
                    if (sizeof(UInt64) + value.size() > write_buffer->available())
                    {
                        finish_block();
                        continue;
                    }
                    else
                    {
                        writeStringBinary(value, *write_buffer);
                    }
                }
                break;
            }
        }

        if (!flushed)
        {
            key_to_index.setWithDelete(keys[index], cache_index);
            keys_buffer.push_back(keys_buffer_pool->copyKeyFrom(keys[index]));
            ++index;
            ++keys_in_block;
        }
        else  // next block in write buffer or flushed to ssd
        {
            init_write_buffer();
        }
    }
    return keys.size() - begin;
}

void SSDComplexKeyCachePartition::flush()
{
    if (current_file_block_id >= max_size)
        clearOldestBlocks();

    if (keys_buffer.empty())
        return;

    AIOContext aio_context{1};

    iocb write_request{};
    iocb * write_request_ptr{&write_request};

#if defined(__FreeBSD__)
    write_request.aio.aio_lio_opcode = LIO_WRITE;
    write_request.aio.aio_fildes = fd;
    write_request.aio.aio_buf = reinterpret_cast<volatile void *>(memory->data());
    write_request.aio.aio_nbytes = block_size * write_buffer_size;
    write_request.aio.aio_offset = (current_file_block_id % max_size) * block_size;
#else
    write_request.aio_lio_opcode = IOCB_CMD_PWRITE;
    write_request.aio_fildes = fd;
    write_request.aio_buf = reinterpret_cast<UInt64>(memory->data());
    write_request.aio_nbytes = block_size * write_buffer_size;
    write_request.aio_offset = (current_file_block_id % max_size) * block_size;
#endif

    while (io_submit(aio_context.ctx, 1, &write_request_ptr) < 0)
    {
        if (errno != EINTR)
            throw Exception("Cannot submit request for asynchronous IO on file " + path + BIN_FILE_EXT, ErrorCodes::CANNOT_IO_SUBMIT);
    }

    CurrentMetrics::Increment metric_increment_write{CurrentMetrics::Write};

    io_event event;
    while (io_getevents(aio_context.ctx, 1, 1, &event, nullptr) < 0)
    {
        if (errno != EINTR)
            throw Exception("Failed to wait for asynchronous IO completion on file " + path + BIN_FILE_EXT, ErrorCodes::CANNOT_IO_GETEVENTS);
    }

    // Unpoison the memory returned from an uninstrumented system function.
    __msan_unpoison(&event, sizeof(event));

    ssize_t  bytes_written;
#if defined(__FreeBSD__)
    bytes_written = aio_return(reinterpret_cast<struct aiocb *>(event.udata));
#else
    bytes_written = event.res;
#endif

    ProfileEvents::increment(ProfileEvents::WriteBufferAIOWrite);
    ProfileEvents::increment(ProfileEvents::WriteBufferAIOWriteBytes, bytes_written);

    if (bytes_written != static_cast<decltype(bytes_written)>(block_size * write_buffer_size))
        throw Exception("Not all data was written for asynchronous IO on file " + path + BIN_FILE_EXT + ". returned: " + std::to_string(bytes_written), ErrorCodes::AIO_WRITE_ERROR);

    if (::fsync(fd) < 0)
        throwFromErrnoWithPath("Cannot fsync " + path + BIN_FILE_EXT, path + BIN_FILE_EXT, ErrorCodes::CANNOT_FSYNC);

    /// commit changes in index
    for (auto & key : keys_buffer)
    {
        Index index;
        if (key_to_index.getKeyAndValue(key, index))
        {
            if (index.inMemory()) // Row can be inserted in the buffer twice, so we need to move to ssd only the last index.
            {
                index.setInMemory(false);
                index.setBlockId((current_file_block_id % max_size) + index.getBlockId());
            }
            key_to_index.set(key, index);
        }
    }

    current_file_block_id += write_buffer_size;
    current_memory_block_id = 0;

    /// clear buffer
    keys_buffer.clear();
    keys_buffer_pool.reset();
    keys_buffer_pool.emplace();
}

template <typename Out, typename GetDefault>
void SSDComplexKeyCachePartition::getValue(
    const size_t attribute_index, const Columns & key_columns, const DataTypes & key_types,
    ResultArrayType<Out> & out, std::vector<bool> & found, GetDefault & get_default,
    std::chrono::system_clock::time_point now) const
{
    auto set_value = [&](const size_t index, ReadBuffer & buf)
    {
        keys_pool.ignoreKey(buf);
        Metadata metadata;
        readVarUInt(metadata.data, buf);

        if (metadata.expiresAt() > now)
        {
            if (metadata.isDefault())
                out[index] = get_default(index);
            else
            {
                ignoreFromBufferToAttributeIndex(attribute_index, buf);
                readBinary(out[index], buf);
            }
            found[index] = true;
        }
    };

    getImpl(key_columns, key_types, set_value, found);
}

void SSDComplexKeyCachePartition::getString(const size_t attribute_index,
    const Columns & key_columns, const DataTypes & key_types,
    StringRefs & refs, ArenaWithFreeLists & arena, std::vector<bool> & found,
    std::vector<size_t> & default_ids,
    std::chrono::system_clock::time_point now) const
{
    auto set_value = [&](const size_t index, ReadBuffer & buf)
    {
        keys_pool.ignoreKey(buf);
        Metadata metadata;
        readBinary(metadata.data, buf);

        if (metadata.expiresAt() > now)
        {
            if (metadata.isDefault())
                default_ids.push_back(index);
            else
            {
                ignoreFromBufferToAttributeIndex(attribute_index, buf);
                size_t size = 0;
                readVarUInt(size, buf);
                char * string_ptr = arena.alloc(size);
                memcpy(string_ptr, buf.position(), size);
                refs[index].data = string_ptr;
                refs[index].size = size;
            }
            found[index] = true;
        }
    };

    getImpl(key_columns, key_types, set_value, found);
}

void SSDComplexKeyCachePartition::has(
    const Columns & key_columns, const DataTypes & key_types, ResultArrayType<UInt8> & out,
    std::vector<bool> & found, std::chrono::system_clock::time_point now) const
{
    auto set_value = [&](const size_t index, ReadBuffer & buf)
    {
        keys_pool.ignoreKey(buf);
        Metadata metadata;
        readBinary(metadata.data, buf);

        if (metadata.expiresAt() > now)
            out[index] = !metadata.isDefault();
    };

    getImpl(key_columns, key_types, set_value, found);
}

template <typename SetFunc>
void SSDComplexKeyCachePartition::getImpl(
    const Columns & key_columns, const DataTypes & /* key_types */,
    SetFunc & set, std::vector<bool> & found) const
{
    TemporalComplexKeysPool tmp_keys_pool;
    StringRefs tmp_refs(key_columns.size());

    std::shared_lock lock(rw_lock);
    PaddedPODArray<Index> indices(key_columns.front()->size());
    for (size_t i = 0; i < key_columns.front()->size(); ++i)
    {
        auto key = tmp_keys_pool.allocKey(i, key_columns, tmp_refs);
        SCOPE_EXIT(tmp_keys_pool.rollback(key));
        Index index;
        if (found[i])
            indices[i].setNotExists();
        else if (key_to_index.get(key, index))
            indices[i] = index;
        else
            indices[i].setNotExists();
    }

    getValueFromMemory(indices, set);
    getValueFromStorage(indices, set);
}

template <typename SetFunc>
void SSDComplexKeyCachePartition::getValueFromMemory(const PaddedPODArray<Index> & indices, SetFunc & set) const
{
    // Do not check checksum while reading from memory.
    for (size_t i = 0; i < indices.size(); ++i)
    {
        const auto & index = indices[i];
        if (index.exists() && index.inMemory())
        {
            const size_t offset = index.getBlockId() * block_size + index.getAddressInBlock();

            ReadBufferFromMemory read_buffer(memory->data() + offset, block_size * write_buffer_size - offset);
            set(i, read_buffer);
        }
    }
}

template <typename SetFunc>
void SSDComplexKeyCachePartition::getValueFromStorage(const PaddedPODArray<Index> & indices, SetFunc & set) const
{
    std::vector<std::pair<Index, size_t>> index_to_out;
    for (size_t i = 0; i < indices.size(); ++i)
    {
        const auto & index = indices[i];
        if (index.exists() && !index.inMemory())
            index_to_out.emplace_back(index, i);
    }
    if (index_to_out.empty())
        return;

    /// sort by (block_id, offset_in_block)
    std::sort(std::begin(index_to_out), std::end(index_to_out));

    Memory read_buffer(block_size * read_buffer_size, BUFFER_ALIGNMENT);

    // TODO: merge requests
    std::vector<iocb> requests;
    std::vector<iocb*> pointers;
    std::vector<std::vector<size_t>> blocks_to_indices;
    requests.reserve(index_to_out.size());
    pointers.reserve(index_to_out.size());
    blocks_to_indices.reserve(index_to_out.size());
    for (size_t i = 0; i < index_to_out.size(); ++i)
    {
        #if defined(__FreeBSD__)
        const size_t back_offset = requests.empty() ? -1 : static_cast<size_t>(requests.back().aio.aio_offset);
        #else
        const size_t back_offset = requests.empty() ? -1 : static_cast<size_t>(requests.back().aio_offset);
        #endif

        if (!requests.empty() && back_offset == index_to_out[i].first.getBlockId() * block_size)
        {
            blocks_to_indices.back().push_back(i);
            continue;
        }

        iocb request{};
#if defined(__FreeBSD__)
        request.aio.aio_lio_opcode = LIO_READ;
        request.aio.aio_fildes = fd;
        request.aio.aio_buf = reinterpret_cast<volatile void *>(
            reinterpret_cast<UInt64>(read_buffer.data()) + block_size * (requests.size() % read_buffer_size));
        request.aio.aio_nbytes = block_size;
        request.aio.aio_offset = index_to_out[i].first.getBlockId() * block_size;
        request.aio_data = requests.size();
#else
        request.aio_lio_opcode = IOCB_CMD_PREAD;
        request.aio_fildes = fd;
        request.aio_buf = reinterpret_cast<UInt64>(read_buffer.data()) + block_size * (requests.size() % read_buffer_size);
        request.aio_nbytes = block_size;
        request.aio_offset = index_to_out[i].first.getBlockId() * block_size;
        request.aio_data = requests.size();
#endif
        requests.push_back(request);
        pointers.push_back(&requests.back());
        blocks_to_indices.emplace_back();
        blocks_to_indices.back().push_back(i);
    }

    AIOContext aio_context(read_buffer_size);

    std::vector<bool> processed(requests.size(), false);
    std::vector<io_event> events(requests.size());
    #if defined(__linux__)
    for (auto & event : events)
        event.res = -1;
    #endif


    size_t to_push = 0;
    size_t to_pop = 0;
    while (to_pop < requests.size())
    {
        /// get io tasks from previous iteration
        int popped = 0;
        while (to_pop < to_push && (popped = io_getevents(aio_context.ctx, to_push - to_pop, to_push - to_pop, &events[to_pop], nullptr)) <= 0)
        {
            if (errno != EINTR)
                throwFromErrno("io_getevents: Failed to get an event for asynchronous IO", ErrorCodes::CANNOT_IO_GETEVENTS);
        }

        for (size_t i = to_pop; i < to_pop + popped; ++i)
        {
            const auto request_id = events[i].data;
            const auto & request = requests[request_id];

            #if defined(__FreeBSD__)
            const auto bytes_written = aio_return(reinterpret_cast<struct aiocb *>(events[i].udata));
            #else
            const auto bytes_written = events[i].res;
            #endif

            if (bytes_written != static_cast<ssize_t>(block_size))
            {
                #if defined(__FreeBSD__)
                    throw Exception("AIO failed to read file " + path + BIN_FILE_EXT + ".", ErrorCodes::AIO_READ_ERROR);
                #else
                    throw Exception("AIO failed to read file " + path + BIN_FILE_EXT + ". " +
                        "request_id= " + std::to_string(request.aio_data) + "/ " + std::to_string(requests.size()) +
                        ", aio_nbytes=" + std::to_string(request.aio_nbytes) + ", aio_offset=" + std::to_string(request.aio_offset) +
                        ", returned=" + std::to_string(events[i].res) + ", errno=" + std::to_string(errno), ErrorCodes::AIO_READ_ERROR);
                #endif
            }
            #if defined(__FreeBSD__)
            const char* buf_ptr = reinterpret_cast<char *>(reinterpret_cast<UInt64>(request.aio.aio_buf));
            #else
            const auto* buf_ptr = reinterpret_cast<char *>(request.aio_buf);
            #endif

            __msan_unpoison(buf_ptr, block_size);
            uint64_t checksum = 0;
            ReadBufferFromMemory buf_special(buf_ptr, block_size);
            readBinary(checksum, buf_special);
            uint64_t calculated_checksum = CityHash_v1_0_2::CityHash64(buf_ptr + BLOCK_CHECKSUM_SIZE_BYTES, block_size - BLOCK_CHECKSUM_SIZE_BYTES);
            if (checksum != calculated_checksum)
            {
                throw Exception("Cache data corrupted. From block = " + std::to_string(checksum) + " calculated = " + std::to_string(calculated_checksum) + ".", ErrorCodes::CORRUPTED_DATA);
            }

            for (const size_t idx : blocks_to_indices[request_id])
            {
                const auto & [file_index, out_index] = index_to_out[idx];
                ReadBufferFromMemory buf(
                        buf_ptr + file_index.getAddressInBlock(),
                        block_size - file_index.getAddressInBlock());
                set(out_index, buf);
            }

            processed[request_id] = true;
        }

        while (to_pop < requests.size() && processed[to_pop])
            ++to_pop;

        /// add new io tasks
        const int new_tasks_count = std::min(read_buffer_size - (to_push - to_pop), requests.size() - to_push);

        int pushed = 0;
        while (new_tasks_count > 0 && (pushed = io_submit(aio_context.ctx, new_tasks_count, &pointers[to_push])) <= 0)
        {
            if (errno != EINTR)
                throwFromErrno("io_submit: Failed to submit a request for asynchronous IO", ErrorCodes::CANNOT_IO_SUBMIT);
        }
        to_push += pushed;
    }
}

void SSDComplexKeyCachePartition::clearOldestBlocks()
{
    // write_buffer_size, because we need to erase the whole buffer.
    Memory read_buffer_memory(block_size * write_buffer_size, BUFFER_ALIGNMENT);

    iocb request{};
#if defined(__FreeBSD__)
    request.aio.aio_lio_opcode = LIO_READ;
    request.aio.aio_fildes = fd;
    request.aio.aio_buf = reinterpret_cast<volatile void *>(reinterpret_cast<UInt64>(read_buffer_memory.data()));
    request.aio.aio_nbytes = block_size * write_buffer_size;
    request.aio.aio_offset = (current_file_block_id % max_size) * block_size;
    request.aio_data = 0;
#else
    request.aio_lio_opcode = IOCB_CMD_PREAD;
    request.aio_fildes = fd;
    request.aio_buf = reinterpret_cast<UInt64>(read_buffer_memory.data());
    request.aio_nbytes = block_size * write_buffer_size;
    request.aio_offset = (current_file_block_id % max_size) * block_size;
    request.aio_data = 0;
#endif

    {
        iocb* request_ptr = &request;
        io_event event{};
        AIOContext aio_context(1);

        while (io_submit(aio_context.ctx, 1, &request_ptr) != 1)
            if (errno != EINTR)
                throwFromErrno("io_submit: Failed to submit a request for asynchronous IO", ErrorCodes::CANNOT_IO_SUBMIT);

        while (io_getevents(aio_context.ctx, 1, 1, &event, nullptr) != 1)
            if (errno != EINTR)
                throwFromErrno("io_getevents: Failed to get an event for asynchronous IO", ErrorCodes::CANNOT_IO_GETEVENTS);

#if defined(__FreeBSD__)
        if (aio_return(reinterpret_cast<struct aiocb *>(event.udata)) != static_cast<ssize_t>(request.aio.aio_nbytes))
            throw Exception("GC: AIO failed to read file " + path + BIN_FILE_EXT + ".", ErrorCodes::AIO_READ_ERROR);
#else
        if (event.res != static_cast<ssize_t>(request.aio_nbytes))
            throw Exception("GC: AIO failed to read file " + path + BIN_FILE_EXT + ". " +
                "aio_nbytes=" + std::to_string(request.aio_nbytes) +
                ", returned=" + std::to_string(event.res) + ".", ErrorCodes::AIO_READ_ERROR);
#endif
        __msan_unpoison(read_buffer_memory.data(), read_buffer_memory.size());
    }

    TemporalComplexKeysPool tmp_keys_pool;
    KeyRefs keys;

    for (size_t i = 0; i < write_buffer_size; ++i)
    {
        ReadBufferFromMemory read_buffer(read_buffer_memory.data() + i * block_size, block_size);

        uint64_t checksum = 0;
        readBinary(checksum, read_buffer);
        uint64_t calculated_checksum = CityHash_v1_0_2::CityHash64(read_buffer_memory.data() + i * block_size + BLOCK_CHECKSUM_SIZE_BYTES, block_size - BLOCK_CHECKSUM_SIZE_BYTES);
        if (checksum != calculated_checksum)
        {
            throw Exception("Cache data corrupted. From block = " + std::to_string(checksum) + " calculated = " + std::to_string(calculated_checksum) + ".", ErrorCodes::CORRUPTED_DATA);
        }

        uint32_t keys_in_current_block = 0;
        readBinary(keys_in_current_block, read_buffer);

        for (uint32_t j = 0; j < keys_in_current_block; ++j)
        {
            keys.emplace_back();
            tmp_keys_pool.readKey(keys.back(), read_buffer);

            Metadata metadata;
            readBinary(metadata.data, read_buffer);

            if (!metadata.isDefault())
            {
                for (const auto & attr : attributes_structure)
                {
                    switch (attr)
                    {
            #define DISPATCH(TYPE) \
                    case AttributeUnderlyingType::ut##TYPE: \
                        read_buffer.ignore(sizeof(TYPE)); \
                        break;

                        DISPATCH(UInt8)
                        DISPATCH(UInt16)
                        DISPATCH(UInt32)
                        DISPATCH(UInt64)
                        DISPATCH(UInt128)
                        DISPATCH(Int8)
                        DISPATCH(Int16)
                        DISPATCH(Int32)
                        DISPATCH(Int64)
                        DISPATCH(Decimal32)
                        DISPATCH(Decimal64)
                        DISPATCH(Decimal128)
                        DISPATCH(Float32)
                        DISPATCH(Float64)
            #undef DISPATCH

                    case AttributeUnderlyingType::utString:
                        {
                            size_t size = 0;
                            readVarUInt(size, read_buffer);
                            read_buffer.ignore(size);
                        }
                        break;
                    }
                }
            }
        }
    }

    const size_t start_block = current_file_block_id % max_size;
    const size_t finish_block = start_block + write_buffer_size;
    for (const auto& key : keys)
    {
        Index index;
        if (key_to_index.get(key, index))
        {
            size_t block_id = index.getBlockId();
            if (start_block <= block_id && block_id < finish_block)
                key_to_index.erase(key);
        }
    }
}

void SSDComplexKeyCachePartition::ignoreFromBufferToAttributeIndex(const size_t attribute_index, ReadBuffer & buf) const
{
    for (size_t i = 0; i < attribute_index; ++i)
    {
        switch (attributes_structure[i])
        {
#define DISPATCH(TYPE) \
        case AttributeUnderlyingType::ut##TYPE: \
            buf.ignore(sizeof(TYPE)); \
            break;

            DISPATCH(UInt8)
            DISPATCH(UInt16)
            DISPATCH(UInt32)
            DISPATCH(UInt64)
            DISPATCH(UInt128)
            DISPATCH(Int8)
            DISPATCH(Int16)
            DISPATCH(Int32)
            DISPATCH(Int64)
            DISPATCH(Decimal32)
            DISPATCH(Decimal64)
            DISPATCH(Decimal128)
            DISPATCH(Float32)
            DISPATCH(Float64)
#undef DISPATCH

        case AttributeUnderlyingType::utString:
            {
                size_t size = 0;
                readVarUInt(size, buf);
                buf.ignore(size);
            }
            break;
        }
    }
}

size_t SSDComplexKeyCachePartition::getId() const
{
    return file_id;
}

double SSDComplexKeyCachePartition::getLoadFactor() const
{
    std::shared_lock lock(rw_lock);
    return static_cast<double>(current_file_block_id) / max_size;
}

size_t SSDComplexKeyCachePartition::getElementCount() const
{
    std::shared_lock lock(rw_lock);
    return key_to_index.size();
}

size_t SSDComplexKeyCachePartition::getBytesAllocated() const
{
    std::shared_lock lock(rw_lock);
    return 16.5 * key_to_index.capacity() + keys_pool.size() +
        (keys_buffer_pool ? keys_buffer_pool->size() : 0) + (memory ? memory->size() : 0);
}

void SSDComplexKeyCachePartition::remove()
{
    std::unique_lock lock(rw_lock);
    std::filesystem::remove(std::filesystem::path(path + BIN_FILE_EXT));
}

SSDComplexKeyCacheStorage::SSDComplexKeyCacheStorage(
        const AttributeTypes & attributes_structure_,
        const std::string & path_,
        const size_t max_partitions_count_,
        const size_t file_size_,
        const size_t block_size_,
        const size_t read_buffer_size_,
        const size_t write_buffer_size_,
        const size_t max_stored_keys_)
    : attributes_structure(attributes_structure_)
    , path(path_)
    , max_partitions_count(max_partitions_count_)
    , file_size(file_size_)
    , block_size(block_size_)
    , read_buffer_size(read_buffer_size_)
    , write_buffer_size(write_buffer_size_)
    , max_stored_keys(max_stored_keys_)
    , log(&Poco::Logger::get("SSDComplexKeyCacheStorage"))
{
}

SSDComplexKeyCacheStorage::~SSDComplexKeyCacheStorage()
{
    std::unique_lock lock(rw_lock);
    partition_delete_queue.splice(std::end(partition_delete_queue), partitions);
    collectGarbage();
}

template <typename Out, typename GetDefault>
void SSDComplexKeyCacheStorage::getValue(
    const size_t attribute_index, const Columns & key_columns, const DataTypes & key_types,
    ResultArrayType<Out> & out, std::unordered_map<KeyRef, std::vector<size_t>> & not_found,
    TemporalComplexKeysPool & not_found_pool,
    GetDefault & get_default, std::chrono::system_clock::time_point now) const
{
    size_t n = key_columns.front()->size();
    std::vector<bool> found(n, false);

    {
        std::shared_lock lock(rw_lock);
        for (const auto & partition : partitions)
            partition->getValue<Out>(attribute_index, key_columns, key_types, out, found, get_default, now);
    }

    size_t count_not_found = 0;
    StringRefs tmp_refs(key_columns.size());
    for (size_t i = 0; i < n; ++i)
    {
        if (!found[i])
        {
            auto key = not_found_pool.allocKey(i, key_columns, tmp_refs);
            not_found[key].push_back(i);
            ++count_not_found;
        }
    }

    query_count.fetch_add(n, std::memory_order_relaxed);
    hit_count.fetch_add(n - count_not_found, std::memory_order_release);
}

void SSDComplexKeyCacheStorage::getString(
    const size_t attribute_index, const Columns & key_columns, const DataTypes & key_types,
    StringRefs & refs, ArenaWithFreeLists & arena,
    std::unordered_map<KeyRef, std::vector<size_t>> & not_found,
    TemporalComplexKeysPool & not_found_pool,
    std::vector<size_t> & default_ids, std::chrono::system_clock::time_point now) const
{
    size_t n = key_columns.front()->size();
    std::vector<bool> found(n, false);

    {
        std::shared_lock lock(rw_lock);
        for (const auto & partition : partitions)
            partition->getString(attribute_index, key_columns, key_types, refs, arena, found, default_ids, now);
    }

    size_t count_not_found = 0;
    StringRefs tmp_refs(key_columns.size());
    for (size_t i = 0; i < n; ++i)
    {
        if (!found[i])
        {
            auto key = not_found_pool.allocKey(i, key_columns, tmp_refs);
            not_found[key].push_back(i);
            ++count_not_found;
        }
    }

    query_count.fetch_add(n, std::memory_order_relaxed);
    hit_count.fetch_add(n - count_not_found, std::memory_order_release);
}

void SSDComplexKeyCacheStorage::has(
    const Columns & key_columns, const DataTypes & key_types, ResultArrayType<UInt8> & out,
    std::unordered_map<KeyRef, std::vector<size_t>> & not_found,
    TemporalComplexKeysPool & not_found_pool, std::chrono::system_clock::time_point now) const
{
    size_t n = key_columns.front()->size();
    for (size_t i = 0; i < n; ++i)
        out[i] = HAS_NOT_FOUND;
    std::vector<bool> found(n, false);

    {
        std::shared_lock lock(rw_lock);
        for (const auto & partition : partitions)
            partition->has(key_columns, key_types, out, found, now);
    }

    size_t count_not_found = 0;
    StringRefs tmp_refs(key_columns.size());
    for (size_t i = 0; i < n; ++i)
    {
        if (out[i] == HAS_NOT_FOUND)
        {
            auto key = not_found_pool.allocKey(i, key_columns, tmp_refs);
            not_found[key].push_back(i);
            ++count_not_found;
        }
    }

    query_count.fetch_add(n, std::memory_order_relaxed);
    hit_count.fetch_add(n - count_not_found, std::memory_order_release);
}

namespace
{
SSDComplexKeyCachePartition::Attributes createAttributesFromBlock(
        const Block & block, const size_t begin_column, const std::vector<AttributeUnderlyingType> & structure)
{
    SSDComplexKeyCachePartition::Attributes attributes;

    const auto columns = block.getColumns();
    for (size_t i = 0; i < structure.size(); ++i)
    {
        const auto & column = columns[i + begin_column];
        switch (structure[i])
        {
#define DISPATCH(TYPE) \
        case AttributeUnderlyingType::ut##TYPE: \
            { \
                SSDComplexKeyCachePartition::Attribute::Container<TYPE> values(column->size()); \
                memcpy(&values[0], column->getRawData().data, sizeof(TYPE) * values.size()); \
                attributes.emplace_back(); \
                attributes.back().type = structure[i]; \
                attributes.back().values = std::move(values); \
            } \
            break;

            DISPATCH(UInt8)
            DISPATCH(UInt16)
            DISPATCH(UInt32)
            DISPATCH(UInt64)
            DISPATCH(UInt128)
            DISPATCH(Int8)
            DISPATCH(Int16)
            DISPATCH(Int32)
            DISPATCH(Int64)
            DISPATCH(Decimal32)
            DISPATCH(Decimal64)
            DISPATCH(Decimal128)
            DISPATCH(Float32)
            DISPATCH(Float64)
#undef DISPATCH

        case AttributeUnderlyingType::utString:
            {
                attributes.emplace_back();
                SSDComplexKeyCachePartition::Attribute::Container<String> values(column->size());
                for (size_t j = 0; j < column->size(); ++j)
                {
                    const auto ref = column->getDataAt(j);
                    values[j].resize(ref.size);
                    memcpy(values[j].data(), ref.data, ref.size);
                }
                attributes.back().type = structure[i];
                attributes.back().values = std::move(values);
            }
            break;
        }
    }

    return attributes;
}
}

template <typename PresentIdHandler, typename AbsentIdHandler>
void SSDComplexKeyCacheStorage::update(
    DictionarySourcePtr & source_ptr,
    const Columns & key_columns,
    const DataTypes & key_types,
    const KeyRefs & required_keys,
    const std::vector<size_t> & required_rows,
    TemporalComplexKeysPool & tmp_keys_pool,
    PresentIdHandler && on_updated,
    AbsentIdHandler && on_key_not_found,
    const DictionaryLifetime lifetime)
{
    assert(key_columns.size() == key_types.size());

    auto append_block = [&key_types, this](
        const Columns & new_keys,
        const SSDComplexKeyCachePartition::Attributes & new_attributes,
        const PaddedPODArray<SSDComplexKeyCachePartition::Metadata> & metadata)
    {
        size_t inserted = 0;
        while (inserted < metadata.size())
        {
            if (!partitions.empty())
                inserted += partitions.front()->appendBlock(
                    new_keys, key_types, new_attributes, metadata, inserted);
            if (inserted < metadata.size())
            {
                partitions.emplace_front(std::make_unique<SSDComplexKeyCachePartition>(
                    AttributeUnderlyingType::utUInt64, attributes_structure, path,
                    (partitions.empty() ? 0 : partitions.front()->getId() + 1),
                    file_size, block_size, read_buffer_size, write_buffer_size, max_stored_keys));
            }
        }

        collectGarbage();
    };

    CurrentMetrics::Increment metric_increment{CurrentMetrics::DictCacheRequests};
    ProfileEvents::increment(ProfileEvents::DictCacheKeysRequested, required_keys.size());

    std::unordered_map<KeyRef, UInt8> remaining_keys{required_keys.size()};
    for (const auto & key : required_keys)
        remaining_keys.insert({key, 0});

    const auto now = std::chrono::system_clock::now();

    {
        const auto keys_size = key_columns.size();
        StringRefs keys(keys_size);

        const ProfilingScopedWriteRWLock write_lock{rw_lock, ProfileEvents::DictCacheLockWriteNs};

        if (now > backoff_end_time)
        {
            try
            {
                if (update_error_count)
                {
                    /// Recover after error: we have to clone the source here because
                    /// it could keep connections which should be reset after error.
                    source_ptr = source_ptr->clone();
                }

                Stopwatch watch;
                auto stream = source_ptr->loadKeys(key_columns, required_rows);
                stream->readPrefix();

                while (const auto block = stream->read())
                {
                    const auto new_key_columns = ext::map<Columns>(
                        ext::range(0, keys_size),
                        [&](const size_t attribute_idx) { return block.safeGetByPosition(attribute_idx).column; });

                    const auto new_attributes = createAttributesFromBlock(block, keys_size, attributes_structure);

                    const auto rows_num = block.rows();
                    PaddedPODArray<SSDComplexKeyCachePartition::Metadata> metadata(rows_num);

                    for (const auto i : ext::range(0, rows_num))
                    {
                        auto key = tmp_keys_pool.allocKey(i, new_key_columns, keys);
                        //SCOPE_EXIT(tmp_keys_pool.rollback(key));

                        std::uniform_int_distribution<UInt64> distribution{lifetime.min_sec, lifetime.max_sec};
                        metadata[i].setExpiresAt(now + std::chrono::seconds(distribution(rnd_engine)));
                        /// mark corresponding id as found
                        on_updated(key, i, new_attributes);
                        remaining_keys[key] = 1;
                    }

                    append_block(new_key_columns, new_attributes, metadata);
                }

                stream->readSuffix();

                update_error_count = 0;
                last_update_exception = std::exception_ptr{};
                backoff_end_time = std::chrono::system_clock::time_point{};

                ProfileEvents::increment(ProfileEvents::DictCacheRequestTimeNs, watch.elapsed());
            }
            catch (...)
            {
                ++update_error_count;
                last_update_exception = std::current_exception();
                backoff_end_time = now + std::chrono::seconds(calculateDurationWithBackoff(rnd_engine, update_error_count));

                tryLogException(last_update_exception, log,
                        "Could not update ssd cache dictionary, next update is scheduled at " + ext::to_string(backoff_end_time));
            }
        }
    }

    auto append_defaults = [this](
        const KeyRefs & new_keys,
        const PaddedPODArray<SSDComplexKeyCachePartition::Metadata> & metadata)
    {
        size_t inserted = 0;
        while (inserted < metadata.size())
        {
            if (!partitions.empty())
                inserted += partitions.front()->appendDefaults(
                    new_keys, metadata, inserted);
            if (inserted < metadata.size())
            {
                partitions.emplace_front(std::make_unique<SSDComplexKeyCachePartition>(
                        AttributeUnderlyingType::utUInt64, attributes_structure, path,
                        (partitions.empty() ? 0 : partitions.front()->getId() + 1),
                        file_size, block_size, read_buffer_size, write_buffer_size, max_stored_keys));
            }
        }

        collectGarbage();
    };

    size_t not_found_num = 0, found_num = 0;
    /// Check which ids have not been found and require setting null_value
    KeyRefs default_keys;

    PaddedPODArray<SSDComplexKeyCachePartition::Metadata> metadata;
    {
        const ProfilingScopedWriteRWLock write_lock{rw_lock, ProfileEvents::DictCacheLockWriteNs};

        for (const auto & key_found_pair : remaining_keys)
        {
            if (key_found_pair.second)
            {
                ++found_num;
                continue;
            }
            ++not_found_num;

            const auto key = key_found_pair.first;

            if (update_error_count)
            {
                /// TODO: use old values.

                // We don't have expired data for that `id` so all we can do is
                // to rethrow `last_exception`. We might have to throw the same
                // exception for different callers of dictGet() in different
                // threads, which might then modify the exception object, so we
                // have to throw a copy.
                try
                {
                    std::rethrow_exception(last_update_exception);
                }
                catch (...)
                {
                    throw DB::Exception(ErrorCodes::CACHE_DICTIONARY_UPDATE_FAIL,
                        "Update failed for dictionary '{}': {}",
                        getPath(),
                        getCurrentExceptionMessage(true /*with stack trace*/,
                            true /*check embedded stack trace*/));
                }
            }

            std::uniform_int_distribution<UInt64> distribution{lifetime.min_sec, lifetime.max_sec};
            metadata.emplace_back();
            metadata.back().setExpiresAt(now + std::chrono::seconds(distribution(rnd_engine)));
            metadata.back().setDefault();

            default_keys.push_back(key);

            /// inform caller that the cell has not been found
            on_key_not_found(key);
        }

        if (not_found_num)
            append_defaults(default_keys, metadata);
    }

    ProfileEvents::increment(ProfileEvents::DictCacheKeysRequestedMiss, not_found_num);
    ProfileEvents::increment(ProfileEvents::DictCacheKeysRequestedFound, found_num);
    ProfileEvents::increment(ProfileEvents::DictCacheRequests);
}

double SSDComplexKeyCacheStorage::getLoadFactor() const
{
    double result = 0;
    std::shared_lock lock(rw_lock);
    for (const auto & partition : partitions)
        result += partition->getLoadFactor();
    return result / partitions.size();
}

size_t SSDComplexKeyCacheStorage::getElementCount() const
{
    size_t result = 0;
    std::shared_lock lock(rw_lock);
    for (const auto & partition : partitions)
        result += partition->getElementCount();
    return result;
}

void SSDComplexKeyCacheStorage::collectGarbage()
{
    // add partitions to queue
    while (partitions.size() > max_partitions_count)
    {
        partition_delete_queue.splice(std::end(partition_delete_queue), partitions, std::prev(std::end(partitions)));
    }

    // drop unused partitions
    while (!partition_delete_queue.empty() && partition_delete_queue.front().use_count() == 1)
    {
        partition_delete_queue.front()->remove();
        partition_delete_queue.pop_front();
    }
}

SSDComplexKeyCacheDictionary::SSDComplexKeyCacheDictionary(
    const StorageID & dict_id_,
    const DictionaryStructure & dict_struct_,
    DictionarySourcePtr source_ptr_,
    const DictionaryLifetime dict_lifetime_,
    const std::string & path_,
    const size_t max_partitions_count_,
    const size_t file_size_,
    const size_t block_size_,
    const size_t read_buffer_size_,
    const size_t write_buffer_size_,
    const size_t max_stored_keys_)
    : IDictionaryBase(dict_id_)
    , dict_struct(dict_struct_)
    , source_ptr(std::move(source_ptr_))
    , dict_lifetime(dict_lifetime_)
    , path(path_)
    , max_partitions_count(max_partitions_count_)
    , file_size(file_size_)
    , block_size(block_size_)
    , read_buffer_size(read_buffer_size_)
    , write_buffer_size(write_buffer_size_)
    , max_stored_keys(max_stored_keys_)
    , storage(ext::map<std::vector>(dict_struct.attributes, [](const auto & attribute) { return attribute.underlying_type; }),
            path, max_partitions_count, file_size, block_size, read_buffer_size, write_buffer_size, max_stored_keys)
    , log(&Poco::Logger::get("SSDComplexKeyCacheDictionary"))
{
    LOG_INFO(log, "Using storage path '{}'.", path);
    if (!this->source_ptr->supportsSelectiveLoad())
        throw Exception{name + ": source cannot be used with CacheDictionary", ErrorCodes::UNSUPPORTED_METHOD};

    createAttributes();
}

#define DECLARE(TYPE) \
    void SSDComplexKeyCacheDictionary::get##TYPE( \
        const std::string & attribute_name, \
        const Columns & key_columns, \
        const DataTypes & key_types, \
        ResultArrayType<TYPE> & out) const \
    { \
        const auto index = getAttributeIndex(attribute_name); \
        checkAttributeType(this, attribute_name, dict_struct.attributes[index].underlying_type, AttributeUnderlyingType::ut##TYPE); \
        const auto null_value = std::get<TYPE>(null_values[index]); /* NOLINT */ \
        getItemsNumberImpl<TYPE, TYPE>(index, key_columns, key_types, out, [&](const size_t) { return null_value; }); /* NOLINT */ \
    }

    DECLARE(UInt8)
    DECLARE(UInt16)
    DECLARE(UInt32)
    DECLARE(UInt64)
    DECLARE(UInt128)
    DECLARE(Int8)
    DECLARE(Int16)
    DECLARE(Int32)
    DECLARE(Int64)
    DECLARE(Float32)
    DECLARE(Float64)
    DECLARE(Decimal32)
    DECLARE(Decimal64)
    DECLARE(Decimal128)
#undef DECLARE

#define DECLARE(TYPE) \
    void SSDComplexKeyCacheDictionary::get##TYPE( \
        const std::string & attribute_name, \
        const Columns & key_columns, \
        const DataTypes & key_types, \
        const PaddedPODArray<TYPE> & def, \
        ResultArrayType<TYPE> & out) const \
    { \
        const auto index = getAttributeIndex(attribute_name); \
        checkAttributeType(this, attribute_name, dict_struct.attributes[index].underlying_type, AttributeUnderlyingType::ut##TYPE); \
        getItemsNumberImpl<TYPE, TYPE>(index, key_columns, key_types, out, [&](const size_t row) { return def[row]; }); /* NOLINT */ \
    }
    DECLARE(UInt8)
    DECLARE(UInt16)
    DECLARE(UInt32)
    DECLARE(UInt64)
    DECLARE(UInt128)
    DECLARE(Int8)
    DECLARE(Int16)
    DECLARE(Int32)
    DECLARE(Int64)
    DECLARE(Float32)
    DECLARE(Float64)
    DECLARE(Decimal32)
    DECLARE(Decimal64)
    DECLARE(Decimal128)
#undef DECLARE

#define DECLARE(TYPE) \
    void SSDComplexKeyCacheDictionary::get##TYPE( \
        const std::string & attribute_name, \
        const Columns & key_columns, \
        const DataTypes & key_types, \
        const TYPE def, \
        ResultArrayType<TYPE> & out) const \
    { \
        const auto index = getAttributeIndex(attribute_name); \
        checkAttributeType(this, attribute_name, dict_struct.attributes[index].underlying_type, AttributeUnderlyingType::ut##TYPE); \
        getItemsNumberImpl<TYPE, TYPE>(index, key_columns, key_types, out, [&](const size_t) { return def; }); /* NOLINT */ \
    }
    DECLARE(UInt8)
    DECLARE(UInt16)
    DECLARE(UInt32)
    DECLARE(UInt64)
    DECLARE(UInt128)
    DECLARE(Int8)
    DECLARE(Int16)
    DECLARE(Int32)
    DECLARE(Int64)
    DECLARE(Float32)
    DECLARE(Float64)
    DECLARE(Decimal32)
    DECLARE(Decimal64)
    DECLARE(Decimal128)
#undef DECLARE

template <typename AttributeType, typename OutputType, typename DefaultGetter>
void SSDComplexKeyCacheDictionary::getItemsNumberImpl(
    const size_t attribute_index,
    const Columns & key_columns, const DataTypes & key_types,
    ResultArrayType<OutputType> & out, DefaultGetter && get_default) const
{
    assert(dict_struct.key);
    assert(key_columns.size() == key_types.size());

    dict_struct.validateKeyTypes(key_types);

    const auto now = std::chrono::system_clock::now();

    TemporalComplexKeysPool not_found_pool;
    std::unordered_map<KeyRef, std::vector<size_t>> not_found_keys;
    storage.getValue<OutputType>(attribute_index, key_columns, key_types, out, not_found_keys, not_found_pool, get_default, now);
    if (not_found_keys.empty())
        return;

    std::vector<KeyRef> required_keys(not_found_keys.size());
    std::transform(std::begin(not_found_keys), std::end(not_found_keys), std::begin(required_keys), [](const auto & pair) { return pair.first; });
    std::vector<size_t> required_rows;
    required_rows.reserve(required_keys.size());
    for (const auto & key_ref : required_keys)
        required_rows.push_back(not_found_keys[key_ref].front());

    TemporalComplexKeysPool tmp_keys_pool;
    storage.update(
            source_ptr,
            key_columns,
            key_types,
            required_keys,
            required_rows,
            tmp_keys_pool,
            [&](const auto key, const auto row, const auto & new_attributes)
            {
                for (const size_t out_row : not_found_keys[key])
                    out[out_row] = std::get<SSDComplexKeyCachePartition::Attribute::Container<OutputType>>(new_attributes[attribute_index].values)[row];
            },
            [&](const auto key)
            {
                for (const size_t row : not_found_keys[key])
                    out[row] = get_default(row);
            },
            getLifetime());
}

void SSDComplexKeyCacheDictionary::getString(
    const std::string & attribute_name,
    const Columns & key_columns, const DataTypes & key_types, ColumnString * out) const
{
    const auto index = getAttributeIndex(attribute_name);
    checkAttributeType(this, attribute_name, dict_struct.attributes[index].underlying_type, AttributeUnderlyingType::utString);

    const auto null_value = StringRef{std::get<String>(null_values[index])};

    getItemsStringImpl(index, key_columns, key_types, out, [&](const size_t) { return null_value; });
}

void SSDComplexKeyCacheDictionary::getString(
        const std::string & attribute_name,
        const Columns & key_columns, const DataTypes & key_types,
        const ColumnString * const def, ColumnString * const out) const
{
    const auto index = getAttributeIndex(attribute_name);
    checkAttributeType(this, attribute_name, dict_struct.attributes[index].underlying_type, AttributeUnderlyingType::utString);

    getItemsStringImpl(index, key_columns, key_types, out, [&](const size_t row) { return def->getDataAt(row); });
}

void SSDComplexKeyCacheDictionary::getString(
        const std::string & attribute_name,
        const Columns & key_columns,
        const DataTypes & key_types,
        const String & def,
        ColumnString * const out) const
{
    const auto index = getAttributeIndex(attribute_name);
    checkAttributeType(this, attribute_name, dict_struct.attributes[index].underlying_type, AttributeUnderlyingType::utString);

    getItemsStringImpl(index, key_columns, key_types, out, [&](const size_t) { return StringRef{def}; });
}

template <typename DefaultGetter>
void SSDComplexKeyCacheDictionary::getItemsStringImpl(
    const size_t attribute_index,
    const Columns & key_columns,
    const DataTypes & key_types,
    ColumnString * out,
    DefaultGetter && get_default) const
{
    dict_struct.validateKeyTypes(key_types);

    const auto now = std::chrono::system_clock::now();

    TemporalComplexKeysPool not_found_pool;
    std::unordered_map<KeyRef, std::vector<size_t>> not_found_keys;

    const size_t n = key_columns.front()->size();

    StringRefs refs(n);
    ArenaWithFreeLists string_arena;
    std::vector<size_t> default_rows;
    storage.getString(
        attribute_index, key_columns, key_types,
        refs, string_arena, not_found_keys, not_found_pool, default_rows, now);
    std::sort(std::begin(default_rows), std::end(default_rows));

    if (not_found_keys.empty())
    {
        size_t default_index = 0;
        for (size_t row = 0; row < n; ++row)
        {
            if (unlikely(default_index != default_rows.size() && default_rows[default_index] == row))
            {
                auto to_insert = get_default(row);
                out->insertData(to_insert.data, to_insert.size);
                ++default_index;
            }
            else
                out->insertData(refs[row].data, refs[row].size);
        }
        return;
    }

    std::vector<KeyRef> required_keys(not_found_keys.size());
    std::transform(std::begin(not_found_keys), std::end(not_found_keys), std::begin(required_keys), [](const auto & pair) { return pair.first; });

    std::unordered_map<KeyRef, String> update_result;

    std::vector<size_t> required_rows;
    required_rows.reserve(required_keys.size());
    for (const auto & key_ref : required_keys)
        required_rows.push_back(not_found_keys[key_ref].front());

    TemporalComplexKeysPool tmp_keys_pool;
    storage.update(
            source_ptr,
            key_columns,
            key_types,
            required_keys,
            required_rows,
            tmp_keys_pool,
            [&](const auto key, const auto row, const auto & new_attributes)
            {
                update_result[key] = std::get<SSDComplexKeyCachePartition::Attribute::Container<String>>(new_attributes[attribute_index].values)[row];
            },
            [&](const auto) {},
            getLifetime());

    StringRefs tmp_refs(key_columns.size());
    size_t default_index = 0;
    for (size_t row = 0; row < n; ++row)
    {
        const auto key = tmp_keys_pool.allocKey(row, key_columns, tmp_refs);
        SCOPE_EXIT(tmp_keys_pool.rollback(key));
        if (unlikely(default_index != default_rows.size() && default_rows[default_index] == row))
        {
            auto to_insert = get_default(row);
            out->insertData(to_insert.data, to_insert.size);
            ++default_index;
        }
        else if (auto it = not_found_keys.find(key); it == std::end(not_found_keys))
        {
            out->insertData(refs[row].data, refs[row].size);
        }
        else if (auto it_update = update_result.find(key); it_update != std::end(update_result))
        {
            out->insertData(it_update->second.data(), it_update->second.size());
        }
        else
        {
            auto to_insert = get_default(row);
            out->insertData(to_insert.data, to_insert.size);
        }
    }
}

void SSDComplexKeyCacheDictionary::has(
    const Columns & key_columns,
    const DataTypes & key_types,
    PaddedPODArray<UInt8> & out) const
{
    const auto now = std::chrono::system_clock::now();

    std::unordered_map<KeyRef, std::vector<size_t>> not_found_keys;
    TemporalComplexKeysPool not_found_pool;
    storage.has(key_columns, key_types, out, not_found_keys, not_found_pool, now);
    if (not_found_keys.empty())
        return;

    std::vector<KeyRef> required_keys(not_found_keys.size());
    std::transform(std::begin(not_found_keys), std::end(not_found_keys), std::begin(required_keys), [](const auto & pair) { return pair.first; });

    std::vector<size_t> required_rows;
    required_rows.reserve(required_keys.size());
    for (const auto & key_ref : required_keys)
        required_rows.push_back(not_found_keys[key_ref].front());

    TemporalComplexKeysPool tmp_keys_pool;
    storage.update(
            source_ptr,
            key_columns,
            key_types,
            required_keys,
            required_rows,
            tmp_keys_pool,
            [&](const auto key, const auto, const auto &)
            {
                for (const size_t out_row : not_found_keys[key])
                    out[out_row] = true;
            },
            [&](const auto key)
            {
                for (const size_t row : not_found_keys[key])
                    out[row] = false;
            },
            getLifetime());
}

BlockInputStreamPtr SSDComplexKeyCacheDictionary::getBlockInputStream(
    const Names & /* column_names */, size_t /* max_block_size*/) const
{
    throw DB::Exception("Method not supported.", ErrorCodes::NOT_IMPLEMENTED);
}

size_t SSDComplexKeyCacheDictionary::getAttributeIndex(const std::string & attr_name) const
{
    auto it = attribute_index_by_name.find(attr_name);
    if (it == std::end(attribute_index_by_name))
        throw  Exception{"Attribute `" + name + "` does not exist.", ErrorCodes::BAD_ARGUMENTS};
    return it->second;
}

template <typename T>
AttributeValueVariant SSDComplexKeyCacheDictionary::createAttributeNullValueWithTypeImpl(const Field & null_value)
{
    AttributeValueVariant var_null_value = static_cast<T>(null_value.get<NearestFieldType<T>>());
    bytes_allocated += sizeof(T);
    return var_null_value;
}

template <>
AttributeValueVariant SSDComplexKeyCacheDictionary::createAttributeNullValueWithTypeImpl<String>(const Field & null_value)
{
    AttributeValueVariant var_null_value = null_value.get<String>();
    bytes_allocated += sizeof(StringRef);
    return var_null_value;
}

AttributeValueVariant SSDComplexKeyCacheDictionary::createAttributeNullValueWithType(const AttributeUnderlyingType type, const Field & null_value)
{
    switch (type)
    {
#define DISPATCH(TYPE) \
case AttributeUnderlyingType::ut##TYPE: \
    return createAttributeNullValueWithTypeImpl<TYPE>(null_value); /* NOLINT */

        DISPATCH(UInt8)
        DISPATCH(UInt16)
        DISPATCH(UInt32)
        DISPATCH(UInt64)
        DISPATCH(UInt128)
        DISPATCH(Int8)
        DISPATCH(Int16)
        DISPATCH(Int32)
        DISPATCH(Int64)
        DISPATCH(Decimal32)
        DISPATCH(Decimal64)
        DISPATCH(Decimal128)
        DISPATCH(Float32)
        DISPATCH(Float64)
        DISPATCH(String)
#undef DISPATCH
    }
    throw Exception{"Unknown attribute type: " + std::to_string(static_cast<int>(type)), ErrorCodes::TYPE_MISMATCH};
}

void SSDComplexKeyCacheDictionary::createAttributes()
{
    null_values.reserve(dict_struct.attributes.size());
    for (size_t i = 0; i < dict_struct.attributes.size(); ++i)
    {
        const auto & attribute = dict_struct.attributes[i];

        attribute_index_by_name.emplace(attribute.name, i);
        null_values.push_back(createAttributeNullValueWithType(attribute.underlying_type, attribute.null_value));

        if (attribute.hierarchical)
            throw Exception{name + ": hierarchical attributes not supported for dictionary of type " + getTypeName(),
                            ErrorCodes::TYPE_MISMATCH};
    }
}

void registerDictionarySSDComplexKeyCache(DictionaryFactory & factory)
{
    auto create_layout = [=](const std::string & name,
                             const DictionaryStructure & dict_struct,
                             const Poco::Util::AbstractConfiguration & config,
                             const std::string & config_prefix,
                             DictionarySourcePtr source_ptr) -> DictionaryPtr
    {
        const auto dict_id = StorageID::fromDictionaryConfig(config, config_prefix);

        if (dict_struct.id)
            throw Exception{"'id' is not supported for dictionary of layout 'complex_key_cache'", ErrorCodes::UNSUPPORTED_METHOD};

        if (dict_struct.range_min || dict_struct.range_max)
            throw Exception{name
                            + ": elements .structure.range_min and .structure.range_max should be defined only "
                              "for a dictionary of layout 'range_hashed'",
                            ErrorCodes::BAD_ARGUMENTS};
        const auto & layout_prefix = config_prefix + ".layout";

        const auto max_partitions_count = config.getInt(layout_prefix + ".complex_key_ssd_cache.max_partitions_count", DEFAULT_PARTITIONS_COUNT);
        if (max_partitions_count <= 0)
            throw Exception{name + ": dictionary of layout 'complex_key_ssd_cache' cannot have 0 (or less) max_partitions_count", ErrorCodes::BAD_ARGUMENTS};

        const auto block_size = config.getInt(layout_prefix + ".complex_key_ssd_cache.block_size", DEFAULT_SSD_BLOCK_SIZE_BYTES);
        if (block_size <= 0)
            throw Exception{name + ": dictionary of layout 'complex_key_ssd_cache' cannot have 0 (or less) block_size", ErrorCodes::BAD_ARGUMENTS};

        const auto file_size = config.getInt64(layout_prefix + ".complex_key_ssd_cache.file_size", DEFAULT_FILE_SIZE_BYTES);
        if (file_size <= 0)
            throw Exception{name + ": dictionary of layout 'complex_key_ssd_cache' cannot have 0 (or less) file_size", ErrorCodes::BAD_ARGUMENTS};
        if (file_size % block_size != 0)
            throw Exception{name + ": file_size must be a multiple of block_size", ErrorCodes::BAD_ARGUMENTS};

        const auto read_buffer_size = config.getInt64(layout_prefix + ".complex_key_ssd_cache.read_buffer_size", DEFAULT_READ_BUFFER_SIZE_BYTES);
        if (read_buffer_size <= 0)
            throw Exception{name + ": dictionary of layout 'complex_key_ssd_cache' cannot have 0 (or less) read_buffer_size", ErrorCodes::BAD_ARGUMENTS};
        if (read_buffer_size % block_size != 0)
            throw Exception{name + ": read_buffer_size must be a multiple of block_size", ErrorCodes::BAD_ARGUMENTS};

        const auto write_buffer_size = config.getInt64(layout_prefix + ".complex_key_ssd_cache.write_buffer_size", DEFAULT_WRITE_BUFFER_SIZE_BYTES);
        if (write_buffer_size <= 0)
            throw Exception{name + ": dictionary of layout 'complex_key_ssd_cache' cannot have 0 (or less) write_buffer_size", ErrorCodes::BAD_ARGUMENTS};
        if (write_buffer_size % block_size != 0)
            throw Exception{name + ": write_buffer_size must be a multiple of block_size", ErrorCodes::BAD_ARGUMENTS};

        auto path = config.getString(layout_prefix + ".complex_key_ssd_cache.path");
        if (path.empty())
            throw Exception{name + ": dictionary of layout 'complex_key_ssd_cache' cannot have empty path",
                            ErrorCodes::BAD_ARGUMENTS};
        if (path.at(0) != '/')
            path = std::filesystem::path{config.getString("path")}.concat(path).string();

        const auto max_stored_keys = config.getInt64(layout_prefix + ".complex_key_ssd_cache.max_stored_keys", DEFAULT_MAX_STORED_KEYS);
        if (max_stored_keys <= 0)
            throw Exception{name + ": dictionary of layout 'complex_key_ssd_cache' cannot have 0 (or less) max_stored_keys", ErrorCodes::BAD_ARGUMENTS};

        const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};
        return std::make_unique<SSDComplexKeyCacheDictionary>(
                dict_id, dict_struct, std::move(source_ptr), dict_lifetime, path,
                max_partitions_count, file_size / block_size, block_size,
                read_buffer_size / block_size, write_buffer_size / block_size,
                max_stored_keys);
    };
    factory.registerLayout("complex_key_ssd_cache", create_layout, true);
}

}

#endif
