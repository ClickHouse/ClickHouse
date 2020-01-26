#include "SSDCacheDictionary.h"

#include <algorithm>
#include <Columns/ColumnsNumber.h>
#include <Common/typeid_cast.h>
#include <Common/ProfileEvents.h>
#include <Common/ProfilingScopedRWLock.h>
#include <Common/MemorySanitizer.h>
#include <DataStreams/IBlockInputStream.h>
#include <Poco/File.h>
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
    extern const int TYPE_MISMATCH;
    extern const int BAD_ARGUMENTS;
    extern const int UNSUPPORTED_METHOD;
    extern const int LOGICAL_ERROR;
    extern const int TOO_SMALL_BUFFER_SIZE;
    extern const int FILE_DOESNT_EXIST;
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_IO_SUBMIT;
    extern const int CANNOT_IO_GETEVENTS;
    extern const int AIO_WRITE_ERROR;
    extern const int CANNOT_FSYNC;
}

namespace
{
    constexpr size_t DEFAULT_SSD_BLOCK_SIZE = DEFAULT_AIO_FILE_BLOCK_SIZE;
    constexpr size_t DEFAULT_FILE_SIZE = 4 * 1024 * 1024 * 1024ULL;
    constexpr size_t DEFAULT_PARTITIONS_COUNT = 16;
    constexpr size_t DEFAULT_READ_BUFFER_SIZE = 16 * DEFAULT_SSD_BLOCK_SIZE;
    constexpr size_t DEFAULT_WRITE_BUFFER_SIZE = DEFAULT_SSD_BLOCK_SIZE;

    constexpr size_t BUFFER_ALIGNMENT = DEFAULT_AIO_FILE_BLOCK_SIZE;

    static constexpr UInt64 KEY_METADATA_EXPIRES_AT_MASK = std::numeric_limits<std::chrono::system_clock::time_point::rep>::max();
    static constexpr UInt64 KEY_METADATA_IS_DEFAULT_MASK = ~KEY_METADATA_EXPIRES_AT_MASK;

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

CachePartition::Metadata::time_point_t CachePartition::Metadata::expiresAt() const
{
    return ext::safe_bit_cast<time_point_t>(data & KEY_METADATA_EXPIRES_AT_MASK);
}
void CachePartition::Metadata::setExpiresAt(const time_point_t & t)
{
    data = ext::safe_bit_cast<time_point_urep_t>(t);
}

bool CachePartition::Metadata::isDefault() const
{
    return (data & KEY_METADATA_IS_DEFAULT_MASK) == KEY_METADATA_IS_DEFAULT_MASK;
}
void CachePartition::Metadata::setDefault()
{
    data |= KEY_METADATA_IS_DEFAULT_MASK;
}

bool CachePartition::Index::inMemory() const
{
    return (index & KEY_IN_MEMORY) == KEY_IN_MEMORY;
}

bool CachePartition::Index::exists() const
{
    return index != NOT_EXISTS;
}

void CachePartition::Index::setNotExists()
{
    index = NOT_EXISTS;
}

void CachePartition::Index::setInMemory(const bool in_memory)
{
    index = (index & ~KEY_IN_MEMORY) | (static_cast<size_t>(in_memory) << KEY_IN_MEMORY_BIT);
}

size_t CachePartition::Index::getAddressInBlock() const
{
    return index & INDEX_IN_BLOCK_MASK;
}

void CachePartition::Index::setAddressInBlock(const size_t address_in_block)
{
    index = (index & ~INDEX_IN_BLOCK_MASK) | address_in_block;
}

size_t CachePartition::Index::getBlockId() const
{
    return (index & BLOCK_INDEX_MASK) >> INDEX_IN_BLOCK_BITS;
}

void CachePartition::Index::setBlockId(const size_t block_id)
{
    index = (index & ~BLOCK_INDEX_MASK) | (block_id << INDEX_IN_BLOCK_BITS);
}

CachePartition::CachePartition(
        const AttributeUnderlyingType & /* key_structure */,
        const std::vector<AttributeUnderlyingType> & attributes_structure_,
        const std::string & dir_path,
        const size_t file_id_,
        const size_t max_size_,
        const size_t block_size_,
        const size_t read_buffer_size_,
        const size_t write_buffer_size_)
    : file_id(file_id_)
    , max_size(max_size_)
    , block_size(block_size_)
    , read_buffer_size(read_buffer_size_)
    , write_buffer_size(write_buffer_size_)
    , path(dir_path + "/" + std::to_string(file_id))
    , attributes_structure(attributes_structure_)
{
    keys_buffer.type = AttributeUnderlyingType::utUInt64;
    keys_buffer.values = CachePartition::Attribute::Container<UInt64>();

    Poco::File directory(dir_path);
    if (!directory.exists())
        directory.createDirectory();

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
        {
            throwFromErrnoWithPath("Cannot preallocate space for the file " + filename, filename, ErrorCodes::CANNOT_ALLOCATE_MEMORY);
        }
    }
}

CachePartition::~CachePartition()
{
    std::unique_lock lock(rw_lock);
    ::close(fd);
}

size_t CachePartition::appendBlock(
        const Attribute & new_keys, const Attributes & new_attributes, const PaddedPODArray<Metadata> & metadata, const size_t begin)
{
    std::unique_lock lock(rw_lock);
    if (current_file_block_id >= max_size)
        return 0;

    if (new_attributes.size() != attributes_structure.size())
        throw Exception{"Wrong columns number in block.", ErrorCodes::BAD_ARGUMENTS};

    const auto & ids = std::get<Attribute::Container<UInt64>>(new_keys.values);
    auto & ids_buffer = std::get<Attribute::Container<UInt64>>(keys_buffer.values);

    if (!memory)
        memory.emplace(block_size, BUFFER_ALIGNMENT);
    if (!write_buffer)
    {
        write_buffer.emplace(memory->data() + current_memory_block_id * block_size, block_size);
        // codec = CompressionCodecFactory::instance().get("NONE", std::nullopt);
        // compressed_buffer.emplace(*write_buffer, codec);
        // hashing_buffer.emplace(*compressed_buffer);
    }

    for (size_t index = begin; index < ids.size();)
    {
        IndexAndMetadata index_and_metadata;
        index_and_metadata.index.setInMemory(true);
        index_and_metadata.index.setBlockId(current_memory_block_id);
        index_and_metadata.index.setAddressInBlock(write_buffer->offset());
        index_and_metadata.metadata = metadata[index];

        bool flushed = false;

        for (const auto & attribute : new_attributes)
        {
            // TODO:: переделать через столбцы + getDataAt
            switch (attribute.type)
            {
#define DISPATCH(TYPE) \
            case AttributeUnderlyingType::ut##TYPE: \
                { \
                    if (sizeof(TYPE) > write_buffer->available()) \
                    { \
                        write_buffer.reset(); \
                        if (++current_memory_block_id == write_buffer_size) \
                            flush(); \
                        flushed = true; \
                        continue; \
                    } \
                    else \
                    { \
                        const auto & values = std::get<Attribute::Container<TYPE>>(attribute.values); \
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
                /*{
                    LOG_DEBUG(&Poco::Logger::get("kek"), "string write");
                    const auto & value = std::get<Attribute::Container<String>>(attribute.values)[index];
                    if (sizeof(UInt64) + value.size() > write_buffer->available())
                    {
                        write_buffer.reset();
                        if (++current_memory_block_id == write_buffer_size)
                            flush();
                        flushed = true;
                        continue;
                    }
                    else
                    {
                        writeStringBinary(value, *write_buffer);
                    }
                }*/
                break;
            }
        }

        if (!flushed)
        {
            key_to_index_and_metadata[ids[index]] = index_and_metadata;
            ids_buffer.push_back(ids[index]);
            ++index;
        }
        else if (current_file_block_id < max_size) // next block in write buffer or flushed to ssd
        {
            write_buffer.emplace(memory->data() + current_memory_block_id * block_size, block_size);
        }
        else // flushed to ssd, end of current file
        {
            memory.reset();
            return index - begin;
        }
    }
    return ids.size() - begin;
}

void CachePartition::flush()
{
    const auto & ids = std::get<Attribute::Container<UInt64>>(keys_buffer.values);
    if (ids.empty())
        return;

    Poco::Logger::get("paritiiton").information("@@@@@@@@@@@@@@@@@@@@ FLUSH!!! " + std::to_string(file_id) + " block: " + std::to_string(current_file_block_id));

    AIOContext aio_context{1};

    iocb write_request{};
    iocb * write_request_ptr{&write_request};

#if defined(__FreeBSD__)
    write_request.aio.aio_lio_opcode = LIO_WRITE;
    write_request.aio.aio_fildes = fd;
    write_request.aio.aio_buf = reinterpret_cast<volatile void *>(memory->data());
    write_request.aio.aio_nbytes = block_size;
    write_request.aio.aio_offset = block_size * current_file_block_id;
#else
    write_request.aio_lio_opcode = IOCB_CMD_PWRITE;
    write_request.aio_fildes = fd;
    write_request.aio_buf = reinterpret_cast<UInt64>(memory->data());
    write_request.aio_nbytes = block_size * write_buffer_size;
    write_request.aio_offset = block_size * current_file_block_id;
#endif

    Poco::Logger::get("try:").information("offset: " + std::to_string(write_request.aio_offset) + "  nbytes: " + std::to_string(write_request.aio_nbytes));

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

    if (bytes_written != static_cast<decltype(bytes_written)>(write_request.aio_nbytes))
        throw Exception("Not all data was written for asynchronous IO on file " + path + BIN_FILE_EXT + ". returned: " + std::to_string(bytes_written), ErrorCodes::AIO_WRITE_ERROR);

    if (::fsync(fd) < 0)
        throwFromErrnoWithPath("Cannot fsync " + path + BIN_FILE_EXT, path + BIN_FILE_EXT, ErrorCodes::CANNOT_FSYNC);

    /// commit changes in index
    for (size_t row = 0; row < ids.size(); ++row)
    {
        auto & index = key_to_index_and_metadata[ids[row]].index;
        if (index.inMemory()) // Row can be inserted in the buffer twice, so we need to move to ssd only the last index.
        {
            index.setInMemory(false);
            index.setBlockId(current_file_block_id + index.getBlockId());
        }
    }

    current_file_block_id += write_buffer_size;
    current_memory_block_id = 0;

    /// clear buffer
    std::visit([](auto & attr) { attr.clear(); }, keys_buffer.values);
}

template <typename Out>
void CachePartition::getValue(const size_t attribute_index, const PaddedPODArray<UInt64> & ids,
      ResultArrayType<Out> & out, std::vector<bool> & found,
      std::chrono::system_clock::time_point now) const
{
    std::shared_lock lock(rw_lock);
    PaddedPODArray<Index> indices(ids.size());
    for (size_t i = 0; i < ids.size(); ++i)
    {
        if (found[i])
        {
            indices[i].setNotExists();
        }
        else if (auto it = key_to_index_and_metadata.find(ids[i]);
                it != std::end(key_to_index_and_metadata) && it->second.metadata.expiresAt() > now)
        {
            indices[i] = it->second.index;
            found[i] = true;
        }
        else
        {
            indices[i].setNotExists();
        }
    }

    auto set_value = [&](const size_t index, ReadBuffer & buf)
    {
        readValueFromBuffer(attribute_index, out, index, buf);
    };

    getValueFromMemory(indices, set_value);
    getValueFromStorage(indices, set_value);
}

template <typename SetFunc>
void CachePartition::getValueFromMemory(const PaddedPODArray<Index> & indices, SetFunc set) const
{
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
void CachePartition::getValueFromStorage(const PaddedPODArray<Index> & indices, SetFunc set) const
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

    std::vector<iocb> requests;
    std::vector<iocb*> pointers;
    std::vector<std::vector<size_t>> blocks_to_indices;
    requests.reserve(index_to_out.size());
    pointers.reserve(index_to_out.size());
    blocks_to_indices.reserve(index_to_out.size());
    for (size_t i = 0; i < index_to_out.size(); ++i)
    {
        if (!requests.empty() &&
                static_cast<size_t>(requests.back().aio_offset) == index_to_out[i].first.getBlockId() * block_size)
        {
            blocks_to_indices.back().push_back(i);
            continue;
        }

        iocb request{};
#if defined(__FreeBSD__)
        request.aio.aio_lio_opcode = LIO_READ;
        request.aio.aio_fildes = fd;
        request.aio.aio_buf = reinterpret_cast<volatile void *>(
                reinterpret_cast<UInt64>(read_buffer.data()) + SSD_BLOCK_SIZE * (requests.size() % READ_BUFFER_SIZE_BLOCKS));
        request.aio.aio_nbytes = SSD_BLOCK_SIZE;
        request.aio.aio_offset = index_to_out[i].first;
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
    for (auto & event : events)
        event.res = -1; // TODO: remove

    size_t to_push = 0;
    size_t to_pop = 0;
    while (to_pop < requests.size())
    {
        /// get io tasks from previous iteration
        size_t popped = 0;
        while (to_pop < to_push && (popped = io_getevents(aio_context.ctx, to_push - to_pop, to_push - to_pop, &events[to_pop], nullptr)) < 0)
        {
            if (errno != EINTR)
                throwFromErrno("io_getevents: Failed to get an event for asynchronous IO", ErrorCodes::CANNOT_IO_GETEVENTS);
        }

        for (size_t i = to_pop; i < to_pop + popped; ++i)
        {
            const auto request_id = events[i].data;
            const auto & request = requests[request_id];
            if (events[i].res != static_cast<ssize_t>(request.aio_nbytes))
                throw Exception("AIO failed to read file " + path + BIN_FILE_EXT + ". " +
                    "request_id= " + std::to_string(request.aio_data) + ", aio_nbytes=" + std::to_string(request.aio_nbytes) + ", aio_offset=" + std::to_string(request.aio_offset) +
                    "returned: " + std::to_string(events[i].res), ErrorCodes::AIO_WRITE_ERROR);
            for (const size_t idx : blocks_to_indices[request_id])
            {
                const auto & [file_index, out_index] = index_to_out[idx];
                ReadBufferFromMemory buf(
                        reinterpret_cast<char *>(request.aio_buf) + file_index.getAddressInBlock(),
                        block_size - file_index.getAddressInBlock());
                set(i, buf);
            }

            processed[request_id] = true;
        }

        while (to_pop < requests.size() && processed[to_pop])
            ++to_pop;

        /// add new io tasks
        const size_t new_tasks_count = std::min(read_buffer_size - (to_push - to_pop), requests.size() - to_push);

        size_t pushed = 0;
        while (new_tasks_count > 0 && (pushed = io_submit(aio_context.ctx, new_tasks_count, &pointers[to_push])) < 0)
        {
            if (errno != EINTR)
                throwFromErrno("io_submit: Failed to submit a request for asynchronous IO", ErrorCodes::CANNOT_IO_SUBMIT);
        }
        to_push += pushed;
    }
}

template <typename Out>
void CachePartition::readValueFromBuffer(const size_t attribute_index, Out & dst, const size_t index, ReadBuffer & buf) const
{
    for (size_t i = 0; i < attribute_index; ++i)
    {
        switch (attributes_structure[i])
        {
#define DISPATCH(TYPE) \
        case AttributeUnderlyingType::ut##TYPE: \
            { \
                buf.ignore(sizeof(TYPE)); \
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
            /*{
                size_t size = 0;
                readVarUInt(size, buf);
                buf.ignore(size);
            }*/
            break;
        }
    }

    //if constexpr (!std::is_same_v<ColumnString, Out>)
    readBinary(dst[index], buf);
    /*else
    {
        LOG_DEBUG(&Poco::Logger::get("kek"), "string READ");
        UNUSED(index);
        size_t size = 0;
        readVarUInt(size, buf);
        dst.insertData(buf.position(), size);
    }*/
}

void CachePartition::has(const PaddedPODArray<UInt64> & ids, ResultArrayType<UInt8> & out, std::chrono::system_clock::time_point now) const
{
    std::shared_lock lock(rw_lock);
    for (size_t i = 0; i < ids.size(); ++i)
    {
        auto it = key_to_index_and_metadata.find(ids[i]);

        if (it == std::end(key_to_index_and_metadata) || it->second.metadata.expiresAt() <= now)
        {
            out[i] = HAS_NOT_FOUND;
        }
        else
        {
            out[i] = !it->second.metadata.isDefault();
        }
    }
}

size_t CachePartition::getId() const
{
    return file_id;
}

double CachePartition::getLoadFactor() const
{
    std::shared_lock lock(rw_lock);
    return static_cast<double>(current_file_block_id) / max_size;
}

size_t CachePartition::getElementCount() const
{
    std::shared_lock lock(rw_lock);
    return key_to_index_and_metadata.size();
}

PaddedPODArray<CachePartition::Key> CachePartition::getCachedIds(const std::chrono::system_clock::time_point now) const
{
    const ProfilingScopedReadRWLock read_lock{rw_lock, ProfileEvents::DictCacheLockReadNs};

    PaddedPODArray<Key> array;
    for (const auto & [key, index_and_metadata] : key_to_index_and_metadata)
        if (!index_and_metadata.metadata.isDefault() && index_and_metadata.metadata.expiresAt() > now)
            array.push_back(key);
    return array;
}

void CachePartition::remove()
{
    std::unique_lock lock(rw_lock);
    //Poco::File(path + BIN_FILE_EXT).remove();
    //std::filesystem::remove(std::filesystem::path(path + BIN_FILE_EXT));
}

CacheStorage::CacheStorage(
        const AttributeTypes & attributes_structure_,
        const std::string & path_,
        const size_t max_partitions_count_,
        const size_t partition_size_,
        const size_t block_size_,
        const size_t read_buffer_size_,
        const size_t write_buffer_size_)
    : attributes_structure(attributes_structure_)
    , path(path_)
    , max_partitions_count(max_partitions_count_)
    , partition_size(partition_size_)
    , block_size(block_size_)
    , read_buffer_size(read_buffer_size_)
    , write_buffer_size(write_buffer_size_)
    , log(&Poco::Logger::get("CacheStorage"))
{
}

CacheStorage::~CacheStorage()
{
    std::unique_lock lock(rw_lock);
    partition_delete_queue.splice(std::end(partition_delete_queue), partitions);
    collectGarbage();
}

template <typename Out>
void CacheStorage::getValue(const size_t attribute_index, const PaddedPODArray<UInt64> & ids,
      ResultArrayType<Out> & out, std::unordered_map<Key, std::vector<size_t>> & not_found,
      std::chrono::system_clock::time_point now) const
{
    std::vector<bool> found(ids.size(), false);

    {
        std::shared_lock lock(rw_lock);
        for (auto & partition : partitions)
            partition->getValue<Out>(attribute_index, ids, out, found, now);

        for (size_t i = 0; i < ids.size(); ++i)
            if (!found[i])
                not_found[ids[i]].push_back(i);
    }
    query_count.fetch_add(ids.size(), std::memory_order_relaxed);
    hit_count.fetch_add(ids.size() - not_found.size(), std::memory_order_release);
}

void CacheStorage::has(const PaddedPODArray<UInt64> & ids, ResultArrayType<UInt8> & out,
     std::unordered_map<Key, std::vector<size_t>> & not_found, std::chrono::system_clock::time_point now) const
{
    {
        std::shared_lock lock(rw_lock);
        for (auto & partition : partitions)
            partition->has(ids, out, now);

        for (size_t i = 0; i < ids.size(); ++i)
            if (out[i] == HAS_NOT_FOUND)
                not_found[ids[i]].push_back(i);
    }
    query_count.fetch_add(ids.size(), std::memory_order_relaxed);
    hit_count.fetch_add(ids.size() - not_found.size(), std::memory_order_release);
}

template <typename PresentIdHandler, typename AbsentIdHandler>
void CacheStorage::update(DictionarySourcePtr & source_ptr, const std::vector<Key> & requested_ids,
        PresentIdHandler && on_updated, AbsentIdHandler && on_id_not_found,
        const DictionaryLifetime lifetime, const std::vector<AttributeValueVariant> & null_values)
{
    auto append_block = [this](const CachePartition::Attribute & new_keys,
            const CachePartition::Attributes & new_attributes, const PaddedPODArray<CachePartition::Metadata> & metadata)
    {
        size_t inserted = 0;
        while (inserted < metadata.size())
        {
            if (!partitions.empty())
                inserted += partitions.front()->appendBlock(new_keys, new_attributes, metadata, inserted);
            if (inserted < metadata.size())
            {
                partitions.emplace_front(std::make_unique<CachePartition>(
                        AttributeUnderlyingType::utUInt64, attributes_structure, path,
                        (partitions.empty() ? 0 : partitions.front()->getId() + 1),
                        partition_size, block_size, read_buffer_size, write_buffer_size));
            }
        }

        collectGarbage();
    };

    CurrentMetrics::Increment metric_increment{CurrentMetrics::DictCacheRequests};
    ProfileEvents::increment(ProfileEvents::DictCacheKeysRequested, requested_ids.size());

    std::unordered_map<Key, UInt8> remaining_ids{requested_ids.size()};
    for (const auto id : requested_ids)
        remaining_ids.insert({id, 0});

    const auto now = std::chrono::system_clock::now();

    {
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
                auto stream = source_ptr->loadIds(requested_ids);
                stream->readPrefix();

                while (const auto block = stream->read())
                {
                    const auto new_keys = std::move(createAttributesFromBlock(block, 0, { AttributeUnderlyingType::utUInt64 }).front());
                    const auto new_attributes = createAttributesFromBlock(block, 1, attributes_structure);

                    const auto & ids = std::get<CachePartition::Attribute::Container<UInt64>>(new_keys.values);

                    PaddedPODArray<CachePartition::Metadata> metadata(ids.size());

                    for (const auto i : ext::range(0, ids.size()))
                    {
                        std::uniform_int_distribution<UInt64> distribution{lifetime.min_sec, lifetime.max_sec};
                        metadata[i].setExpiresAt(now + std::chrono::seconds(distribution(rnd_engine)));
                        /// mark corresponding id as found
                        on_updated(ids[i], i, new_attributes);
                        remaining_ids[ids[i]] = 1;
                    }

                    append_block(new_keys, new_attributes, metadata);
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

    size_t not_found_num = 0, found_num = 0;

    /// Check which ids have not been found and require setting null_value
    CachePartition::Attribute new_keys;
    new_keys.type = AttributeUnderlyingType::utUInt64;
    new_keys.values = CachePartition::Attribute::Container<UInt64>();
    CachePartition::Attributes new_attributes;
    {
        /// TODO: create attributes from structure
        for (const auto & attribute_type : attributes_structure)
        {
            switch (attribute_type)
            {
#define DISPATCH(TYPE) \
            case AttributeUnderlyingType::ut##TYPE: \
                new_attributes.emplace_back(); \
                new_attributes.back().type = attribute_type; \
                new_attributes.back().values = CachePartition::Attribute::Container<TYPE>(); \
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
                /*{
                    new_attributes.emplace_back();
                    new_attributes.back().type = attribute_type;
                    new_attributes.back().values = CachePartition::Attribute::Container<String>();
                }*/
                break;
            }
        }
    }

    PaddedPODArray<CachePartition::Metadata> metadata;

    for (const auto & id_found_pair : remaining_ids)
    {
        if (id_found_pair.second)
        {
            ++found_num;
            continue;
        }
        ++not_found_num;

        const auto id = id_found_pair.first;

        if (update_error_count)
        {
            /// TODO: юзать старые значения.

            /// We don't have expired data for that `id` so all we can do is to rethrow `last_exception`.
            std::rethrow_exception(last_update_exception);
        }

        // Set key
        std::get<CachePartition::Attribute::Container<UInt64>>(new_keys.values).push_back(id);

        std::uniform_int_distribution<UInt64> distribution{lifetime.min_sec, lifetime.max_sec};
        metadata.emplace_back();
        metadata.back().setExpiresAt(now + std::chrono::seconds(distribution(rnd_engine)));
        metadata.back().setDefault();

        /// Set null_value for each attribute
        for (size_t i = 0; i < attributes_structure.size(); ++i)
        {
            const auto & attribute = attributes_structure[i];
            // append null
            switch (attribute)
            {
#define DISPATCH(TYPE) \
            case AttributeUnderlyingType::ut##TYPE: \
                { \
                    auto & to_values = std::get<CachePartition::Attribute::Container<TYPE>>(new_attributes[i].values); \
                    auto & null_value = std::get<TYPE>(null_values[i]); \
                    to_values.push_back(null_value); \
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
                /*{
                    auto & to_values = std::get<CachePartition::Attribute::Container<String>>(new_attributes[i].values);
                    auto & null_value = std::get<String>(null_values[i]);
                    to_values.push_back(null_value);
                }*/
                break;
            }
        }

        /// inform caller that the cell has not been found
        on_id_not_found(id);
    }

    {
        const ProfilingScopedWriteRWLock write_lock{rw_lock, ProfileEvents::DictCacheLockWriteNs};
        if (not_found_num)
            append_block(new_keys, new_attributes, metadata);
    }

    ProfileEvents::increment(ProfileEvents::DictCacheKeysRequestedMiss, not_found_num);
    ProfileEvents::increment(ProfileEvents::DictCacheKeysRequestedFound, found_num);
    ProfileEvents::increment(ProfileEvents::DictCacheRequests);
}

PaddedPODArray<CachePartition::Key> CacheStorage::getCachedIds() const
{
    PaddedPODArray<Key> array;

    const auto now = std::chrono::system_clock::now();

    std::shared_lock lock(rw_lock);
    for (auto & partition : partitions)
    {
        const auto cached_in_partition = partition->getCachedIds(now);
        array.insert(std::begin(cached_in_partition), std::end(cached_in_partition));
    }

    return array;
}

double CacheStorage::getLoadFactor() const
{
    double result = 0;
    std::shared_lock lock(rw_lock);
    for (const auto & partition : partitions)
        result += partition->getLoadFactor();
    return result / partitions.size();
}

size_t CacheStorage::getElementCount() const
{
    size_t result = 0;
    std::shared_lock lock(rw_lock);
    for (const auto & partition : partitions)
        result += partition->getElementCount();
    return result;
}

void CacheStorage::collectGarbage()
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

CachePartition::Attributes CacheStorage::createAttributesFromBlock(
        const Block & block, const size_t begin_column, const std::vector<AttributeUnderlyingType> & structure)
{
    CachePartition::Attributes attributes;

    const auto columns = block.getColumns();
    for (size_t i = 0; i < structure.size(); ++i)
    {
        const auto & column = columns[i + begin_column];
        switch (structure[i])
        {
#define DISPATCH(TYPE) \
        case AttributeUnderlyingType::ut##TYPE: \
            { \
                CachePartition::Attribute::Container<TYPE> values(column->size()); \
                const auto raw_data = column->getRawData(); \
                memcpy(&values[0], raw_data.data, raw_data.size * sizeof(TYPE)); \
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
            /*{
                attributes.emplace_back();
                CachePartition::Attribute::Container<String> values(column->size());
                for (size_t j = 0; j < column->size(); ++j)
                {
                    const auto ref = column->getDataAt(j);
                    values[j].resize(ref.size);
                    memcpy(values[j].data(), ref.data, ref.size);
                }
                attributes.back().type = structure[i];
                attributes.back().values = std::move(values);
            }*/
            break;
        }
    }

    return attributes;
}

SSDCacheDictionary::SSDCacheDictionary(
    const std::string & name_,
    const DictionaryStructure & dict_struct_,
    DictionarySourcePtr source_ptr_,
    const DictionaryLifetime dict_lifetime_,
    const std::string & path_,
    const size_t max_partitions_count_,
    const size_t partition_size_,
    const size_t block_size_,
    const size_t read_buffer_size_,
    const size_t write_buffer_size_)
    : name(name_)
    , dict_struct(dict_struct_)
    , source_ptr(std::move(source_ptr_))
    , dict_lifetime(dict_lifetime_)
    , path(path_)
    , max_partitions_count(max_partitions_count_)
    , partition_size(partition_size_)
    , block_size(block_size_)
    , read_buffer_size(read_buffer_size_)
    , write_buffer_size(write_buffer_size_)
    , storage(ext::map<std::vector>(dict_struct.attributes, [](const auto & attribute) { return attribute.underlying_type; }),
            path, max_partitions_count, partition_size, block_size, read_buffer_size, write_buffer_size)
    , log(&Poco::Logger::get("SSDCacheDictionary"))
{
    if (!this->source_ptr->supportsSelectiveLoad())
        throw Exception{name + ": source cannot be used with CacheDictionary", ErrorCodes::UNSUPPORTED_METHOD};

    createAttributes();
}

#define DECLARE(TYPE) \
    void SSDCacheDictionary::get##TYPE( \
        const std::string & attribute_name, const PaddedPODArray<Key> & ids, ResultArrayType<TYPE> & out) const \
    { \
        const auto index = getAttributeIndex(attribute_name); \
        checkAttributeType(name, attribute_name, dict_struct.attributes[index].underlying_type, AttributeUnderlyingType::ut##TYPE); \
        const auto null_value = std::get<TYPE>(null_values[index]); \
        getItemsNumberImpl<TYPE, TYPE>( \
                index, \
                ids, \
                out, \
                [&](const size_t) { return null_value; }); \
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
    void SSDCacheDictionary::get##TYPE( \
        const std::string & attribute_name, \
        const PaddedPODArray<Key> & ids, \
        const PaddedPODArray<TYPE> & def, \
        ResultArrayType<TYPE> & out) const \
    { \
        const auto index = getAttributeIndex(attribute_name); \
        checkAttributeType(name, attribute_name, dict_struct.attributes[index].underlying_type, AttributeUnderlyingType::ut##TYPE); \
        getItemsNumberImpl<TYPE, TYPE>( \
            index, \
            ids, \
            out, \
            [&](const size_t row) { return def[row]; }); \
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
    void SSDCacheDictionary::get##TYPE( \
        const std::string & attribute_name, \
        const PaddedPODArray<Key> & ids, \
        const TYPE def, \
        ResultArrayType<TYPE> & out) const \
    { \
        const auto index = getAttributeIndex(attribute_name); \
        checkAttributeType(name, attribute_name, dict_struct.attributes[index].underlying_type, AttributeUnderlyingType::ut##TYPE); \
        getItemsNumberImpl<TYPE, TYPE>( \
            index, \
            ids, \
            out, \
            [&](const size_t) { return def; }); \
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
void SSDCacheDictionary::getItemsNumberImpl(
        const size_t attribute_index, const PaddedPODArray<Key> & ids, ResultArrayType<OutputType> & out, DefaultGetter && get_default) const
{
    const auto now = std::chrono::system_clock::now();

    std::unordered_map<Key, std::vector<size_t>> not_found_ids;
    storage.getValue<OutputType>(attribute_index, ids, out, not_found_ids, now);
    if (not_found_ids.empty())
        return;

    std::vector<Key> required_ids(not_found_ids.size());
    std::transform(std::begin(not_found_ids), std::end(not_found_ids), std::begin(required_ids), [](const auto & pair) { return pair.first; });

    storage.update(
            source_ptr,
            required_ids,
            [&](const auto id, const auto row, const auto & new_attributes) {
                for (const size_t out_row : not_found_ids[id])
                    out[out_row] = std::get<CachePartition::Attribute::Container<OutputType>>(new_attributes[attribute_index].values)[row];
            },
            [&](const size_t id)
            {
                for (const size_t row : not_found_ids[id])
                    out[row] = get_default(row);
            },
            getLifetime(),
            null_values);
}

void SSDCacheDictionary::getString(const std::string & attribute_name, const PaddedPODArray<Key> & ids, ColumnString * out) const
{
    const auto index = getAttributeIndex(attribute_name);
    checkAttributeType(name, attribute_name, dict_struct.attributes[index].underlying_type, AttributeUnderlyingType::utString);

    const auto null_value = StringRef{std::get<String>(null_values[index])};

    getItemsStringImpl(index, ids, out, [&](const size_t) { return null_value; });
}

void SSDCacheDictionary::getString(
        const std::string & attribute_name, const PaddedPODArray<Key> & ids, const ColumnString * const def, ColumnString * const out) const
{
    const auto index = getAttributeIndex(attribute_name);
    checkAttributeType(name, attribute_name, dict_struct.attributes[index].underlying_type, AttributeUnderlyingType::utString);

    getItemsStringImpl(index, ids, out, [&](const size_t row) { return def->getDataAt(row); });
}

void SSDCacheDictionary::getString(
        const std::string & attribute_name, const PaddedPODArray<Key> & ids, const String & def, ColumnString * const out) const
{
    const auto index = getAttributeIndex(attribute_name);
    checkAttributeType(name, attribute_name, dict_struct.attributes[index].underlying_type, AttributeUnderlyingType::utString);

    getItemsStringImpl(index, ids, out, [&](const size_t) { return StringRef{def}; });
}

template <typename DefaultGetter>
void SSDCacheDictionary::getItemsStringImpl(const size_t attribute_index, const PaddedPODArray<Key> & ids,
        ColumnString * out, DefaultGetter && get_default) const
{
    UNUSED(attribute_index);
    UNUSED(ids);
    UNUSED(out);
    const auto now = std::chrono::system_clock::now();
    UNUSED(now);
    UNUSED(get_default);

    return;
/*
    std::unordered_map<Key, std::vector<size_t>> not_found_ids;

    auto from_cache = ColumnString::create();
    //storage.template getValue<String>(attribute_index, ids, *from_cache, not_found_ids, now);
    if (not_found_ids.empty())
    {
        out->getChars().resize(from_cache->getChars().size());
        memcpy(out->getChars().data(), from_cache->getChars().data(), from_cache->getChars().size() * sizeof(from_cache->getChars()[0]));
        out->getOffsets().resize(from_cache->getOffsets().size());
        memcpy(out->getOffsets().data(), from_cache->getOffsets().data(), from_cache->getOffsets().size() * sizeof(from_cache->getOffsets()[0]));
        return;
    }

    std::vector<Key> required_ids(not_found_ids.size());
    std::transform(std::begin(not_found_ids), std::end(not_found_ids), std::begin(required_ids), [](const auto & pair) { return pair.first; });

    std::unordered_map<Key, String> update_result;

    storage.update(
            source_ptr,
            required_ids,
            [&](const auto id, const auto row, const auto & new_attributes)
            {
                update_result[id] = std::get<CachePartition::Attribute::Container<String>>(new_attributes[attribute_index].values)[row];
            },
            [&](const size_t) {},
            getLifetime(),
            null_values);

    LOG_DEBUG(&Poco::Logger::get("log"), "fill data");
    size_t from_cache_counter = 0;
    for (size_t row = 0; row < ids.size(); ++row)
    {
        const auto & id = ids[row];
        auto it = not_found_ids.find(id);
        if (it == std::end(not_found_ids))
        {
            LOG_DEBUG(&Poco::Logger::get("log"), "fill found " << row << " " << id);
            out->insertFrom(*from_cache, from_cache_counter++);
        }
        else
        {
            auto it_update = update_result.find(id);
            if (it_update != std::end(update_result))
            {
                LOG_DEBUG(&Poco::Logger::get("log"), "fill update " << row << " " << id);
                out->insertData(it_update->second.data(), it_update->second.size());
            }
            else
            {
                LOG_DEBUG(&Poco::Logger::get("log"), "fill default " << row << " " << id);
                auto to_insert = get_default(row);
                out->insertData(to_insert.data, to_insert.size);
            }
        }
    }*/
}

void SSDCacheDictionary::has(const PaddedPODArray<Key> & ids, PaddedPODArray<UInt8> & out) const
{
    const auto now = std::chrono::system_clock::now();

    std::unordered_map<Key, std::vector<size_t>> not_found_ids;
    storage.has(ids, out, not_found_ids, now);
    if (not_found_ids.empty())
        return;

    std::vector<Key> required_ids(not_found_ids.size());
    std::transform(std::begin(not_found_ids), std::end(not_found_ids), std::begin(required_ids), [](const auto & pair) { return pair.first; });

    storage.update(
            source_ptr,
            required_ids,
            [&](const auto id, const auto, const auto &) {
                for (const size_t out_row : not_found_ids[id])
                    out[out_row] = true;
            },
            [&](const size_t id)
            {
                for (const size_t row : not_found_ids[id])
                    out[row] = false;
            },
            getLifetime(),
            null_values);
}

BlockInputStreamPtr SSDCacheDictionary::getBlockInputStream(const Names & column_names, size_t max_block_size) const
{
    using BlockInputStreamType = DictionaryBlockInputStream<SSDCacheDictionary, Key>;
    return std::make_shared<BlockInputStreamType>(shared_from_this(), max_block_size, storage.getCachedIds(), column_names);
}

size_t SSDCacheDictionary::getAttributeIndex(const std::string & attr_name) const
{
    auto it = attribute_index_by_name.find(attr_name);
    if (it == std::end(attribute_index_by_name))
        throw  Exception{"Attribute `" + name + "` does not exist.", ErrorCodes::BAD_ARGUMENTS};
    return it->second;
}

template <typename T>
AttributeValueVariant SSDCacheDictionary::createAttributeNullValueWithTypeImpl(const Field & null_value)
{
    AttributeValueVariant var_null_value = static_cast<T>(null_value.get<NearestFieldType<T>>());
    bytes_allocated += sizeof(T);
    return var_null_value;
}

template <>
AttributeValueVariant SSDCacheDictionary::createAttributeNullValueWithTypeImpl<String>(const Field & null_value)
{
    AttributeValueVariant var_null_value = null_value.get<String>();
    bytes_allocated += sizeof(StringRef);
    return var_null_value;
}

AttributeValueVariant SSDCacheDictionary::createAttributeNullValueWithType(const AttributeUnderlyingType type, const Field & null_value)
{
    switch (type)
    {
#define DISPATCH(TYPE) \
case AttributeUnderlyingType::ut##TYPE: \
    return createAttributeNullValueWithTypeImpl<TYPE>(null_value);

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

void SSDCacheDictionary::createAttributes()
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

void registerDictionarySSDCache(DictionaryFactory & factory)
{
    auto create_layout = [=](const std::string & name,
                             const DictionaryStructure & dict_struct,
                             const Poco::Util::AbstractConfiguration & config,
                             const std::string & config_prefix,
                             DictionarySourcePtr source_ptr) -> DictionaryPtr
    {
        if (dict_struct.key)
            throw Exception{"'key' is not supported for dictionary of layout 'cache'", ErrorCodes::UNSUPPORTED_METHOD};

        if (dict_struct.range_min || dict_struct.range_max)
            throw Exception{name
                            + ": elements .structure.range_min and .structure.range_max should be defined only "
                              "for a dictionary of layout 'range_hashed'",
                            ErrorCodes::BAD_ARGUMENTS};
        const auto & layout_prefix = config_prefix + ".layout";

        const auto max_partitions_count = config.getInt(layout_prefix + ".ssd.max_partitions_count", DEFAULT_PARTITIONS_COUNT);
        if (max_partitions_count <= 0)
            throw Exception{name + ": dictionary of layout 'ssdcache' cannot have 0 (or less) max_partitions_count", ErrorCodes::BAD_ARGUMENTS};

        const auto block_size = config.getInt(layout_prefix + ".ssd.block_size", DEFAULT_SSD_BLOCK_SIZE);
        if (block_size <= 0)
            throw Exception{name + ": dictionary of layout 'ssdcache' cannot have 0 (or less) block_size", ErrorCodes::BAD_ARGUMENTS};

        const auto partition_size = config.getInt64(layout_prefix + ".ssd.partition_size", DEFAULT_FILE_SIZE);
        if (partition_size <= 0)
            throw Exception{name + ": dictionary of layout 'ssdcache' cannot have 0 (or less) partition_size", ErrorCodes::BAD_ARGUMENTS};
        if (partition_size % block_size != 0)
            throw Exception{name + ": partition_size must be a multiple of block_size", ErrorCodes::BAD_ARGUMENTS};

        const auto read_buffer_size = config.getInt64(layout_prefix + ".ssd.read_buffer_size", DEFAULT_READ_BUFFER_SIZE);
        if (read_buffer_size <= 0)
            throw Exception{name + ": dictionary of layout 'ssdcache' cannot have 0 (or less) read_buffer_size", ErrorCodes::BAD_ARGUMENTS};
        if (read_buffer_size % block_size != 0)
            throw Exception{name + ": read_buffer_size must be a multiple of block_size", ErrorCodes::BAD_ARGUMENTS};

        const auto write_buffer_size = config.getInt64(layout_prefix + ".ssd.write_buffer_size", DEFAULT_WRITE_BUFFER_SIZE);
        if (write_buffer_size <= 0)
            throw Exception{name + ": dictionary of layout 'ssdcache' cannot have 0 (or less) write_buffer_size", ErrorCodes::BAD_ARGUMENTS};
        if (write_buffer_size % block_size != 0)
            throw Exception{name + ": write_buffer_size must be a multiple of block_size", ErrorCodes::BAD_ARGUMENTS};

        const auto path = config.getString(layout_prefix + ".ssd.path");
        if (path.empty())
            throw Exception{name + ": dictionary of layout 'ssdcache' cannot have empty path",
                            ErrorCodes::BAD_ARGUMENTS};

        const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};
        return std::make_unique<SSDCacheDictionary>(
                name, dict_struct, std::move(source_ptr), dict_lifetime, path,
                max_partitions_count, partition_size / block_size, block_size,
                read_buffer_size / block_size, write_buffer_size / block_size);
    };
    factory.registerLayout("ssd", create_layout, false);
}

}
