#include "SSDCacheDictionary.h"

#include <algorithm>
#include <Columns/ColumnsNumber.h>
#include <Common/typeid_cast.h>
#include <Common/ProfileEvents.h>
#include <Common/ProfilingScopedRWLock.h>
#include <Common/MemorySanitizer.h>
#include <DataStreams/IBlockInputStream.h>
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
    constexpr size_t SSD_BLOCK_SIZE = DEFAULT_AIO_FILE_BLOCK_SIZE; // TODO: в параметры
    constexpr size_t BUFFER_ALIGNMENT = DEFAULT_AIO_FILE_BLOCK_SIZE; // TODO: в параметры

    constexpr size_t AIO_MAX_SIMULTANIOUS_REQUESTS = 32;
    constexpr size_t READ_BUFFER_SIZE_BLOCKS = 16; // TODO: в параметры

    static constexpr UInt64 KEY_METADATA_EXPIRES_AT_MASK = std::numeric_limits<std::chrono::system_clock::time_point::rep>::max();
    static constexpr UInt64 KEY_METADATA_IS_DEFAULT_MASK = ~KEY_METADATA_EXPIRES_AT_MASK;

    constexpr size_t KEY_IN_MEMORY_BIT = 63;
    constexpr size_t KEY_IN_MEMORY = (1ULL << KEY_IN_MEMORY_BIT);
    constexpr size_t BLOCK_INDEX_BITS = 32;
    constexpr size_t INDEX_IN_BLOCK_BITS = 16;
    constexpr size_t INDEX_IN_BLOCK_MASK = (1ULL << INDEX_IN_BLOCK_BITS) - 1;
    constexpr size_t BLOCK_INDEX_MASK = ((1ULL << (BLOCK_INDEX_BITS + INDEX_IN_BLOCK_BITS)) - 1) ^ INDEX_IN_BLOCK_MASK;

    constexpr size_t NOT_EXISTS = -1;

    const std::string BIN_FILE_EXT = ".bin";
    const std::string IND_FILE_EXT = ".idx";
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
        const AttributeUnderlyingType & /* key_structure */, const std::vector<AttributeUnderlyingType> & attributes_structure_,
        const std::string & dir_path, const size_t file_id_, const size_t max_size_)
    : file_id(file_id_), max_size(max_size_), path(dir_path + "/" + std::to_string(file_id))
    , attributes_structure(attributes_structure_), memory(SSD_BLOCK_SIZE, BUFFER_ALIGNMENT)
{
    keys_buffer.type = AttributeUnderlyingType::utUInt64;
    keys_buffer.values = std::vector<UInt64>();

    {
        ProfileEvents::increment(ProfileEvents::FileOpen);

        const std::string filename = path + BIN_FILE_EXT;
        fd = ::open(filename.c_str(), O_RDWR | O_CREAT | O_TRUNC | O_DIRECT, 0666);
        if (fd == -1)
        {
            auto error_code = (errno == ENOENT) ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE;
            throwFromErrnoWithPath("Cannot open file " + filename, filename, error_code);
        }
    }
}

CachePartition::~CachePartition()
{
    std::unique_lock lock(rw_lock);
    ::close(fd);
}

void CachePartition::appendBlock(const Attribute & new_keys, const Attributes & new_attributes, const std::vector<Metadata> & metadata)
{
    std::unique_lock lock(rw_lock);
    if (new_attributes.size() != attributes_structure.size())
        throw Exception{"Wrong columns number in block.", ErrorCodes::BAD_ARGUMENTS};

    const auto & ids = std::get<Attribute::Container<UInt64>>(new_keys.values);
    auto & ids_buffer = std::get<Attribute::Container<UInt64>>(keys_buffer.values);

    if (!write_buffer)
        write_buffer.emplace(memory.data(), SSD_BLOCK_SIZE);

    for (size_t index = 0; index < ids.size();)
    {
        auto & index_and_metadata = key_to_index_and_metadata[ids[index]];
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
                    // TODO: string support
                    break;
            }
        }

        if (!flushed)
        {
            ids_buffer.push_back(ids[index]);
            ++index;
        }
        else
            write_buffer.emplace(memory.data(), SSD_BLOCK_SIZE);
    }
}

size_t CachePartition::appendValuesToAttribute(Attribute & to, const Attribute & from)
{
    switch (to.type)
    {
#define DISPATCH(TYPE) \
    case AttributeUnderlyingType::ut##TYPE: \
        { \
            auto &to_values = std::get<Attribute::Container<TYPE>>(to.values); \
            auto &from_values = std::get<Attribute::Container<TYPE>>(from.values); \
            size_t prev_size = to_values.size(); \
            to_values.resize(to_values.size() + from_values.size()); \
            memcpy(&to_values[prev_size], &from_values[0], from_values.size() * sizeof(TYPE)); \
            return from_values.size() * sizeof(TYPE); \
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
            // TODO: string support
            break;
    }
    throw Exception{"Unknown attribute type: " + std::to_string(static_cast<int>(to.type)), ErrorCodes::TYPE_MISMATCH};
}

void CachePartition::flush()
{
    write_buffer.reset();
    const auto & ids = std::get<Attribute::Container<UInt64>>(keys_buffer.values);
    if (ids.empty())
        return;

    Poco::Logger::get("paritiiton").information("@@@@@@@@@@@@@@@@@@@@ FLUSH!!!");

    AIOContext aio_context{1};

    iocb write_request{};
    iocb * write_request_ptr{&write_request};

#if defined(__FreeBSD__)
    write_request.aio.aio_lio_opcode = LIO_WRITE;
    write_request.aio.aio_fildes = fd;
    write_request.aio.aio_buf = reinterpret_cast<volatile void *>(memory.data());
    write_request.aio.aio_nbytes = SSD_BLOCK_SIZE;
    write_request.aio.aio_offset = SSD_BLOCK_SIZE * current_file_block_id;
#else
    write_request.aio_lio_opcode = IOCB_CMD_PWRITE;
    write_request.aio_fildes = fd;
    write_request.aio_buf = reinterpret_cast<UInt64>(memory.data());
    write_request.aio_nbytes = SSD_BLOCK_SIZE;
    write_request.aio_offset = SSD_BLOCK_SIZE * current_file_block_id;
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
        key_to_index_and_metadata[ids[row]].index.setInMemory(false);
        key_to_index_and_metadata[ids[row]].index.setBlockId(current_file_block_id);
    }

    ++current_file_block_id;

    /// clear buffer
    std::visit([](auto & attr) { attr.clear(); }, keys_buffer.values);
}

template <typename Out, typename Key>
void CachePartition::getValue(const size_t attribute_index, const PaddedPODArray<UInt64> & ids,
          ResultArrayType<Out> & out, std::unordered_map<Key, std::vector<size_t>> & not_found,
          std::chrono::system_clock::time_point now) const
{
    std::shared_lock lock(rw_lock);
    PaddedPODArray<Index> indices(ids.size());
    for (size_t i = 0; i < ids.size(); ++i)
    {
        auto it = key_to_index_and_metadata.find(ids[i]);
        if (it == std::end(key_to_index_and_metadata))
        {
            indices[i].setNotExists();
            not_found[ids[i]].push_back(i);
        }
        else if (it->second.metadata.expiresAt() <= now)
        {
            indices[i].setNotExists();
            not_found[ids[i]].push_back(i);
        }
        else
        {
            indices[i] = it->second.index;
        }
    }

    getValueFromMemory<Out>(attribute_index, indices, out);
    getValueFromStorage<Out>(attribute_index, indices, out);
}

template <typename Out>
void CachePartition::getValueFromMemory(
        const size_t attribute_index, const PaddedPODArray<Index> & indices, ResultArrayType<Out> & out) const
{
    for (size_t i = 0; i < indices.size(); ++i)
    {
        const auto & index = indices[i];
        if (index.exists() && index.inMemory())
        {
            const size_t offset = index.getAddressInBlock();

            ReadBufferFromMemory read_buffer(memory.data() + offset, SSD_BLOCK_SIZE - offset);
            readValueFromBuffer(attribute_index, out[i], read_buffer);
        }
    }
}

template <typename Out>
void CachePartition::getValueFromStorage(
        const size_t attribute_index, const PaddedPODArray<Index> & indices, ResultArrayType<Out> & out) const
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

    DB::Memory read_buffer(SSD_BLOCK_SIZE * READ_BUFFER_SIZE_BLOCKS, BUFFER_ALIGNMENT);

    std::vector<iocb> requests;
    std::vector<iocb*> pointers;
    std::vector<std::vector<size_t>> blocks_to_indices;
    requests.reserve(index_to_out.size());
    pointers.reserve(index_to_out.size());
    blocks_to_indices.reserve(index_to_out.size());
    for (size_t i = 0; i < index_to_out.size(); ++i)
    {
        if (!requests.empty() &&
                static_cast<size_t>(requests.back().aio_offset) == index_to_out[i].first.getBlockId() * SSD_BLOCK_SIZE)
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
        request.aio_buf = reinterpret_cast<UInt64>(read_buffer.data()) + SSD_BLOCK_SIZE * (requests.size() % READ_BUFFER_SIZE_BLOCKS);
        request.aio_nbytes = SSD_BLOCK_SIZE;
        request.aio_offset = index_to_out[i].first.getBlockId() * SSD_BLOCK_SIZE;
        request.aio_data = requests.size();
#endif
        requests.push_back(request);
        pointers.push_back(&requests.back());
        blocks_to_indices.emplace_back();
        blocks_to_indices.back().push_back(i);
    }

    AIOContext aio_context(READ_BUFFER_SIZE_BLOCKS);

    std::vector<bool> processed(requests.size(), false);
    std::vector<io_event> events(requests.size());

    size_t to_push = 0;
    size_t to_pop = 0;
    while (to_pop < requests.size())
    {
        /// get io tasks from previous iteration
        size_t popped = 0;
        while (to_pop < to_push && (popped = io_getevents(aio_context.ctx, to_push - to_pop, to_push - to_pop, &events[to_pop], nullptr)) < 0)
        {
            if (errno != EINTR)
                throwFromErrno("io_submit: Failed to submit a request for asynchronous IO", ErrorCodes::CANNOT_IO_SUBMIT);
        }

        for (size_t i = to_pop; i < to_pop + popped; ++i)
        {
            const auto request_id = events[i].data;
            const auto & request = requests[request_id];
            if (events[i].res != static_cast<ssize_t>(request.aio_nbytes))
                throw Exception("AIO failed to read file " + path + BIN_FILE_EXT + ". returned: " + std::to_string(events[i].res), ErrorCodes::AIO_WRITE_ERROR);

            for (const size_t idx : blocks_to_indices[request_id])
            {
                const auto & [file_index, out_index] = index_to_out[idx];
                DB::ReadBufferFromMemory buf(
                        reinterpret_cast<char *>(request.aio_buf) + file_index.getAddressInBlock(),
                        SSD_BLOCK_SIZE - file_index.getAddressInBlock());
                readValueFromBuffer(attribute_index, out[out_index], buf);
            }

            processed[request_id] = true;
        }

        while (to_pop < requests.size() && processed[to_pop])
            ++to_pop;

        /// add new io tasks
        const size_t new_tasks_count = std::min(READ_BUFFER_SIZE_BLOCKS - (to_push - to_pop), requests.size() - to_push);

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
void CachePartition::readValueFromBuffer(const size_t attribute_index, Out & dst, ReadBuffer & buf) const
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
                // TODO: string support
                break;
        }
    }

    switch (attributes_structure[attribute_index])
    {
#define DISPATCH(TYPE) \
                case AttributeUnderlyingType::ut##TYPE: \
                    readBinary(dst, buf); \
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
            // TODO: string support
            break;
    }
}

template <typename Key>
void CachePartition::has(const PaddedPODArray<UInt64> & ids, ResultArrayType<UInt8> & out,
        std::unordered_map<Key, std::vector<size_t>> & not_found, std::chrono::system_clock::time_point now) const
{
    std::shared_lock lock(rw_lock);
    for (size_t i = 0; i < ids.size(); ++i)
    {
        auto it = key_to_index_and_metadata.find(ids[i]);

        if (it == std::end(key_to_index_and_metadata))
        {
            not_found[ids[i]].push_back(i);
        }
        else if (it->second.metadata.expiresAt() <= now)
        {
            not_found[ids[i]].push_back(i);
        }
        else
        {
            out[i] = !it->second.metadata.isDefault();
        }
    }
}

CacheStorage::CacheStorage(
        const Attributes & attributes_structure_, const std::string & path_,
        const size_t partitions_count_, const size_t partition_max_size_)
    : attributes_structure(attributes_structure_)
    , path(path_)
    , partition_max_size(partition_max_size_)
    , log(&Poco::Logger::get("CacheStorage"))
{
    for (size_t partition_id = 0; partition_id < partitions_count_; ++partition_id)
        partitions.emplace_back(std::make_unique<CachePartition>(AttributeUnderlyingType::utUInt64,
                attributes_structure, path_, partition_id, partition_max_size));
}

template <typename PresentIdHandler, typename AbsentIdHandler>
void CacheStorage::update(DictionarySourcePtr & source_ptr, const std::vector<Key> & requested_ids,
        PresentIdHandler && on_updated, AbsentIdHandler && on_id_not_found,
        const DictionaryLifetime lifetime, const std::vector<AttributeValueVariant> & null_values)
{
    CurrentMetrics::Increment metric_increment{CurrentMetrics::DictCacheRequests};
    ProfileEvents::increment(ProfileEvents::DictCacheKeysRequested, requested_ids.size());

    std::unordered_map<Key, UInt8> remaining_ids{requested_ids.size()};
    for (const auto id : requested_ids)
        remaining_ids.insert({id, 0});

    const auto now = std::chrono::system_clock::now();

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
                const auto new_keys = createAttributesFromBlock(block, 0, { AttributeUnderlyingType::utUInt64 }).front();
                const auto new_attributes = createAttributesFromBlock(block, 1, attributes_structure);

                const auto & ids = std::get<CachePartition::Attribute::Container<UInt64>>(new_keys.values);

                std::vector<CachePartition::Metadata> metadata(ids.size());

                for (const auto i : ext::range(0, ids.size()))
                {
                    std::uniform_int_distribution<UInt64> distribution{lifetime.min_sec, lifetime.max_sec};
                    metadata[i].setExpiresAt(now + std::chrono::seconds(distribution(rnd_engine)));
                    /// mark corresponding id as found
                    on_updated(ids[i], i, new_attributes);
                    remaining_ids[ids[i]] = 1;
                }

                partitions[0]->appendBlock(new_keys, new_attributes, metadata);
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

    size_t not_found_num = 0, found_num = 0;

    /// Check which ids have not been found and require setting null_value
    CachePartition::Attribute new_keys;
    new_keys.type = AttributeUnderlyingType::utUInt64;
    new_keys.values = std::vector<UInt64>();
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
                new_attributes.back().values = std::vector<TYPE>(); \
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
                // TODO: string support
                break;
            }
        }
    }

    std::vector<CachePartition::Metadata> metadata;

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
        std::get<std::vector<UInt64>>(new_keys.values).push_back(id);

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
                    auto & to_values = std::get<std::vector<TYPE>>(new_attributes[i].values); \
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
                // TODO: string support
                break;
            }
        }

        /// inform caller that the cell has not been found
        on_id_not_found(id);
    }

    if (not_found_num)
        partitions[0]->appendBlock(new_keys, new_attributes, metadata);

    ProfileEvents::increment(ProfileEvents::DictCacheKeysRequestedMiss, not_found_num);
    ProfileEvents::increment(ProfileEvents::DictCacheKeysRequestedFound, found_num);
    ProfileEvents::increment(ProfileEvents::DictCacheRequests);
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
                std::vector<TYPE> values(column->size()); \
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
            // TODO: string support
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
    const size_t partition_max_size_)
    : name(name_)
    , dict_struct(dict_struct_)
    , source_ptr(std::move(source_ptr_))
    , dict_lifetime(dict_lifetime_)
    , path(path_)
    , partition_max_size(partition_max_size_)
    , storage(ext::map<std::vector>(dict_struct.attributes, [](const auto & attribute) { return attribute.underlying_type; }),
            path, 1, partition_max_size)
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
                    out[out_row] = std::get<std::vector<OutputType>>(new_attributes[attribute_index].values)[row];
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

    getItemsString(index, ids, out, [&](const size_t) { return null_value; });
}

void SSDCacheDictionary::getString(
        const std::string & attribute_name, const PaddedPODArray<Key> & ids, const ColumnString * const def, ColumnString * const out) const
{
    const auto index = getAttributeIndex(attribute_name);
    checkAttributeType(name, attribute_name, dict_struct.attributes[index].underlying_type, AttributeUnderlyingType::utString);

    getItemsString(index, ids, out, [&](const size_t row) { return def->getDataAt(row); });
}

void SSDCacheDictionary::getString(
        const std::string & attribute_name, const PaddedPODArray<Key> & ids, const String & def, ColumnString * const out) const
{
    const auto index = getAttributeIndex(attribute_name);
    checkAttributeType(name, attribute_name, dict_struct.attributes[index].underlying_type, AttributeUnderlyingType::utString);

    getItemsString(index, ids, out, [&](const size_t) { return StringRef{def}; });
}

template <typename DefaultGetter>
void SSDCacheDictionary::getItemsString(const size_t attribute_index, const PaddedPODArray<Key> & ids,
        ColumnString * out, DefaultGetter && get_default) const
{
    UNUSED(attribute_index);
    UNUSED(ids);
    UNUSED(out);
    UNUSED(get_default);
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
    //if (!string_arena)
    //    string_arena = std::make_unique<ArenaWithFreeLists>();
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
        const auto max_partition_size = config.getInt(layout_prefix + ".ssd.max_partition_size");
        if (max_partition_size == 0)
            throw Exception{name + ": dictionary of layout 'cache' cannot have 0 cells", ErrorCodes::TOO_SMALL_BUFFER_SIZE};

        const auto path = config.getString(layout_prefix + ".ssd.path");
        if (path.empty())
            throw Exception{name + ": dictionary of layout 'cache' cannot have empty path",
                            ErrorCodes::BAD_ARGUMENTS};

        const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};
        return std::make_unique<SSDCacheDictionary>(name, dict_struct, std::move(source_ptr), dict_lifetime, path, max_partition_size);
    };
    factory.registerLayout("ssd", create_layout, false);
}

}
