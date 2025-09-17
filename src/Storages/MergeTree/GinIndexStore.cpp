// NOLINTBEGIN(clang-analyzer-optin.core.EnumCastOutOfRange)

#include <IO/VarInt.h>
#include <Storages/MergeTree/GinIndexStore.h>
#include <Columns/ColumnString.h>
#include <Common/FST.h>
#include <Common/HashTable/HashSet.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <Compression/CompressionFactory.h>
#include <Compression/ICompressionCodec.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/BloomFilterHash.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>
#include <vector>
#include <unordered_map>
#include <algorithm>

#include "config.h"

#if USE_FASTPFOR
#  include <codecfactory.h>
#  include <fastpfor.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
};

const CompressionCodecPtr & GinCompressionFactory::zstdCodec()
{
    static constexpr auto GIN_COMPRESSION_CODEC = "ZSTD";
    static constexpr auto GIN_COMPRESSION_LEVEL = 1;

    static auto codec = CompressionCodecFactory::instance().get(GIN_COMPRESSION_CODEC, GIN_COMPRESSION_LEVEL);
    return codec;
}

#if USE_FASTPFOR
UInt64 GinPostingListDeltaPforSerialization::serialize(WriteBuffer & buffer, const GinPostingsList & rowids)
{
    std::vector<UInt32> deltas = encodeDeltaScalar(rowids);

    /// FastPFOR requires the output buffer to be "big enough", so +20% buffer is our attempt to comply with that.
    std::vector<UInt32> compressed(static_cast<size_t>(std::ceil(deltas.size() * 1.2)));
    size_t compressed_size = compressed.size();
    if (deltas.size() < FASTPFOR_THRESHOLD)
    {
        std::memcpy(compressed.data(), deltas.data(), sizeof(UInt32) * deltas.size());
        compressed_size = deltas.size();
    }
    else
    {
        codec()->encodeArray(deltas.data(), deltas.size(), compressed.data(), compressed_size);
    }

    UInt64 written_bytes = 0;

    UInt64 num_deltas = deltas.size();
    writeVarUInt(num_deltas, buffer);
    written_bytes += getLengthOfVarUInt(num_deltas);

    writeVarUInt(compressed_size, buffer);
    written_bytes += getLengthOfVarUInt(compressed_size);

    buffer.write(reinterpret_cast<char *>(compressed.data()), compressed_size * sizeof(UInt32));
    written_bytes += compressed_size * sizeof(UInt32);

    return written_bytes;
}

GinPostingsListPtr GinPostingListDeltaPforSerialization::deserialize(ReadBuffer & buffer)
{
    size_t num_deltas = 0;
    size_t compressed_size = 0;
    readVarUInt(num_deltas, buffer);
    readVarUInt(compressed_size, buffer);

    std::vector<UInt32> compressed(compressed_size);
    buffer.readStrict(reinterpret_cast<char *>(compressed.data()), compressed_size * sizeof(UInt32));

    std::vector<UInt32> deltas(num_deltas);
    if (deltas.size() < FASTPFOR_THRESHOLD)
    {
        std::memcpy(deltas.data(), compressed.data(), sizeof(UInt32) * deltas.size());
    }
    else
    {
        codec()->decodeArray(compressed.data(), compressed_size, deltas.data(), num_deltas);
    }

    decodeDeltaScalar(deltas);

    GinPostingsListPtr postings_list = std::make_shared<GinPostingsList>();
    postings_list->addMany(deltas.size(), deltas.data());
    return postings_list;
}

std::shared_ptr<FastPForLib::IntegerCODEC> GinPostingListDeltaPforSerialization::codec()
{
    static thread_local std::shared_ptr<FastPForLib::IntegerCODEC> codec = FastPForLib::simdfastpfor128_codec();
    return codec;
}

std::vector<UInt32> GinPostingListDeltaPforSerialization::encodeDeltaScalar(const GinPostingsList & rowids)
{
    const UInt64 num_rowids = rowids.cardinality();
    std::vector<UInt32> deltas(num_rowids);
    UInt32 prev = 0;
    for (size_t i = 0; const UInt32 rowid : rowids)
    {
        deltas[i] = rowid - prev;
        prev = rowid;
        ++i;
    }
    return deltas;
}

void GinPostingListDeltaPforSerialization::decodeDeltaScalar(std::vector<UInt32> & deltas)
{
    for (size_t i = 1; i < deltas.size(); ++i)
        deltas[i] += deltas[i - 1];
}
#endif

UInt64 GinPostingListRoaringZstdSerialization::serialize(WriteBuffer & buffer, const GinPostingsList & rowids)
{
    const UInt64 num_rowids = rowids.cardinality();

    if (num_rowids < MIN_SIZE_FOR_ROARING_ENCODING)
    {
        std::vector<UInt32> values(num_rowids);
        rowids.toUint32Array(values.data());

        UInt64 header = (num_rowids << 1) | ARRAY_CONTAINER_MASK;

        UInt64 written_bytes = 0;

        writeVarUInt(header, buffer);
        written_bytes += getLengthOfVarUInt(header);

        for (const auto & value : values)
        {
            writeVarUInt(value, buffer);
            written_bytes += getLengthOfVarUInt(value);
        }

        return written_bytes;
    }

    const bool compress = num_rowids >= ROARING_ENCODING_COMPRESSION_CARDINALITY_THRESHOLD;
    const UInt64 uncompressed_size = rowids.getSizeInBytes();

    std::vector<char> buf(uncompressed_size);
    rowids.write(buf.data());

    UInt64 header = uncompressed_size;
    if (compress)
    {
        Memory<> memory;
        const auto & codec = GinCompressionFactory::zstdCodec();
        memory.resize(codec->getCompressedReserveSize(static_cast<UInt32>(uncompressed_size)));
        auto compressed_size = codec->compress(buf.data(), static_cast<UInt32>(uncompressed_size), memory.data());

        header = (header << 2) | (ROARING_COMPRESSED_MASK << 1) | ROARING_CONTAINER_MASK;

        UInt64 written_bytes = 0;

        writeVarUInt(header, buffer);
        written_bytes += getLengthOfVarUInt(header);

        writeVarUInt(compressed_size, buffer);
        written_bytes += getLengthOfVarUInt(compressed_size);

        buffer.write(memory.data(), compressed_size);
        written_bytes += compressed_size;

        return written_bytes;
    }
    else
    {
        header = (header << 2) | (ROARING_UNCOMPRESSED_MASK << 1) | ROARING_CONTAINER_MASK;

        UInt64 written_bytes = 0;

        writeVarUInt(header, buffer);
        written_bytes += getLengthOfVarUInt(header);

        buffer.write(buf.data(), uncompressed_size);
        written_bytes += uncompressed_size;

        return written_bytes;
    }
}

GinPostingsListPtr GinPostingListRoaringZstdSerialization::deserialize(ReadBuffer & buffer)
{
    /// Header value maps into following states:
    /// The lowest bit indicates if values are stored as an array or Roaring bitmap
    /// In case of array container, the rest of the bits is the number of entries in the array.
    /// In case of Roaring bitmap, the second lowest bit indicates if Roaring bitmap is compressed or uncompressed, the rest of the bits is the uncompressed size.
    UInt64 header = 0;
    readVarUInt(header, buffer);

    if (header & ARRAY_CONTAINER_MASK) /// Array
    {
        UInt64 num_entries = (header >> 1);
        std::vector<UInt32> values(num_entries);
        for (size_t i = 0; i < num_entries; ++i)
            readVarUInt(values[i], buffer);

        GinPostingsListPtr postings_list = std::make_shared<GinPostingsList>();
        postings_list->addMany(values.size(), values.data());
        return postings_list;
    }
    else /// Roaring
    {
        header >>= 1;

        const bool compressed = header & ROARING_COMPRESSED_MASK;
        const UInt64 uncompressed_size = (header >> 1);
        if (compressed)
        {
            size_t compressed_size = 0;
            readVarUInt(compressed_size, buffer);
            std::vector<char> buf(compressed_size);
            buffer.readStrict(reinterpret_cast<char *>(buf.data()), compressed_size);

            Memory<> memory;
            memory.resize(uncompressed_size);
            const auto & codec = GinCompressionFactory::zstdCodec();
            codec->decompress(buf.data(), static_cast<UInt32>(compressed_size), memory.data());

            return std::make_shared<GinPostingsList>(GinPostingsList::read(memory.data()));
        }
        else
        {
            /// Deserialize uncompressed roaring bitmap
            std::vector<char> buf(uncompressed_size);
            buffer.readStrict(buf.data(), uncompressed_size);
            return std::make_shared<GinPostingsList>(GinPostingsList::read(buf.data()));
        }
    }
}

bool GinPostingsListBuilder::contains(UInt32 row_id) const
{
    return rowids.contains(row_id);
}

void GinPostingsListBuilder::add(UInt32 row_id)
{
    rowids.add(row_id);
}

UInt64 GinPostingsListBuilder::serialize(WriteBuffer & buffer)
{
    rowids.runOptimize();

    UInt64 written_bytes = 0;
#if USE_FASTPFOR
    auto ch = static_cast<char>(Serialization::DELTA_PFOR);
    writeChar(ch, buffer);
    written_bytes += 1;

    written_bytes += GinPostingListDeltaPforSerialization::serialize(buffer, rowids);
#else
    auto ch = static_cast<char>(Serialization::ROARING_ZSTD);
    writeChar(ch, buffer);
    written_bytes += 1;

    written_bytes += GinPostingListRoaringZstdSerialization::serialize(buffer, rowids);
#endif
    return written_bytes;
}

GinPostingsListPtr GinPostingsListBuilder::deserialize(ReadBuffer & buffer)
{
    UInt8 serialization = 0;
    readBinary(serialization, buffer);

    if (serialization == static_cast<std::underlying_type_t<Serialization>>(Serialization::DELTA_PFOR))
    {
#if USE_FASTPFOR
        return GinPostingListDeltaPforSerialization::deserialize(buffer);
#else
        throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "Text index: Posting list is compressed by Delta and FastPfor, but library is disabled.");
#endif
    }

    return GinPostingListRoaringZstdSerialization::deserialize(buffer);
}

GinDictionaryBloomFilter::GinDictionaryBloomFilter(UInt64 unique_count_, size_t bits_per_rows_, size_t num_hashes_)
    : unique_count(unique_count_)
    , bits_per_row(bits_per_rows_)
    , num_hashes(num_hashes_)
    , bloom_filter(((bits_per_row * unique_count) + sizeof(BloomFilter::UnderType) - 1) / sizeof(BloomFilter::UnderType), num_hashes, 0)
{
}

void GinDictionaryBloomFilter::add(std::string_view token)
{
    bloom_filter.add(token.data(), token.size());
}

bool GinDictionaryBloomFilter::contains(std::string_view token)
{
    return bloom_filter.find(token.data(), token.size());
}

UInt64 GinDictionaryBloomFilter::serialize(WriteBuffer & write_buffer)
{
    UInt64 bytes_written = 0;
    const size_t filter_size_bytes = bloom_filter.getFilter().size() * sizeof(BloomFilter::UnderType);

    writeVarUInt(unique_count, write_buffer);
    bytes_written += getLengthOfVarUInt(unique_count);

    writeVarUInt(bits_per_row, write_buffer);
    bytes_written += getLengthOfVarUInt(bits_per_row);

    writeVarUInt(num_hashes, write_buffer);
    bytes_written += getLengthOfVarUInt(num_hashes);

    writeVarUInt(filter_size_bytes, write_buffer);
    bytes_written += getLengthOfVarUInt(filter_size_bytes);

    write_buffer.write(reinterpret_cast<const char *>(bloom_filter.getFilter().data()), filter_size_bytes);
    bytes_written += filter_size_bytes;

    return bytes_written;
}

std::unique_ptr<GinDictionaryBloomFilter> GinDictionaryBloomFilter::deserialize(ReadBuffer & read_buffer)
{
    UInt64 unique_count;
    readVarUInt(unique_count, read_buffer);

    UInt64 bits_per_row = 0;
    readVarUInt(bits_per_row, read_buffer);

    UInt64 num_hashes = 0;
    readVarUInt(num_hashes, read_buffer);

    UInt64 filter_size_bytes = 0;
    readVarUInt(filter_size_bytes, read_buffer);

    auto gin_bloom_filter = std::make_unique<GinDictionaryBloomFilter>(unique_count, bits_per_row, num_hashes);
    read_buffer.readStrict(reinterpret_cast<char *>(gin_bloom_filter->bloom_filter.getFilter().data()), filter_size_bytes);

    return gin_bloom_filter;
}

GinIndexStore::GinIndexStore(const String & name_, DataPartStoragePtr storage_)
    : name(name_)
    , storage(storage_)
{
}

GinIndexStore::GinIndexStore(
    const String & name_,
    DataPartStoragePtr storage_,
    MutableDataPartStoragePtr data_part_storage_builder_,
    UInt64 segment_digestion_threshold_bytes_,
    double bloom_filter_false_positive_rate_)
    : name(name_)
    , storage(storage_)
    , data_part_storage_builder(data_part_storage_builder_)
    , segment_digestion_threshold_bytes(segment_digestion_threshold_bytes_)
    , bloom_filter_false_positive_rate(bloom_filter_false_positive_rate_)
{
}

bool GinIndexStore::exists() const
{
    String segment_id_file_name = getName() + GIN_SEGMENT_ID_FILE_TYPE;
    return storage->existsFile(segment_id_file_name);
}

UInt32 GinIndexStore::getNextRowIdRange(size_t n)
{
    UInt32 result = current_segment.next_row_id;
    current_segment.next_row_id += n;
    return result;
}

UInt32 GinIndexStore::getNextSegmentId()
{
    std::lock_guard guard(mutex);

    if (next_available_segment_id == 0)
        initSegmentId();

    UInt32 segment_id = next_available_segment_id;
    ++next_available_segment_id;
    return segment_id;
}

namespace
{

GinIndexStore::Format getFormatVersion(uint8_t version)
{
    using FormatAsInt = std::underlying_type_t<GinIndexStore::Format>;
    switch (version)
    {
        case static_cast<FormatAsInt>(GinIndexStore::Format::v1):
            return GinIndexStore::Format::v1;
        default:
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Text Index: segment ID file contains an unsupported version '{}'", version);
    }
}

}

UInt32 GinIndexStore::getNumOfSegments()
{
    if (cached_segment_num)
        return cached_segment_num;

    String segment_id_file_name = getName() + GIN_SEGMENT_ID_FILE_TYPE;
    if (!storage->existsFile(segment_id_file_name))
        return 0;

    UInt32 result = 0;
    {
        std::unique_ptr<DB::ReadBufferFromFileBase> istr = this->storage->readFile(segment_id_file_name, {}, std::nullopt);

        uint8_t version = 0;
        readBinary(version, *istr);

        getFormatVersion(version);

        readVarUInt(result, *istr);
    }

    cached_segment_num = result - 1;
    return cached_segment_num;
}

GinIndexStore::Format GinIndexStore::getVersion()
{
    String segment_id_file_name = getName() + GIN_SEGMENT_ID_FILE_TYPE;
    if (!storage->existsFile(segment_id_file_name))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Text Index: segment ID file does not exist");

    std::unique_ptr<DB::ReadBufferFromFileBase> istr = this->storage->readFile(segment_id_file_name, {}, std::nullopt);
    uint8_t version = 0;
    readBinary(version, *istr);
    return getFormatVersion(version);
}

bool GinIndexStore::needToWriteCurrentSegment() const
{
    /// segment_digestion_threshold_bytes != 0 means GinIndexStore splits the index data into separate segments.
    /// In case it's equal to 0 (zero), segment size is unlimited. Therefore, there will be a single segment.
    return (segment_digestion_threshold_bytes != UNLIMITED_SEGMENT_DIGESTION_THRESHOLD_BYTES) && (current_size_bytes > segment_digestion_threshold_bytes);
}

void GinIndexStore::finalize()
{
    if (!token_postings_lists.empty())
    {
        writeSegment();
        writeSegmentId();
    }

    if (segment_descriptor_file_stream)
        segment_descriptor_file_stream->finalize();

    if (bloom_filter_file_stream)
        bloom_filter_file_stream->finalize();

    if (dict_file_stream)
        dict_file_stream->finalize();

    if (postings_file_stream)
        postings_file_stream->finalize();
}

void GinIndexStore::cancel() noexcept
{
    if (segment_descriptor_file_stream)
        segment_descriptor_file_stream->cancel();

    if (bloom_filter_file_stream)
        bloom_filter_file_stream->cancel();

    if (dict_file_stream)
        dict_file_stream->cancel();

    if (postings_file_stream)
        postings_file_stream->cancel();
}

GinIndexStore::Statistics::Statistics(const GinIndexStore & store)
    : num_tokens(store.token_postings_lists.size())
    , current_size_bytes(store.current_size_bytes)
    , segment_descriptor_file_size(store.segment_descriptor_file_stream ? store.segment_descriptor_file_stream->count() : 0)
    , bloom_filter_file_size(store.bloom_filter_file_stream ? store.bloom_filter_file_stream->count() : 0)
    , dictionary_file_size(store.dict_file_stream ? store.dict_file_stream->count() : 0)
    , posting_lists_file_size(store.postings_file_stream ? store.postings_file_stream->count() : 0)
{
}

String GinIndexStore::Statistics::toString() const
{
    return fmt::format(
        "number of tokens = {}, tokens byte size = {}, segment descriptor size = {}, bloom filter size = {}, dictionary size = {}, posting lists size = {}",
        num_tokens,
        ReadableSize(current_size_bytes),
        ReadableSize(segment_descriptor_file_size),
        ReadableSize(bloom_filter_file_size),
        ReadableSize(dictionary_file_size),
        ReadableSize(posting_lists_file_size));
}

GinIndexStore::Statistics GinIndexStore::getStatistics()
{
    return Statistics(*this);
}

void GinIndexStore::initSegmentId()
{
    String segment_id_file_name = getName() + GIN_SEGMENT_ID_FILE_TYPE;

    UInt32 segment_id;
    if (storage->existsFile(segment_id_file_name))
    {
        std::unique_ptr<DB::ReadBufferFromFileBase> istr = this->storage->readFile(segment_id_file_name, {}, std::nullopt);

        uint8_t version = 0;
        readBinary(version, *istr);

        getFormatVersion(version);

        readVarUInt(segment_id, *istr);
    }
    else
        segment_id = 1;

    next_available_segment_id = segment_id;
}

void GinIndexStore::initFileStreams()
{
    String segment_descriptor_file_name = getName() + GIN_SEGMENT_DESCRIPTOR_FILE_TYPE;
    String bloom_filter_file_name = getName() + GIN_BLOOM_FILTER_FILE_TYPE;
    String dict_file_name = getName() + GIN_DICTIONARY_FILE_TYPE;
    String postings_file_name = getName() + GIN_POSTINGS_FILE_TYPE;

    segment_descriptor_file_stream = data_part_storage_builder->writeFile(segment_descriptor_file_name, 4096, WriteMode::Append, {});
    bloom_filter_file_stream = data_part_storage_builder->writeFile(bloom_filter_file_name, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append, {});
    dict_file_stream = data_part_storage_builder->writeFile(dict_file_name, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append, {});
    postings_file_stream = data_part_storage_builder->writeFile(postings_file_name, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append, {});
}

void GinIndexStore::writeSegmentId()
{
    String segment_id_file_name = getName() + GIN_SEGMENT_ID_FILE_TYPE;
    std::unique_ptr<DB::WriteBufferFromFileBase> ostr = this->data_part_storage_builder->writeFile(segment_id_file_name, 8, {});

    /// Write version
    writeChar(static_cast<char>(CURRENT_GIN_FILE_FORMAT_VERSION), *ostr);

    writeVarUInt(next_available_segment_id, *ostr);
    ostr->sync();
    ostr->finalize();
}

namespace
{

/// Initialize bloom filter from tokens from the token dictionary
GinDictionaryBloomFilter initializeBloomFilter(
    const GinIndexStore::GinTokenPostingsLists & token_postings_lists,
    double bloom_filter_false_positive_rate)
{
    size_t unique_token_count = token_postings_lists.size(); /// token_postings_lists is a dictionary
    const auto & [bits_per_rows, num_hashes] = BloomFilterHash::calculationBestPractices(bloom_filter_false_positive_rate);
    GinDictionaryBloomFilter bloom_filter(unique_token_count, bits_per_rows, num_hashes);
    for (const auto & [token, _] : token_postings_lists)
        bloom_filter.add(token);
    return bloom_filter;
}

}

void GinIndexStore::writeSegment()
{
    if (segment_descriptor_file_stream == nullptr)
        initFileStreams();

    LOG_TRACE(
        logger, "Start writing text index '{}' segment id {} of part '{}'", name, current_segment.segment_id, storage->getPartDirectory());
    Statistics before_write_segment_stats = getStatistics();

    /// Write segment descriptor
    segment_descriptor_file_stream->write(reinterpret_cast<char *>(&current_segment), sizeof(GinSegmentDescriptor));

    using TokenPostingsBuilderPair = std::pair<std::string_view, GinPostingsListBuilderPtr>;
    using TokenPostingsBuilderPairs = std::vector<TokenPostingsBuilderPair>;

    TokenPostingsBuilderPairs token_postings_list_pairs;
    token_postings_list_pairs.reserve(token_postings_lists.size());
    for (const auto & [token, postings_list] : token_postings_lists)
        token_postings_list_pairs.push_back({token, postings_list});

    GinDictionaryBloomFilter bloom_filter = initializeBloomFilter(token_postings_lists, bloom_filter_false_positive_rate);

    /// Sort token-postings list pairs since all tokens have to be added in FST in sorted order
    std::ranges::sort(token_postings_list_pairs,
                    [](const TokenPostingsBuilderPair & x, const TokenPostingsBuilderPair & y)
                    {
                        return x.first < y.first;
                    });

    /// Write postings
    std::vector<UInt64> posting_list_byte_sizes(token_postings_lists.size(), 0);

    for (size_t i = 0; const auto & [token, postings_list] : token_postings_list_pairs)
    {
        auto posting_list_byte_size = postings_list->serialize(*postings_file_stream);

        posting_list_byte_sizes[i] = posting_list_byte_size;
        i++;
        current_segment.postings_start_offset += posting_list_byte_size;
    }

    /// Write bloom filter
    current_segment.bloom_filter_start_offset += bloom_filter.serialize(*bloom_filter_file_stream);

    /// Write item dictionary
    std::vector<UInt8> buffer;
    WriteBufferFromVector<std::vector<UInt8>> write_buf(buffer);
    FST::Builder fst_builder(write_buf);

    UInt64 offset = 0;
    for (size_t i = 0; const auto & [token, postings_list] : token_postings_list_pairs)
    {
        fst_builder.add(token, offset);
        offset += posting_list_byte_sizes[i];
        i++;
    }

    fst_builder.build();
    write_buf.finalize();

    const size_t uncompressed_size = buffer.size();
    const bool compress_fst = uncompressed_size >= FST_SIZE_COMPRESSION_THRESHOLD;

    /// Header contains the uncompressed size and a single bit to indicate whether FST is compressed or uncompressed.
    UInt64 fst_size_header = (uncompressed_size << 1) | (compress_fst ? 0x1 : 0x0);
    /// Write FST size header
    writeVarUInt(fst_size_header, *dict_file_stream);
    current_segment.dict_start_offset += getLengthOfVarUInt(fst_size_header);

    if (compress_fst)
    {
        const auto & codec = GinCompressionFactory::zstdCodec();
        Memory<> memory;
        memory.resize(codec->getCompressedReserveSize(static_cast<UInt32>(uncompressed_size)));
        auto compressed_size = codec->compress(reinterpret_cast<char *>(buffer.data()), uncompressed_size, memory.data());

        /// Write FST compressed size
        writeVarUInt(compressed_size, *dict_file_stream);
        current_segment.dict_start_offset += getLengthOfVarUInt(compressed_size);

        /// Write FST compressed blob
        dict_file_stream->write(memory.data(), compressed_size);
        current_segment.dict_start_offset += compressed_size;
    }
    else
    {
        /// Write FST uncompressed blob
        dict_file_stream->write(reinterpret_cast<char *>(buffer.data()), uncompressed_size);
        current_segment.dict_start_offset += uncompressed_size;
    }

    auto statistics = getStatistics() - before_write_segment_stats;
    LOG_TRACE(
        logger,
        "Done writing text index '{}' segment id {} of part '{}': {}",
        name,
        current_segment.segment_id,
        storage->getPartDirectory(),
        statistics.toString());

    current_size_bytes = 0;
    token_postings_lists.clear();
    current_segment.segment_id = getNextSegmentId();

    segment_descriptor_file_stream->sync();
    bloom_filter_file_stream->sync();
    dict_file_stream->sync();
    postings_file_stream->sync();
}

GinIndexStoreDeserializer::GinIndexStoreDeserializer(const GinIndexStorePtr & store_)
    : store(store_)
{
    initFileStreams();
}

void GinIndexStoreDeserializer::initFileStreams()
{
    String segment_descriptors_file_name = store->getName() + GinIndexStore::GIN_SEGMENT_DESCRIPTOR_FILE_TYPE;
    String bloom_filter_file_name = store->getName() + GinIndexStore::GIN_BLOOM_FILTER_FILE_TYPE;
    String dict_file_name = store->getName() + GinIndexStore::GIN_DICTIONARY_FILE_TYPE;
    String postings_file_name = store->getName() + GinIndexStore::GIN_POSTINGS_FILE_TYPE;

    segment_descriptor_file_stream = store->storage->readFile(segment_descriptors_file_name, {}, std::nullopt);
    bloom_filter_file_stream = store->storage->readFile(bloom_filter_file_name, {}, std::nullopt);
    dict_file_stream = store->storage->readFile(dict_file_name, {}, std::nullopt);
    postings_file_stream = store->storage->readFile(postings_file_name, {}, std::nullopt);
}

void GinIndexStoreDeserializer::readSegments()
{
    UInt32 num_segments = store->getNumOfSegments();
    if (num_segments == 0)
        return;

    chassert(segment_descriptor_file_stream != nullptr);

    LOG_TRACE(logger, "Start reading text index '{}' segments of part '{}'", store->getName(), store->storage->getPartDirectory());

    if (store->getVersion() == GinIndexStore::Format::v1)
    {
        std::vector<GinSegmentDescriptor> segment_descriptors(num_segments);
        segment_descriptor_file_stream->readStrict(reinterpret_cast<char *>(segment_descriptors.data()), num_segments * sizeof(GinSegmentDescriptor));
        for (UInt32 i = 0; i < num_segments; ++i)
        {
            auto dictionary = std::make_shared<GinDictionary>();
            dictionary->postings_start_offset = segment_descriptors[i].postings_start_offset;
            dictionary->dict_start_offset = segment_descriptors[i].dict_start_offset;
            dictionary->bloom_filter_start_offset = segment_descriptors[i].bloom_filter_start_offset;
            store->segment_dictionaries[segment_descriptors[i].segment_id] = dictionary;
        }
    }

    LOG_TRACE(
        logger,
        "Done reading text index '{}' segments of part '{}': number of segments = {}",
        store->getName(),
        store->storage->getPartDirectory(),
        num_segments);
}

void GinIndexStoreDeserializer::prepareSegmentsForReading()
{
    for (UInt32 segment = 0; segment < store->getNumOfSegments(); ++segment)
        prepareSegmentForReading(segment);
}

void GinIndexStoreDeserializer::prepareSegmentForReading(UInt32 segment_id)
{
    /// Check validity of segment_id
    auto it = store->segment_dictionaries.find(segment_id);
    if (it == store->segment_dictionaries.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid segment id {}", segment_id);

    LOG_TRACE(
        logger,
        "Start reading the bloom filter of text index '{}' segment id {} of part '{}'",
        store->getName(),
        segment_id,
        store->storage->getPartDirectory());

    const GinDictionaryPtr & dictionary = it->second;

    switch (auto version = store->getVersion(); version)
    {
        case GinIndexStore::Format::v1: {
            /// V1 supports bloom filter, so we can delay reading a segment until it's needed.

            /// Set file pointer of filter file
            chassert(bloom_filter_file_stream != nullptr);
            bloom_filter_file_stream->seek(dictionary->bloom_filter_start_offset, SEEK_SET);
            dictionary->bloom_filter = GinDictionaryBloomFilter::deserialize(*bloom_filter_file_stream);
            break;
        }
    }

    LOG_TRACE(
        logger,
        "Done reading the bloom filter of text index '{}' segment id {} of part '{}': size = {}",
        store->getName(),
        segment_id,
        store->storage->getPartDirectory(),
        ReadableSize(bloom_filter_file_stream->count() - dictionary->bloom_filter_start_offset));
}

void GinIndexStoreDeserializer::readSegmentFST(UInt32 segment_id, GinDictionary & dictionary)
{
    /// Set file pointer of dictionary file
    chassert(dict_file_stream != nullptr);
    dict_file_stream->seek(dictionary.dict_start_offset, SEEK_SET);

    LOG_TRACE(
        logger,
        "Start reading the dictionary (FST) of text index '{}' segment id {} of part '{}'",
        store->getName(),
        segment_id,
        store->storage->getPartDirectory());

    dictionary.fst = std::make_unique<FST::FiniteStateTransducer>();

    switch (auto version = store->getVersion(); version)
    {
        case GinIndexStore::Format::v1: {
            /// Read FST size header
            UInt64 fst_size_header;
            readVarUInt(fst_size_header, *dict_file_stream);

            size_t uncompressed_fst_size = fst_size_header >> 1;
            dictionary.fst->getData().clear();
            dictionary.fst->getData().resize(uncompressed_fst_size);
            if (fst_size_header & 0x1) /// FST is compressed
            {
                /// Read compressed FST size
                size_t compressed_fst_size = 0;
                readVarUInt(compressed_fst_size, *dict_file_stream);
                /// Read compressed FST blob
                std::vector<char> buf(compressed_fst_size);
                dict_file_stream->readStrict(buf.data(), compressed_fst_size);
                const auto & codec = DB::GinCompressionFactory::zstdCodec();
                codec->decompress(
                    buf.data(),
                    static_cast<UInt32>(compressed_fst_size),
                    reinterpret_cast<char *>(dictionary.fst->getData().data()));
            }
            else
            {
                /// Read uncompressed FST blob
                dict_file_stream->readStrict(reinterpret_cast<char *>(dictionary.fst->getData().data()), uncompressed_fst_size);
            }
            break;
        }
    }

    LOG_TRACE(
        logger,
        "Done reading the dictionary (FST) of text index '{}' segment id {} of part '{}': size = {}",
        store->getName(),
        segment_id,
        store->storage->getPartDirectory(),
        ReadableSize(dict_file_stream->count() - dictionary.dict_start_offset));
}

GinSegmentPostingsLists GinIndexStoreDeserializer::readSegmentPostingsLists(const String & token)
{
    chassert(postings_file_stream != nullptr);

    GinSegmentPostingsLists segment_postings_lists;
    for (auto const & segment_dictionary : store->segment_dictionaries)
    {
        UInt32 segment_id = segment_dictionary.first;
        const GinDictionaryPtr & dictionary = segment_dictionary.second;

        FST::FiniteStateTransducer::Output fst_output;
        {
            std::lock_guard guard(dictionary->fst_mutex);

            if (dictionary->fst == nullptr)
            {
                /// Segment dictionary is not loaded, first check if the token is in bloom filter
                if (dictionary->bloom_filter && !dictionary->bloom_filter->contains(token))
                    continue;

                /// token might be in segment dictionary
                readSegmentFST(segment_id, *dictionary);
            }

            fst_output = dictionary->fst->getOutput(token);
            if (!fst_output.found)
                continue;
        }

        LOG_TRACE(
            logger,
            "Start reading the posting list for token '{}' from text index '{}' segment id {} of part '{}'",
            token,
            store->getName(),
            segment_id,
            store->storage->getPartDirectory());

        /// Set postings file pointer for reading postings list
        postings_file_stream->seek(segment_dictionary.second->postings_start_offset + fst_output.offset, SEEK_SET);

        /// Read posting list
        auto postings_list = GinPostingsListBuilder::deserialize(*postings_file_stream);
        segment_postings_lists[segment_id] = postings_list;

        LOG_TRACE(
            logger,
            "Done reading the posting list for token '{}' from text index '{}' segment id {} of part '{}': size = {}",
            token,
            store->getName(),
            segment_id,
            store->storage->getPartDirectory(),
            ReadableSize(postings_file_stream->count() - segment_dictionary.second->postings_start_offset));
    }
    return segment_postings_lists;
}

GinPostingsListsCachePtr GinIndexStoreDeserializer::createPostingsListsCacheFromTokens(const std::vector<String> & tokens)
{
    auto postings_lists_cache = std::make_shared<GinPostingsListsCache>();
    for (const auto & token : tokens)
    {
        /// Make sure don't read for duplicated tokens
        if (postings_lists_cache->contains(token))
            continue;

        auto segment_postings_lists = readSegmentPostingsLists(token);
        (*postings_lists_cache)[token] = segment_postings_lists;
    }
    return postings_lists_cache;
}

GinPostingsListsCachePtr GinPostingsListsCacheForStore::getPostingsLists(const String & query_string) const
{
    auto it = cache.find(query_string);
    if (it == cache.end())
        return nullptr;
    return it->second;
}

GinIndexStoreFactory & GinIndexStoreFactory::instance()
{
    static GinIndexStoreFactory instance;
    return instance;
}

GinIndexStorePtr GinIndexStoreFactory::get(const String & name, DataPartStoragePtr storage)
{
    const String & part_path = storage->getRelativePath();
    String key = name + ":" + part_path;

    std::lock_guard lock(mutex);
    GinIndexStores::const_iterator it = stores.find(key);

    if (it == stores.end())
    {
        GinIndexStorePtr store = std::make_shared<GinIndexStore>(name, storage);
        if (!store->exists())
            return nullptr;

        GinIndexStoreDeserializer deserializer(store);
        deserializer.readSegments();
        deserializer.prepareSegmentsForReading();

        stores[key] = store;

        return store;
    }
    return it->second;
}

void GinIndexStoreFactory::remove(const String & part_path)
{
    std::lock_guard lock(mutex);
    for (auto it = stores.begin(); it != stores.end();)
    {
        if (it->first.find(part_path) != String::npos)
            it = stores.erase(it);
        else
            ++it;
    }
}

bool isGinFile(const String & file_name)
{
    return file_name.ends_with(GinIndexStore::GIN_SEGMENT_ID_FILE_TYPE)
        || file_name.ends_with(GinIndexStore::GIN_SEGMENT_DESCRIPTOR_FILE_TYPE)
        || file_name.ends_with(GinIndexStore::GIN_BLOOM_FILTER_FILE_TYPE)
        || file_name.ends_with(GinIndexStore::GIN_DICTIONARY_FILE_TYPE)
        || file_name.ends_with(GinIndexStore::GIN_POSTINGS_FILE_TYPE);
}
}

// NOLINTEND(clang-analyzer-optin.core.EnumCastOutOfRange)
