// NOLINTBEGIN(clang-analyzer-optin.core.EnumCastOutOfRange)

#include <Storages/MergeTree/GinIndexStore.h>
#include <Columns/ColumnString.h>
#include <Common/FST.h>
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

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_FORMAT_VERSION;
};

const CompressionCodecPtr & GinIndexCompressionFactory::zstdCodec()
{
    static constexpr auto GIN_COMPRESSION_CODEC = "ZSTD";
    static constexpr auto GIN_COMPRESSION_LEVEL = 1;

    static auto codec = CompressionCodecFactory::instance().get(GIN_COMPRESSION_CODEC, GIN_COMPRESSION_LEVEL);
    return codec;
}

bool GinIndexPostingsBuilder::contains(UInt32 row_id) const
{
    return rowids.contains(row_id);
}

void GinIndexPostingsBuilder::add(UInt32 row_id)
{
    rowids.add(row_id);
}

UInt64 GinIndexPostingsBuilder::serialize(WriteBuffer & buffer)
{
    rowids.runOptimize();

    const UInt64 cardinality = rowids.cardinality();

    if (cardinality < MIN_SIZE_FOR_ROARING_ENCODING)
    {
        std::vector<UInt32> values(cardinality);
        rowids.toUint32Array(values.data());

        UInt64 header = (cardinality << 1) | ARRAY_CONTAINER_MASK;
        writeVarUInt(header, buffer);

        UInt64 written_bytes = getLengthOfVarUInt(header);
        for (const auto & value : values)
        {
            writeVarUInt(value, buffer);
            written_bytes += getLengthOfVarUInt(value);
        }

        return written_bytes;
    }

    const bool compress = cardinality >= ROARING_ENCODING_COMPRESSION_CARDINALITY_THRESHOLD;
    const UInt64 uncompressed_size = rowids.getSizeInBytes();

    std::vector<char> buf(uncompressed_size);
    rowids.write(buf.data());

    UInt64 header = uncompressed_size;
    if (compress)
    {
        Memory<> memory;
        const auto & codec = GinIndexCompressionFactory::zstdCodec();
        memory.resize(codec->getCompressedReserveSize(static_cast<UInt32>(uncompressed_size)));
        auto compressed_size = codec->compress(buf.data(), static_cast<UInt32>(uncompressed_size), memory.data());

        header = (header << 2) | (ROARING_COMPRESSED_MASK << 1) | ROARING_CONTAINER_MASK;

        writeVarUInt(header, buffer);
        writeVarUInt(compressed_size, buffer);
        buffer.write(memory.data(), compressed_size);

        return getLengthOfVarUInt(header) + getLengthOfVarUInt(compressed_size) + compressed_size;
    }
    else
    {
        header = (header << 2) | (ROARING_UNCOMPRESSED_MASK << 1) | ROARING_CONTAINER_MASK;

        writeVarUInt(header, buffer);
        buffer.write(buf.data(), uncompressed_size);

        return getLengthOfVarUInt(header) + uncompressed_size;
    }
}

GinIndexPostingsListPtr GinIndexPostingsBuilder::deserialize(ReadBuffer & buffer)
{
    /**
     * Header value maps into following states:
     * The lowest bit indicates if values are stored as an array or Roaring bitmap
     * In case of array container, the rest of the bits is the number of entries in the array.
     * In case of Roaring bitmap, the second lowest bit indicates if Roaring bitmap is compressed or uncompressed, the rest of the bits is the uncompressed size.
     */
    UInt64 header = 0;
    readVarUInt(header, buffer);

    if (header & ARRAY_CONTAINER_MASK) /// Array
    {
        UInt64 num_entries = (header >> 1);
        std::vector<UInt32> values(num_entries);
        for (size_t i = 0; i < num_entries; ++i)
            readVarUInt(values[i], buffer);

        GinIndexPostingsListPtr postings_list = std::make_shared<GinIndexPostingsList>();
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
            const auto & codec = GinIndexCompressionFactory::zstdCodec();
            codec->decompress(buf.data(), static_cast<UInt32>(compressed_size), memory.data());

            return std::make_shared<GinIndexPostingsList>(GinIndexPostingsList::read(memory.data()));
        }
        else
        {
            /// Deserialize uncompressed roaring bitmap
            std::vector<char> buf(uncompressed_size);
            buffer.readStrict(buf.data(), uncompressed_size);
            return std::make_shared<GinIndexPostingsList>(GinIndexPostingsList::read(buf.data()));
        }
    }
}

GinSegmentDictionaryBloomFilter::GinSegmentDictionaryBloomFilter(double max_conflict_probability, UInt64 max_token_size_)
{
    const auto & [bits_per_row, num_hash_functions] = BloomFilterHash::calculationBestPractices(max_conflict_probability);
    filter_size = bits_per_row;
    hashes = num_hash_functions;
    bloom_filter = std::make_unique<BloomFilter>(filter_size, hashes, 0);

    /// Use first and last [max_token_size / 2] characters and max is 7.
    first_characters = max_token_size_ >= 14 ? 7 : ((max_token_size_ + 1) / 2);
    last_characters = max_token_size_ >= 14 ? 7 : (max_token_size_ - first_characters);
}

void GinSegmentDictionaryBloomFilter::add(const char * token, UInt64 size)
{
    if (size <= (first_characters + last_characters))
    {
        bloom_filter->add(token, size);
    }
    else
    {
        bloom_filter->add(token, first_characters);
        bloom_filter->add(token + (size - last_characters), last_characters);
    }
}

bool GinSegmentDictionaryBloomFilter::contains(const char * token, UInt64 size)
{
    if (size <= (first_characters + last_characters))
        return bloom_filter->find(token, size);
    return bloom_filter->find(token, first_characters) && bloom_filter->find(token + (size - last_characters), last_characters);
}

UInt64 GinSegmentDictionaryBloomFilter::serialize(WriteBuffer & write_buffer)
{
    /// We need around 15 bits (~2 bytes) to store all values
    /// Filter size can be max 20 (5 bits) [offset = 10].
    /// Hashes can be max 15 (4 bits) [offset = 6].
    /// First characters can be max 7 (3 bits) [offset = 3].
    /// Last characters can be max 7 (3 bits) [offset = 0].
    UInt16 header = (filter_size << 10) | (hashes << 6) | (first_characters << 3) | (last_characters);
    writeVarUInt(header, write_buffer);
    write_buffer.write(reinterpret_cast<const char *>(bloom_filter->getFilter().data()), filter_size);
    return getLengthOfVarUInt(header) + filter_size;
}

void GinSegmentDictionaryBloomFilter::deserialize(ReadBuffer & read_buffer)
{
    {
        UInt16 header = 0;
        readVarUInt(header, read_buffer);
        filter_size = header >> 10;             // 5 bits
        hashes = (header >> 6) & 0xf;           // 4 bits
        first_characters = (header >> 3) & 0x7; // 3 bits
        last_characters = header & 0x7;         // 3 bits
    }
    bloom_filter = std::make_unique<BloomFilter>(filter_size, hashes, 0);
    read_buffer.readStrict(reinterpret_cast<char *>(bloom_filter->getFilter().data()), filter_size);
}

GinIndexStore::GinIndexStore(const String & name_, DataPartStoragePtr storage_)
    : name(name_)
    , storage(storage_)
{
}

GinIndexStore::GinIndexStore(const String & name_, DataPartStoragePtr storage_, MutableDataPartStoragePtr data_part_storage_builder_, UInt64 max_digestion_size_)
    : name(name_)
    , storage(storage_)
    , data_part_storage_builder(data_part_storage_builder_)
    , max_digestion_size(max_digestion_size_)
{
}

bool GinIndexStore::exists() const
{
    String segment_id_file_name = getName() + GIN_SEGMENT_ID_FILE_TYPE;
    return storage->existsFile(segment_id_file_name);
}

UInt32 GinIndexStore::getNextSegmentIDRange(size_t n)
{
    std::lock_guard guard(mutex);

    if (next_available_segment_id == 0)
        initSegmentId();

    UInt32 segment_id = next_available_segment_id;
    next_available_segment_id += n;
    return segment_id;
}

UInt32 GinIndexStore::getNextRowIDRange(size_t numIDs)
{
    UInt32 result = current_segment.next_row_id;
    current_segment.next_row_id += numIDs;
    return result;
}

UInt32 GinIndexStore::getNextSegmentID()
{
    return getNextSegmentIDRange(1);
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
        case static_cast<FormatAsInt>(GinIndexStore::Format::v2):
            return GinIndexStore::Format::v2;
        default:
            return GinIndexStore::Format::v0;
    }
}

void verifyFormatVersionIsSupported(GinIndexStore::Format version)
{
    if ((version < GinIndexStore::Format::v1) || (version > GinIndexStore::Format::v2))
        throw Exception(
            ErrorCodes::UNKNOWN_FORMAT_VERSION,
            "Unsupported text index version: supported versions {} and {}, but got {}",
            GinIndexStore::Format::v1,
            GinIndexStore::Format::v2,
            version);
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
        std::unique_ptr<DB::ReadBufferFromFileBase> istr = this->storage->readFile(segment_id_file_name, {}, std::nullopt, std::nullopt);

        uint8_t version = 0;
        readBinary(version, *istr);

        verifyFormatVersionIsSupported(getFormatVersion(version));

        readVarUInt(result, *istr);
    }

    cached_segment_num = result - 1;
    return cached_segment_num;
}

GinIndexStore::Format GinIndexStore::getVersion()
{
    String segment_id_file_name = getName() + GIN_SEGMENT_ID_FILE_TYPE;
    if (!storage->existsFile(segment_id_file_name))
        return GinIndexStore::Format::v0;

    std::unique_ptr<DB::ReadBufferFromFileBase> istr = this->storage->readFile(segment_id_file_name, {}, std::nullopt, std::nullopt);
    uint8_t version = 0;
    readBinary(version, *istr);
    return getFormatVersion(version);
}

bool GinIndexStore::needToWrite() const
{
    assert(max_digestion_size > 0);
    return current_size > max_digestion_size;
}

void GinIndexStore::finalize()
{
    if (!current_postings.empty())
    {
        writeSegment();
        writeSegmentId();
    }

    if (metadata_file_stream)
        metadata_file_stream->finalize();

    if (dict_file_stream)
        dict_file_stream->finalize();

    if (postings_file_stream)
        postings_file_stream->finalize();

    if (filter_file_stream)
        filter_file_stream->finalize();
}

void GinIndexStore::cancel() noexcept
{
    if (metadata_file_stream)
        metadata_file_stream->cancel();

    if (dict_file_stream)
        dict_file_stream->cancel();

    if (postings_file_stream)
        postings_file_stream->cancel();

    if (filter_file_stream)
        filter_file_stream->cancel();
}

void GinIndexStore::initSegmentId()
{
    String segment_id_file_name = getName() + GIN_SEGMENT_ID_FILE_TYPE;

    UInt32 segment_id;
    if (storage->existsFile(segment_id_file_name))
    {
        std::unique_ptr<DB::ReadBufferFromFileBase> istr = this->storage->readFile(segment_id_file_name, {}, std::nullopt, std::nullopt);

        uint8_t version = 0;
        readBinary(version, *istr);

        verifyFormatVersionIsSupported(getFormatVersion(version));

        readVarUInt(segment_id, *istr);
    }
    else
        segment_id = 1;

    next_available_segment_id = segment_id;
}

void GinIndexStore::initFileStreams()
{
    String metadata_file_name = getName() + GIN_SEGMENT_METADATA_FILE_TYPE;
    String dict_file_name = getName() + GIN_DICTIONARY_FILE_TYPE;
    String postings_file_name = getName() + GIN_POSTINGS_FILE_TYPE;
    String filter_file_name = getName() + GIN_FILTER_FILE_TYPE;

    metadata_file_stream = data_part_storage_builder->writeFile(metadata_file_name, 4096, WriteMode::Append, {});
    dict_file_stream = data_part_storage_builder->writeFile(dict_file_name, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append, {});
    postings_file_stream = data_part_storage_builder->writeFile(postings_file_name, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append, {});
    filter_file_stream = data_part_storage_builder->writeFile(filter_file_name, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append, {});
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

void GinIndexStore::writeSegment()
{
    if (metadata_file_stream == nullptr)
        initFileStreams();

    /// Write segment
    metadata_file_stream->write(reinterpret_cast<char *>(&current_segment), sizeof(GinIndexSegment));

    using TokenPostingsBuilderPair = std::pair<std::string_view, GinIndexPostingsBuilderPtr>;
    using TokenPostingsBuilderPairs = std::vector<TokenPostingsBuilderPair>;

    TokenPostingsBuilderPairs token_postings_list_pairs;
    token_postings_list_pairs.reserve(current_postings.size());

    GinSegmentDictionaryBloomFilter bloom_filter(0.001, GIN_INDEX_BLOOM_FILTER_DEFAULT_MAX_TOKEN_SIZE);
    for (const auto & [token, postings_list] : current_postings)
    {
        token_postings_list_pairs.push_back({token, postings_list});
        bloom_filter.add(token.data(), token.size());
    }

    /// Sort token-postings list pairs since all tokens have to be added in FST in sorted order
    std::sort(token_postings_list_pairs.begin(), token_postings_list_pairs.end(),
                    [](const TokenPostingsBuilderPair & x, const TokenPostingsBuilderPair & y)
                    {
                        return x.first < y.first;
                    });

    /// Write postings
    std::vector<UInt64> posting_list_byte_sizes(current_postings.size(), 0);

    for (size_t i = 0; const auto & [token, postings_list] : token_postings_list_pairs)
    {
        auto posting_list_byte_size = postings_list->serialize(*postings_file_stream);

        posting_list_byte_sizes[i] = posting_list_byte_size;
        i++;
        current_segment.postings_start_offset += posting_list_byte_size;
    }

    /// Write bloom filter
    current_segment.filter_start_offset += bloom_filter.serialize(*filter_file_stream);

    /// Write item dictionary
    std::vector<UInt8> buffer;
    WriteBufferFromVector<std::vector<UInt8>> write_buf(buffer);
    FST::FstBuilder fst_builder(write_buf);

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
        const auto & codec = GinIndexCompressionFactory::zstdCodec();
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

    current_size = 0;
    current_postings.clear();
    current_segment.segment_id = getNextSegmentID();

    metadata_file_stream->sync();
    dict_file_stream->sync();
    postings_file_stream->sync();
    filter_file_stream->sync();
}

GinIndexStoreDeserializer::GinIndexStoreDeserializer(const GinIndexStorePtr & store_)
    : store(store_)
{
    initFileStreams();
}

void GinIndexStoreDeserializer::initFileStreams()
{
    String metadata_file_name = store->getName() + GinIndexStore::GIN_SEGMENT_METADATA_FILE_TYPE;
    String dict_file_name = store->getName() + GinIndexStore::GIN_DICTIONARY_FILE_TYPE;
    String postings_file_name = store->getName() + GinIndexStore::GIN_POSTINGS_FILE_TYPE;
    String filter_file_name = store->getName() + GinIndexStore::GIN_FILTER_FILE_TYPE;

    metadata_file_stream = store->storage->readFile(metadata_file_name, {}, std::nullopt, std::nullopt);
    dict_file_stream = store->storage->readFile(dict_file_name, {}, std::nullopt, std::nullopt);
    postings_file_stream = store->storage->readFile(postings_file_name, {}, std::nullopt, std::nullopt);
    filter_file_stream = store->storage->readFile(filter_file_name, {}, std::nullopt, std::nullopt);
}
void GinIndexStoreDeserializer::readSegments()
{
    UInt32 num_segments = store->getNumOfSegments();
    if (num_segments == 0)
        return;

    assert(metadata_file_stream != nullptr);

    /// TODO(ahmadov): clean up versions and V1 handling
    if (store->getVersion() == GinIndexStore::Format::v1)
    {
        struct GinIndexSegmentV1
        {
            UInt32 segment_id;
            UInt32 next_row_id;
            UInt64 postings_start_offset;
            UInt64 dict_start_offset;
        };
        std::vector<GinIndexSegmentV1> segments(num_segments);
        metadata_file_stream->readStrict(reinterpret_cast<char *>(segments.data()), num_segments * sizeof(GinIndexSegmentV1));
        for (UInt32 i = 0; i < num_segments; ++i)
        {
            auto seg_dict = std::make_shared<GinSegmentDictionary>();
            seg_dict->postings_start_offset = segments[i].postings_start_offset;
            seg_dict->dict_start_offset = segments[i].dict_start_offset;
            store->segment_dictionaries[segments[i].segment_id] = seg_dict;
        }
    }
    else
    {
        std::vector<GinIndexSegment> segments(num_segments);
        metadata_file_stream->readStrict(reinterpret_cast<char *>(segments.data()), num_segments * sizeof(GinIndexSegment));
        for (UInt32 i = 0; i < num_segments; ++i)
        {
            auto seg_dict = std::make_shared<GinSegmentDictionary>();
            seg_dict->postings_start_offset = segments[i].postings_start_offset;
            seg_dict->dict_start_offset = segments[i].dict_start_offset;
            seg_dict->filter_start_offset = segments[i].filter_start_offset;
            store->segment_dictionaries[segments[i].segment_id] = seg_dict;
        }
    }
}

void GinIndexStoreDeserializer::readSegmentDictionaries()
{
    for (UInt32 seg_index = 0; seg_index < store->getNumOfSegments(); ++seg_index)
        readSegmentDictionary(seg_index);
}

void GinIndexStoreDeserializer::readSegmentDictionary(UInt32 segment_id)
{
    /// Check validity of segment_id
    auto it = store->segment_dictionaries.find(segment_id);
    if (it == store->segment_dictionaries.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid segment id {}", segment_id);

    const GinSegmentDictionaryPtr & seg_dict = it->second;
    switch (auto version = store->getVersion(); version)
    {
        case GinIndexStore::Format::v1:
            /// V1 does not support bloom filters, so read dictionaries directly
            readSegmentFST(seg_dict);
            break;
        case GinIndexStore::Format::v2: {
            /// V2 supports bloom filter, so we can delay reading a segment until it's needed.

            /// Set file pointer of filter file
            assert(filter_file_stream != nullptr);
            filter_file_stream->seek(it->second->filter_start_offset, SEEK_SET);
            seg_dict->bloom_filter = std::make_unique<GinSegmentDictionaryBloomFilter>();
            seg_dict->bloom_filter->deserialize(*filter_file_stream);
            break;
        }
        default:
            verifyFormatVersionIsSupported(version);
    }
}

void GinIndexStoreDeserializer::readSegmentFST(GinSegmentDictionaryPtr segment_dictionary)
{
    /// Set file pointer of dictionary file
    assert(dict_file_stream != nullptr);
    dict_file_stream->seek(segment_dictionary->dict_start_offset, SEEK_SET);

    segment_dictionary->fst = std::make_unique<FST::FiniteStateTransducer>();
    switch (auto version = store->getVersion(); version)
    {
        case GinIndexStore::Format::v1: {
            /// Read FST size
            size_t fst_size = 0;
            readVarUInt(fst_size, *dict_file_stream);

            /// Read FST blob
            segment_dictionary->fst->getData().clear();
            segment_dictionary->fst->getData().resize(fst_size);
            dict_file_stream->readStrict(reinterpret_cast<char *>(segment_dictionary->fst->getData().data()), fst_size);
            break;
        }
        case GinIndexStore::Format::v2: {
            /// Read FST size header
            UInt64 fst_size_header;
            readVarUInt(fst_size_header, *dict_file_stream);

            size_t uncompressed_fst_size = fst_size_header >> 1;
            segment_dictionary->fst->getData().clear();
            segment_dictionary->fst->getData().resize(uncompressed_fst_size);
            if (fst_size_header & 0x1) /// FST is compressed
            {
                /// Read compressed FST size
                size_t compressed_fst_size = 0;
                readVarUInt(compressed_fst_size, *dict_file_stream);
                /// Read compressed FST blob
                std::vector<char> buf(compressed_fst_size);
                dict_file_stream->readStrict(buf.data(), compressed_fst_size);
                const auto & codec = DB::GinIndexCompressionFactory::zstdCodec();
                codec->decompress(
                    buf.data(),
                    static_cast<UInt32>(compressed_fst_size),
                    reinterpret_cast<char *>(segment_dictionary->fst->getData().data()));
            }
            else
            {
                /// Read uncompressed FST blob
                dict_file_stream->readStrict(reinterpret_cast<char *>(segment_dictionary->fst->getData().data()), uncompressed_fst_size);
            }
            break;
        }
        default:
            verifyFormatVersionIsSupported(version);
    }
}

GinSegmentedPostingsListContainer GinIndexStoreDeserializer::readSegmentedPostingsLists(const String & term)
{
    assert(postings_file_stream != nullptr);

    GinSegmentedPostingsListContainer container;
    for (auto const & seg_dict : store->segment_dictionaries)
    {
        auto segment_id = seg_dict.first;

        if (seg_dict.second->bloom_filter && !seg_dict.second->bloom_filter->contains(term.data(), term.size()))
            continue;

        if (seg_dict.second->fst == nullptr)
            readSegmentFST(seg_dict.second);

        auto [offset, found] = seg_dict.second->fst->getOutput(term);
        if (!found)
            continue;

        // Set postings file pointer for reading postings list
        postings_file_stream->seek(seg_dict.second->postings_start_offset + offset, SEEK_SET);

        // Read posting list
        auto postings_list = GinIndexPostingsBuilder::deserialize(*postings_file_stream);
        container[segment_id] = postings_list;
    }
    return container;
}

GinPostingsCachePtr GinIndexStoreDeserializer::createPostingsCacheFromTerms(const std::vector<String> & terms)
{
    auto postings_cache = std::make_shared<GinPostingsCache>();
    for (const auto & term : terms)
    {
        // Make sure don't read for duplicated terms
        if (postings_cache->find(term) != postings_cache->end())
            continue;

        auto container = readSegmentedPostingsLists(term);
        (*postings_cache)[term] = container;
    }
    return postings_cache;
}

GinPostingsCachePtr PostingsCacheForStore::getPostings(const String & query_string) const
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
        deserializer.readSegmentDictionaries();

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
}

// NOLINTEND(clang-analyzer-optin.core.EnumCastOutOfRange)
