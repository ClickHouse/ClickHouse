// NOLINTBEGIN(clang-analyzer-optin.core.EnumCastOutOfRange)

#include <Storages/MergeTree/GinIndexStore.h>
#include <Columns/ColumnString.h>
#include <Common/FST.h>
#include <Compression/CompressionFactory.h>
#include <Compression/ICompressionCodec.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
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

GinIndexPostingsBuilder::GinIndexPostingsBuilder(UInt64 limit)
    : rowid_lst{}
    , size_limit(limit)
{}

bool GinIndexPostingsBuilder::contains(UInt32 row_id) const
{
    if (useRoaring())
        return rowid_bitmap.contains(row_id);

    const auto it = std::find(rowid_lst.begin(), rowid_lst.begin() + rowid_lst_length, row_id);
    return it != rowid_lst.begin() + rowid_lst_length;
}

void GinIndexPostingsBuilder::add(UInt32 row_id)
{
    if (containsAllRows())
        return;

    if (useRoaring())
    {
        if (rowid_bitmap.cardinality() == size_limit)
        {
            /// reset the postings list with MATCH ALWAYS;
            rowid_lst_length = 1; /// makes sure useRoaring() returns false;
            rowid_lst[0] = CONTAINS_ALL; /// set CONTAINS_ALL flag;
        }
        else
            rowid_bitmap.add(row_id);
    }
    else
    {
        assert(rowid_lst_length < MIN_SIZE_FOR_ROARING_ENCODING);
        rowid_lst[rowid_lst_length] = row_id;
        rowid_lst_length++;

        if (rowid_lst_length == MIN_SIZE_FOR_ROARING_ENCODING)
        {
            for (size_t i = 0; i < rowid_lst_length; i++)
                rowid_bitmap.add(rowid_lst[i]);

            rowid_lst_length = USES_BIT_MAP;
        }
    }
}

UInt64 GinIndexPostingsBuilder::serialize(WriteBuffer & buffer)
{
    UInt64 written_bytes = 0;
    buffer.write(rowid_lst_length);
    written_bytes += 1;

    if (useRoaring())
    {
        rowid_bitmap.runOptimize();
        auto size = rowid_bitmap.getSizeInBytes();
        auto buf = std::make_unique<char[]>(size);
        rowid_bitmap.write(buf.get());

        auto codec = CompressionCodecFactory::instance().get(GIN_COMPRESSION_CODEC, GIN_COMPRESSION_LEVEL);
        Memory<> memory;
        memory.resize(codec->getCompressedReserveSize(static_cast<UInt32>(size)));
        auto compressed_size = codec->compress(buf.get(), static_cast<UInt32>(size), memory.data());

        writeVarUInt(size, buffer);
        written_bytes += getLengthOfVarUInt(size);

        writeVarUInt(compressed_size, buffer);
        written_bytes += getLengthOfVarUInt(compressed_size);

        buffer.write(memory.data(), compressed_size);
        written_bytes += compressed_size;
    }
    else
    {
        for (size_t i = 0; i <  rowid_lst_length; ++i)
        {
            writeVarUInt(rowid_lst[i], buffer);
            written_bytes += getLengthOfVarUInt(rowid_lst[i]);
        }
    }

    return written_bytes;
}

GinIndexPostingsListPtr GinIndexPostingsBuilder::deserialize(ReadBuffer & buffer)
{
    UInt8 postings_list_size = 0;
    buffer.readStrict(reinterpret_cast<char &>(postings_list_size));

    if (postings_list_size == USES_BIT_MAP)
    {
        size_t size = 0;
        size_t compressed_size = 0;
        readVarUInt(size, buffer);
        readVarUInt(compressed_size, buffer);
        auto buf = std::make_unique<char[]>(compressed_size);
        buffer.readStrict(reinterpret_cast<char *>(buf.get()), compressed_size);

        Memory<> memory;
        memory.resize(size);
        auto codec = CompressionCodecFactory::instance().get(GIN_COMPRESSION_CODEC, GIN_COMPRESSION_LEVEL);
        codec->decompress(buf.get(), static_cast<UInt32>(compressed_size), memory.data());

        GinIndexPostingsListPtr postings_list = std::make_shared<GinIndexPostingsList>(GinIndexPostingsList::read(memory.data()));

        return postings_list;
    }

    assert(postings_list_size < MIN_SIZE_FOR_ROARING_ENCODING);
    GinIndexPostingsListPtr postings_list = std::make_shared<GinIndexPostingsList>();
    UInt32 row_ids[MIN_SIZE_FOR_ROARING_ENCODING];

    for (auto i = 0; i < postings_list_size; ++i)
        readVarUInt(row_ids[i], buffer);
    postings_list->addMany(postings_list_size, row_ids);
    return postings_list;
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

void GinIndexStore::verifyFormatVersionIsSupported(size_t version)
{
    if (version != static_cast<std::underlying_type_t<Format>>(CURRENT_GIN_FILE_FORMAT_VERSION))
        throw Exception(
            ErrorCodes::UNKNOWN_FORMAT_VERSION,
            "Unsupported text index version: expected version {}, but got {}",
            CURRENT_GIN_FILE_FORMAT_VERSION,
            version);
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

        verifyFormatVersionIsSupported(version);

        readVarUInt(result, *istr);
    }

    cached_segment_num = result - 1;
    return cached_segment_num;
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
}

void GinIndexStore::cancel() noexcept
{
    if (metadata_file_stream)
        metadata_file_stream->cancel();

    if (dict_file_stream)
        dict_file_stream->cancel();

    if (postings_file_stream)
        postings_file_stream->cancel();
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

        verifyFormatVersionIsSupported(version);

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

    metadata_file_stream = data_part_storage_builder->writeFile(metadata_file_name, 4096, WriteMode::Append, {});
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

void GinIndexStore::writeSegment()
{
    if (metadata_file_stream == nullptr)
        initFileStreams();

    using TokenPostingsBuilderPair = std::pair<std::string_view, GinIndexPostingsBuilderPtr>;
    using TokenPostingsBuilderPairs = std::vector<TokenPostingsBuilderPair>;

    /// Write segment
    metadata_file_stream->write(reinterpret_cast<char *>(&current_segment), sizeof(GinIndexSegment));
    TokenPostingsBuilderPairs token_postings_list_pairs;
    token_postings_list_pairs.reserve(current_postings.size());

    for (const auto & [token, postings_list] : current_postings)
        token_postings_list_pairs.push_back({token, postings_list});

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
    ///write item dictionary
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

    /// Write FST size
    writeVarUInt(buffer.size(), *dict_file_stream);
    current_segment.dict_start_offset += getLengthOfVarUInt(buffer.size());

    /// Write FST blob
    dict_file_stream->write(reinterpret_cast<char *>(buffer.data()), buffer.size());
    current_segment.dict_start_offset += buffer.size();

    current_size = 0;
    current_postings.clear();
    current_segment.segment_id = getNextSegmentID();

    metadata_file_stream->sync();
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
    String metadata_file_name = store->getName() + GinIndexStore::GIN_SEGMENT_METADATA_FILE_TYPE;
    String dict_file_name = store->getName() + GinIndexStore::GIN_DICTIONARY_FILE_TYPE;
    String postings_file_name = store->getName() + GinIndexStore::GIN_POSTINGS_FILE_TYPE;

    metadata_file_stream = store->storage->readFile(metadata_file_name, {}, std::nullopt, std::nullopt);
    dict_file_stream = store->storage->readFile(dict_file_name, {}, std::nullopt, std::nullopt);
    postings_file_stream = store->storage->readFile(postings_file_name, {}, std::nullopt, std::nullopt);
}
void GinIndexStoreDeserializer::readSegments()
{
    UInt32 num_segments = store->getNumOfSegments();
    if (num_segments == 0)
        return;

    using GinIndexSegments = std::vector<GinIndexSegment>;
    GinIndexSegments segments (num_segments);

    assert(metadata_file_stream != nullptr);

    metadata_file_stream->readStrict(reinterpret_cast<char *>(segments.data()), num_segments * sizeof(GinIndexSegment));
    for (UInt32 i = 0; i < num_segments; ++i)
    {
        auto seg_id = segments[i].segment_id;
        auto seg_dict = std::make_shared<GinSegmentDictionary>();
        seg_dict->postings_start_offset = segments[i].postings_start_offset;
        seg_dict->dict_start_offset = segments[i].dict_start_offset;
        store->segment_dictionaries[seg_id] = seg_dict;
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

    assert(dict_file_stream != nullptr);

    /// Set file pointer of dictionary file
    dict_file_stream->seek(it->second->dict_start_offset, SEEK_SET);

    it->second->offsets.getData().clear();
    /// Read FST size
    size_t fst_size = 0;
    readVarUInt(fst_size, *dict_file_stream);

    /// Read FST blob
    it->second->offsets.getData().resize(fst_size);
    dict_file_stream->readStrict(reinterpret_cast<char *>(it->second->offsets.getData().data()), fst_size);
}

GinSegmentedPostingsListContainer GinIndexStoreDeserializer::readSegmentedPostingsLists(const String & term)
{
    assert(postings_file_stream != nullptr);

    GinSegmentedPostingsListContainer container;
    for (auto const & seg_dict : store->segment_dictionaries)
    {
        auto segment_id = seg_dict.first;

        auto [offset, found] = seg_dict.second->offsets.getOutput(term);
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
