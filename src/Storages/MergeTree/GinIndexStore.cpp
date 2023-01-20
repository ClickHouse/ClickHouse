#include <Storages/MergeTree/GinIndexStore.h>
#include <Columns/ColumnString.h>
#include <Common/FST.h>
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
#include <iostream>
#include <numeric>
#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_FORMAT_VERSION;
};

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

GinIndexPostingsBuilder::GinIndexPostingsBuilder(UInt64 limit)
    : rowid_lst{}
    , size_limit(limit)
{}

bool GinIndexPostingsBuilder::contains(UInt32 row_id) const
{
    if (useRoaring())
        return rowid_bitmap.contains(row_id);

    const auto * const it = std::find(rowid_lst.begin(), rowid_lst.begin()+rowid_lst_length, row_id);
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
            //reset the postings list with MATCH ALWAYS;
            rowid_lst_length = 1; //makes sure useRoaring() returns false;
            rowid_lst[0] = CONTAINS_ALL; //set CONTAINS ALL flag;
        }
        else
            rowid_bitmap.add(row_id);
        return;
    }

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

UInt64 GinIndexPostingsBuilder::serialize(WriteBuffer & buffer) const
{
    UInt64 written_bytes = 0;
    buffer.write(rowid_lst_length);
    written_bytes += 1;

    if (!useRoaring())
    {
        for (size_t i = 0; i <  rowid_lst_length; ++i)
        {
            writeVarUInt(rowid_lst[i], buffer);
            written_bytes += getLengthOfVarUInt(rowid_lst[i]);
        }
    }
    else
    {
        auto size = rowid_bitmap.getSizeInBytes();

        writeVarUInt(size, buffer);
        written_bytes += getLengthOfVarUInt(size);

        auto buf = std::make_unique<char[]>(size);
        rowid_bitmap.write(buf.get());
        buffer.write(buf.get(), size);
        written_bytes += size;
    }
    return written_bytes;
}

GinIndexPostingsListPtr GinIndexPostingsBuilder::deserialize(ReadBuffer & buffer)
{
    UInt8 postings_list_size = 0;
    buffer.readStrict(reinterpret_cast<char &>(postings_list_size));

    if (postings_list_size != USES_BIT_MAP)
    {
        assert(postings_list_size < MIN_SIZE_FOR_ROARING_ENCODING);
        GinIndexPostingsListPtr postings_list = std::make_shared<GinIndexPostingsList>();
        UInt32 row_ids[MIN_SIZE_FOR_ROARING_ENCODING];

        for (auto i = 0; i < postings_list_size; ++i)
            readVarUInt(row_ids[i], buffer);
        postings_list->addMany(postings_list_size, row_ids);
        return postings_list;
    }
    else
    {
        size_t size = 0;
        readVarUInt(size, buffer);
        auto buf = std::make_unique<char[]>(size);
        buffer.readStrict(reinterpret_cast<char *>(buf.get()), size);

        GinIndexPostingsListPtr postings_list = std::make_shared<GinIndexPostingsList>(GinIndexPostingsList::read(buf.get()));

        return postings_list;
    }
}

bool GinIndexStore::exists() const
{
    String id_file_name = getName() + GIN_SEGMENT_ID_FILE_TYPE;
    return storage->exists(id_file_name);
}

UInt32 GinIndexStore::getNextSegmentIDRange(const String & file_name, size_t n)
{
    std::lock_guard guard(mutex);

    /// When the method is called for the first time, the file doesn't exist yet, need to create it
    /// and write segment ID 1.
    if (!storage->exists(file_name))
    {
        /// Create file and write initial segment id = 1
        std::unique_ptr<DB::WriteBufferFromFileBase> ostr = this->data_part_storage_builder->writeFile(file_name, DBMS_DEFAULT_BUFFER_SIZE, {});

        /// Write version
        writeChar(static_cast<char>(CURRENT_GIN_FILE_FORMAT_VERSION), *ostr);

        writeVarUInt(1, *ostr);
        ostr->sync();
    }

    /// read id in file
    UInt32 result = 0;
    {
        std::unique_ptr<DB::ReadBufferFromFileBase> istr = this->storage->readFile(file_name, {}, std::nullopt, std::nullopt);

        /// Skip version
        istr->seek(1, SEEK_SET);

        readVarUInt(result, *istr);
    }
    //save result+n
    {
        std::unique_ptr<DB::WriteBufferFromFileBase> ostr = this->data_part_storage_builder->writeFile(file_name, DBMS_DEFAULT_BUFFER_SIZE, {});

        /// Write version
        writeChar(static_cast<char>(CURRENT_GIN_FILE_FORMAT_VERSION), *ostr);

        writeVarUInt(result + n, *ostr);
        ostr->sync();
    }
    return result;
}

UInt32 GinIndexStore::getNextRowIDRange(size_t numIDs)
{
    UInt32 result =current_segment.next_row_id;
    current_segment.next_row_id += numIDs;
    return result;
}

UInt32 GinIndexStore::getNextSegmentID()
{
    String sid_file_name = getName() + GIN_SEGMENT_ID_FILE_TYPE;
    return getNextSegmentIDRange(sid_file_name, 1);
}

UInt32 GinIndexStore::getNumOfSegments()
{
    if (cached_segment_num)
        return cached_segment_num;

    String sid_file_name = getName() + GIN_SEGMENT_ID_FILE_TYPE;
    if (!storage->exists(sid_file_name))
        return 0;

    UInt32 result = 0;
    {
        std::unique_ptr<DB::ReadBufferFromFileBase> istr = this->storage->readFile(sid_file_name, {}, std::nullopt, std::nullopt);

        uint8_t version = 0;
        readBinary(version, *istr);

        if (version > static_cast<std::underlying_type_t<Format>>(CURRENT_GIN_FILE_FORMAT_VERSION))
            throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unsupported inverted index version {}", version);

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
        writeSegment();
}

void GinIndexStore::initFileStreams()
{
    String segment_file_name = getName() + GIN_SEGMENT_FILE_TYPE;
    String dict_file_name = getName() + GIN_DICTIONARY_FILE_TYPE;
    String postings_file_name = getName() + GIN_POSTINGS_FILE_TYPE;

    segment_file_stream = data_part_storage_builder->writeFile(segment_file_name, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append, {});
    dict_file_stream = data_part_storage_builder->writeFile(dict_file_name, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append, {});
    postings_file_stream = data_part_storage_builder->writeFile(postings_file_name, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append, {});
}

void GinIndexStore::writeSegment()
{
    if (segment_file_stream == nullptr)
        initFileStreams();

    using TokenPostingsBuilderPair = std::pair<std::string_view, GinIndexPostingsBuilderPtr>;
    using TokenPostingsBuilderPairs = std::vector<TokenPostingsBuilderPair>;

    /// Write segment
    segment_file_stream->write(reinterpret_cast<char *>(&current_segment), sizeof(GinIndexSegment));
    TokenPostingsBuilderPairs token_postings_list_pairs;
    token_postings_list_pairs.reserve(current_postings.size());

    for (const auto & [token, postings_list] : current_postings)
        token_postings_list_pairs.push_back({token, postings_list});

    /// Sort token-postings list pairs since all tokens have to be added in FST in sorted order
    std::sort(token_postings_list_pairs.begin(), token_postings_list_pairs.end(),
                    [](const TokenPostingsBuilderPair & a, const TokenPostingsBuilderPair & b)
                    {
                        return a.first < b.first;
                    });

    ///write postings
    std::vector<UInt64> posting_list_byte_sizes(current_postings.size(), 0);

    for (size_t current_index = 0; const auto & [token, postings_list] : token_postings_list_pairs)
    {
        auto posting_list_byte_size = postings_list->serialize(*postings_file_stream);

        posting_list_byte_sizes[current_index] = posting_list_byte_size;
        current_index++;
        current_segment.postings_start_offset += posting_list_byte_size;
    }
    ///write item dictionary
    std::vector<UInt8> buffer;
    WriteBufferFromVector<std::vector<UInt8>> write_buf(buffer);
    FST::FstBuilder builder(write_buf);

    UInt64 offset = 0;
    for (size_t current_index = 0; const auto & [token, postings_list] : token_postings_list_pairs)
    {
        builder.add(token, offset);
        offset += posting_list_byte_sizes[current_index];
        current_index++;
    }

    builder.build();
    write_buf.finalize();

    /// Write FST size
    writeVarUInt(buffer.size(), *dict_file_stream);
    current_segment.dict_start_offset += getLengthOfVarUInt(buffer.size());

    /// Write FST content
    dict_file_stream->write(reinterpret_cast<char *>(buffer.data()), buffer.size());
    current_segment.dict_start_offset += buffer.size();

    current_size = 0;
    current_postings.clear();
    current_segment.segment_id = getNextSegmentID();

    segment_file_stream->sync();
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
    String segment_file_name = store->getName() + GinIndexStore::GIN_SEGMENT_FILE_TYPE;
    String dict_file_name = store->getName() + GinIndexStore::GIN_DICTIONARY_FILE_TYPE;
    String postings_file_name = store->getName() + GinIndexStore::GIN_POSTINGS_FILE_TYPE;

    segment_file_stream = store->storage->readFile(segment_file_name, {}, std::nullopt, std::nullopt);
    dict_file_stream = store->storage->readFile(dict_file_name, {}, std::nullopt, std::nullopt);
    postings_file_stream = store->storage->readFile(postings_file_name, {}, std::nullopt, std::nullopt);
}
void GinIndexStoreDeserializer::readSegments()
{
    auto num_segments = store->getNumOfSegments();
    if (num_segments == 0)
        return;

    using GinIndexSegments = std::vector<GinIndexSegment>;
    GinIndexSegments segments (num_segments);

    assert(segment_file_stream != nullptr);

    segment_file_stream->readStrict(reinterpret_cast<char *>(segments.data()), num_segments * sizeof(GinIndexSegment));
    for (size_t i = 0; i < num_segments; ++i)
    {
        auto seg_id = segments[i].segment_id;
        auto dict = std::make_shared<GinSegmentDictionary>();
        dict->postings_start_offset = segments[i].postings_start_offset;
        dict->dict_start_offset = segments[i].dict_start_offset;
        store->dicts[seg_id] = dict;
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
    auto it = store->dicts.find(segment_id);
    if (it == store->dicts.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid segment id {}", segment_id);

    assert(dict_file_stream != nullptr);

    /// Set file pointer of dictionary file
    dict_file_stream->seek(it->second->dict_start_offset, SEEK_SET);

    it->second->offsets.getData().clear();
    /// Read FST size
    size_t fst_size = 0;
    readVarUInt(fst_size, *dict_file_stream);

    /// Read FST content
    it->second->offsets.getData().resize(fst_size);
    dict_file_stream->readStrict(reinterpret_cast<char *>(it->second->offsets.getData().data()), fst_size);
}

SegmentedPostingsListContainer GinIndexStoreDeserializer::readSegmentedPostingsLists(const String & term)
{
    assert(postings_file_stream != nullptr);

    SegmentedPostingsListContainer container;
    for (auto const & seg_dict : store->dicts)
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

PostingsCachePtr GinIndexStoreDeserializer::createPostingsCacheFromTerms(const std::vector<String> & terms)
{
    auto postings_cache = std::make_shared<PostingsCache>();
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
