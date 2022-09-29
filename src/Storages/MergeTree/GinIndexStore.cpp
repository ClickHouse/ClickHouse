#include <vector>
#include <unordered_map>
#include <iostream>
#include <numeric>
#include <algorithm>
#include <Columns/ColumnString.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromVector.h>
#include <Storages/MergeTree/GinIndexStore.h>
#include <Common/FST.h>
#include <chrono>  // for high_resolution_clock

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
};

bool GinIndexPostingsBuilder::contains(UInt32 row_id) const
{
    if (useRoaring())
        return rowid_bitmap.contains(row_id);

    const auto *const it(std::find(lst.begin(), lst.begin()+lst_length, row_id));
    return it != lst.begin()+lst_length;
}

void GinIndexPostingsBuilder::add(UInt32 row_id)
{
    if (useRoaring())
    {
        rowid_bitmap.add(row_id);
        return;
    }
    assert(lst_length < MIN_SIZE_FOR_ROARING_ENCODING);
    lst[lst_length++] = row_id;

    if (lst_length == MIN_SIZE_FOR_ROARING_ENCODING)
    {
        for (size_t i = 0; i < lst_length; i++)
            rowid_bitmap.add(lst[i]);

        lst_length = UsesBitMap;
    }
}

bool GinIndexPostingsBuilder::useRoaring() const
{
    return lst_length == UsesBitMap;
}

UInt64 GinIndexPostingsBuilder::serialize(WriteBuffer &buffer) const
{
    UInt64 written_bytes = 0;
    buffer.write(lst_length);
    written_bytes += 1;

    if (!useRoaring())
    {
        for (size_t i = 0; i <  lst_length; ++i)
        {
            writeVarUInt(lst[i], buffer);
            written_bytes += getLengthOfVarUInt(lst[i]);
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

GinIndexPostingsListPtr GinIndexPostingsBuilder::deserialize(ReadBuffer &buffer)
{
    UInt8 postings_list_size{0};
    buffer.read(reinterpret_cast<char&>(postings_list_size));

    if (postings_list_size != UsesBitMap)
    {
        assert(postings_list_size < MIN_SIZE_FOR_ROARING_ENCODING);
        GinIndexPostingsListPtr postings_list = std::make_shared<GinIndexPostingsList>();
        UInt32 row_ids[MIN_SIZE_FOR_ROARING_ENCODING];

        for (auto i = 0; i < postings_list_size; ++i)
        {
            readVarUInt(row_ids[i], buffer);
        }
        postings_list->addMany(postings_list_size, row_ids);
        return postings_list;
    }
    else
    {
        size_t size{0};
        readVarUInt(size, buffer);
        auto buf = std::make_unique<char[]>(size);
        buffer.readStrict(reinterpret_cast<char*>(buf.get()), size);

        GinIndexPostingsListPtr postings_list = std::make_shared<GinIndexPostingsList>(GinIndexPostingsList::read(buf.get()));

        return postings_list;
    }
}

bool GinIndexStore::exists() const
{
    String id_file_name = getName() + GIN_SEGMENT_ID_FILE_TYPE;
    return storage->exists(id_file_name);
}

UInt32 GinIndexStore::getNextIDRange(const String& file_name, size_t n)
{
    std::lock_guard<std::mutex> guard{gin_index_store_mutex};

    if (!storage->exists(file_name))
    {
        std::unique_ptr<DB::WriteBufferFromFileBase> ostr = this->data_part_storage_builder->writeFile(file_name, DBMS_DEFAULT_BUFFER_SIZE, {});

        const auto& int_type = DB::DataTypePtr(std::make_shared<DB::DataTypeUInt32>());
        auto size_serialization = int_type->getDefaultSerialization();
        size_serialization->serializeBinary(1, *ostr);
        ostr->sync();
    }

    /// read id in file
    UInt32 result = 0;
    {
        std::unique_ptr<DB::ReadBufferFromFileBase> istr = this->storage->readFile(file_name, {}, std::nullopt, std::nullopt);

        Field field_rows;
        const auto& size_type = DB::DataTypePtr(std::make_shared<DB::DataTypeUInt32>());
        auto size_serialization = size_type->getDefaultSerialization();

        size_type->getDefaultSerialization()->deserializeBinary(field_rows, *istr);
        result = field_rows.get<UInt32>();
    }
    //save result+n
    {
        std::unique_ptr<DB::WriteBufferFromFileBase> ostr = this->data_part_storage_builder->writeFile(file_name, DBMS_DEFAULT_BUFFER_SIZE, {});

        const auto& int_type = DB::DataTypePtr(std::make_shared<DB::DataTypeUInt32>());
        auto size_serialization = int_type->getDefaultSerialization();
        size_serialization->serializeBinary(result + n, *ostr);
        ostr->sync();
    }
    return result;
}

UInt32 GinIndexStore::getNextRowIDRange(size_t n)
{
    UInt32 result =current_segment.next_row_id;
    current_segment.next_row_id += n;
    return result;
}

UInt32 GinIndexStore::getNextSegmentID()
{
    String sid_file_name = getName() + GIN_SEGMENT_ID_FILE_TYPE;
    return getNextIDRange(sid_file_name, 1);
}

UInt32 GinIndexStore::getSegmentNum()
{
    if (cached_segment_num)
        return cached_segment_num;

    String sid_file_name = getName() + GIN_SEGMENT_ID_FILE_TYPE;
    if (!storage->exists(sid_file_name))
        return 0;
    Int32 result = 0;
    {
        std::unique_ptr<DB::ReadBufferFromFileBase> istr = this->storage->readFile(sid_file_name, {}, std::nullopt, std::nullopt);

        Field field_rows;
        const auto& size_type = DB::DataTypePtr(std::make_shared<DB::DataTypeUInt32>());
        auto size_serialization = size_type->getDefaultSerialization();

        size_type->getDefaultSerialization()->deserializeBinary(field_rows, *istr);
        result = field_rows.get<UInt32>();
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
    }
}

void GinIndexStore::initFileStreams()
{
    String segment_file_name = getName() + GIN_SEGMENT_FILE_TYPE;
    String item_dict_file_name = getName() + GIN_DICTIONARY_FILE_TYPE;
    String postings_file_name = getName() + GIN_POSTINGS_FILE_TYPE;

    segment_file_stream = data_part_storage_builder->writeFile(segment_file_name, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append, {});
    term_dict_file_stream = data_part_storage_builder->writeFile(item_dict_file_name, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append, {});
    postings_file_stream = data_part_storage_builder->writeFile(postings_file_name, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append, {});
}

void GinIndexStore::writeSegment()
{
    if (segment_file_stream == nullptr)
    {
        initFileStreams();
    }

    ///write segment
    segment_file_stream->write(reinterpret_cast<char*>(&current_segment), sizeof(GinIndexSegment));
    std::vector<std::pair<std::string_view, GinIndexPostingsBuilderPtr>> token_postings_list_pairs;
    token_postings_list_pairs.reserve(current_postings.size());

    for (const auto& [token, postings_list] : current_postings)
    {
        token_postings_list_pairs.push_back({std::string_view(token), postings_list});
    }
    std::sort(token_postings_list_pairs.begin(), token_postings_list_pairs.end(),
                    [](const std::pair<std::string_view, GinIndexPostingsBuilderPtr>& a, const std::pair<std::string_view, GinIndexPostingsBuilderPtr>& b)
                    {
                        return a.first < b.first;
                    });

    ///write postings
    std::vector<UInt64> encoding_lengths(current_postings.size(), 0);
    size_t current_index = 0;

    for (const auto& [token, postings_list] : token_postings_list_pairs)
    {
        auto encoding_length = postings_list->serialize(*postings_file_stream);

        encoding_lengths[current_index++] = encoding_length;
        current_segment.postings_start_offset += encoding_length;
    }
    ///write item dictionary
    std::vector<UInt8> buffer;
    WriteBufferFromVector<std::vector<UInt8>> write_buf(buffer);
    FST::FSTBuilder builder(write_buf);

    UInt64 offset{0};
    current_index = 0;
    for (const auto& [token, postings_list] : token_postings_list_pairs)
    {
        String str_token{token};
        builder.add(str_token, offset);
        offset += encoding_lengths[current_index++];
    }

    builder.build();
    write_buf.finalize();

    /// Write FST size
    writeVarUInt(buffer.size(), *term_dict_file_stream);
    current_segment.item_dict_start_offset += getLengthOfVarUInt(buffer.size());

    /// Write FST content
    term_dict_file_stream->write(reinterpret_cast<char*>(buffer.data()), buffer.size());
    current_segment.item_dict_start_offset += buffer.size();

    current_size = 0;
    current_postings.clear();
    current_segment.segment_id = getNextSegmentID();

    segment_file_stream->sync();
    term_dict_file_stream->sync();
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
    String item_dict_file_name = store->getName() + GinIndexStore::GIN_DICTIONARY_FILE_TYPE;
    String postings_file_name = store->getName() + GinIndexStore::GIN_POSTINGS_FILE_TYPE;

    segment_file_stream = store->storage->readFile(segment_file_name, {}, std::nullopt, std::nullopt);
    term_dict_file_stream = store->storage->readFile(item_dict_file_name, {}, std::nullopt, std::nullopt);
    postings_file_stream = store->storage->readFile(postings_file_name, {}, std::nullopt, std::nullopt);
}
void GinIndexStoreDeserializer::readSegments()
{
    auto segment_num = store->getSegmentNum();
    if (segment_num == 0)
        return;

    GinIndexSegments segments (segment_num);

    assert(segment_file_stream != nullptr);

    segment_file_stream->read(reinterpret_cast<char*>(segments.data()), segment_num * sizeof(GinIndexSegment));
    for (size_t i = 0; i < segment_num; ++i)
    {
        auto seg_id = segments[i].segment_id;
        auto term_dict = std::make_shared<TermDictionary>();
        term_dict->postings_start_offset = segments[i].postings_start_offset;
        term_dict->item_dict_start_offset = segments[i].item_dict_start_offset;
        store->term_dicts[seg_id] = term_dict;
    }
}

void GinIndexStoreDeserializer::readTermDictionary(UInt32 segment_id)
{
    /// Check validity of segment_id
    auto it{ store->term_dicts.find(segment_id) };
    if (it == store->term_dicts.cend())
    {
        throw Exception("Invalid segment id " + std::to_string(segment_id), ErrorCodes::LOGICAL_ERROR);
    }

    it->second->offsets.getData().clear();

    /// Set file pointer of term dictionary file
    term_dict_file_stream->seek(it->second->item_dict_start_offset, SEEK_SET);

    /// Read FST size
    size_t fst_size{0};
    readVarUInt(fst_size, *term_dict_file_stream);

    /// Read FST content
    it->second->offsets.getData().resize(fst_size);
    term_dict_file_stream->readStrict(reinterpret_cast<char*>(it->second->offsets.getData().data()), fst_size);
}

SegmentedPostingsListContainer GinIndexStoreDeserializer::readSegmentedPostingsLists(const String& token)
{
    SegmentedPostingsListContainer container;
    for (auto const& seg_term_dict : store->term_dicts)
    {
        auto segment_id = seg_term_dict.first;

        auto [offset, found] = seg_term_dict.second->offsets.getOutput(token);
        if (!found)
            continue;

        // Set postings file pointer for reading postings list
        postings_file_stream->seek(seg_term_dict.second->postings_start_offset + offset, SEEK_SET);

        // Read posting list
        auto postings_list = GinIndexPostingsBuilder::deserialize(*postings_file_stream);
        container[segment_id] = postings_list;
    }
    return container;
}

PostingsCachePtr GinIndexStoreDeserializer::loadPostingsIntoCache(const std::vector<String>& terms)
{
    auto postings_cache = std::make_shared<PostingsCache>();
    for (const auto& term : terms)
    {
        // Make sure don't read for duplicated terms
        if (postings_cache->find(term) != postings_cache->cend())
            continue;

        auto container = readSegmentedPostingsLists(term);
        (*postings_cache)[term] = container;
    }
    return postings_cache;
}

GinIndexStoreFactory& GinIndexStoreFactory::instance()
{
    static GinIndexStoreFactory instance;
    return instance;
}

GinIndexStorePtr GinIndexStoreFactory::get(const String& name, DataPartStoragePtr storage_)
{
    const String& part_path = storage_->getRelativePath();
    String key = name + String(":")+part_path;

    std::lock_guard lock(stores_mutex);
    GinIndexStores::const_iterator it = stores.find(key);

    if (it == stores.cend())
    {
        GinIndexStorePtr store = std::make_shared<GinIndexStore>(name, storage_);
        if (!store->exists())
            return nullptr;

        GinIndexStoreDeserializer reader(store);
        reader.readSegments();

        for (size_t seg_index = 0; seg_index < store->getSegmentNum(); ++seg_index)
        {
            reader.readTermDictionary(seg_index);
        }

        stores[key] = store;

        return store;
    }
    return it->second;
}

void GinIndexStoreFactory::remove(const String& part_path)
{
    std::lock_guard lock(stores_mutex);
    for (auto it = stores.begin(); it != stores.end();)
    {
        if (it->first.find(part_path) != String::npos)
            it = stores.erase(it);
        else
            ++it;
    }
}
}
