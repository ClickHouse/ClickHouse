#pragma once

#include <array>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <Core/Block.h>
#include <Disks/IDisk.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>
#include <roaring.hh>
#include <Common/FST.h>
#include <Storages/MergeTree/IDataPartStorage.h>
namespace DB
{

constexpr int MIN_SIZE_FOR_ROARING_ENCODING = 16;

/// Gin Index Segment information, which contains:
struct GinIndexSegment
{
    /// .gin_post file offset of this segment's postings lists
    UInt64 postings_start_offset{0};

    /// .gin_dict file offset of this segment's term dictionaries
    UInt64 item_dict_start_offset{0};

    /// Next row ID for this segment
    UInt32 next_row_id{1};

    ///  Segment ID retrieved from next available ID from file .gin_sid
    UInt32 segment_id {0};
};

using GinIndexSegments = std::vector<GinIndexSegment>;

/// GinIndexPostingsList which uses 32-bit Roaring
using GinIndexPostingsList = roaring::Roaring;

using GinIndexPostingsListPtr = std::shared_ptr<roaring::Roaring>;

/// Gin Index Postings List Builder.
class GinIndexPostingsBuilder
{
public:
    /// When the list length is no greater than MIN_SIZE_FOR_ROARING_ENCODING, array 'lst' is used
    std::array<UInt32, MIN_SIZE_FOR_ROARING_ENCODING> lst;

    /// When the list length is greater than MIN_SIZE_FOR_ROARING_ENCODING, Roaring bitmap 'rowid_bitmap' is used
    roaring::Roaring rowid_bitmap;

    /// lst_length stores the number of row IDs in 'lst' array, can also be a flag(0xFF) indicating that roaring bitmap is used
    UInt8 lst_length{0};

    /// Check whether a row_id is already added
    bool contains(UInt32 row_id) const;

    /// Add a row_id into the builder
    void add(UInt32 row_id);

    /// Check whether the builder is using roaring bitmap
    bool useRoaring() const;

    /// Serialize the content of builder to given WriteBuffer, returns the bytes of serialized data
    UInt64 serialize(WriteBuffer &buffer) const;

    /// Deserialize the postings list data from given ReadBuffer, return a pointer to the GinIndexPostingsList created by deserialization
    static GinIndexPostingsListPtr deserialize(ReadBuffer &buffer);
private:
    static constexpr UInt8 UsesBitMap = 0xFF;
};

using GinIndexPostingsBuilderPtr = std::shared_ptr<GinIndexPostingsBuilder>;

/// Container for all term's Gin Index Postings List Builder
using GinIndexPostingsBuilderContainer = std::unordered_map<std::string, GinIndexPostingsBuilderPtr>;

/// Container for postings lists for each segment
using SegmentedPostingsListContainer = std::unordered_map<UInt32, GinIndexPostingsListPtr>;

/// Postings lists and terms built from query string
using PostingsCache = std::unordered_map<std::string, SegmentedPostingsListContainer>;
using PostingsCachePtr = std::shared_ptr<PostingsCache>;

/// Term dictionary information, which contains:
struct TermDictionary
{
    /// .gin_post file offset of this segment's postings lists
    UInt64 postings_start_offset;

    /// .gin_dict file offset of this segment's term dictionaries
    UInt64 item_dict_start_offset;

    /// Finite State Transducer, which can be viewed as a map of <term, offset>
    FST::FiniteStateTransducer offsets;
};

using TermDictionaryPtr = std::shared_ptr<TermDictionary>;
using TermDictionaries = std::unordered_map<UInt32, TermDictionaryPtr>;

/// Gin Index Store which has Gin Index meta data for the corresponding Data Part
class GinIndexStore
{
public:
    explicit GinIndexStore(const String& name_, DataPartStoragePtr storage_)
        : name(name_),
        storage(storage_)
    {
    }
    GinIndexStore(const String& name_, DataPartStoragePtr storage_, DataPartStorageBuilderPtr data_part_storage_builder_, UInt64 max_digestion_size_)
        : name(name_),
        storage(storage_),
        data_part_storage_builder(data_part_storage_builder_),
        max_digestion_size(max_digestion_size_)
    {
    }

    bool load();

    /// Check existence by checking the existence of file .gin_seg
    bool exists() const;

    UInt32 getNextIDRange(const String &file_name, size_t n);

    UInt32 getNextRowIDRange(size_t n);

    UInt32 getNextSegmentID();

    UInt32 getSegmentNum();

    using GinIndexStorePtr = std::shared_ptr<GinIndexStore>;

    GinIndexPostingsBuilderContainer& getPostings() { return current_postings; }

    /// Check if we need to write segment to Gin index files
    bool needToWrite() const;

    /// Accumulate the size of text data which has been digested
    void addSize(UInt64 sz) { current_size += sz; }

    UInt64 getCurrentSegmentID() { return current_segment.segment_id;}

    /// Do last segment writing
    void finalize();

    /// method for writing segment data to Gin index files
    void writeSegment();

    const String & getName() const {return name;}

private:
    friend class GinIndexStoreDeserializer;

    void initFileStreams();

    String name;
    DataPartStoragePtr storage;
    DataPartStorageBuilderPtr data_part_storage_builder;

    UInt32 cached_segment_num = 0;

    std::mutex gin_index_store_mutex;

    /// Terms dictionaries which are loaded from .gin_dict files
    TermDictionaries term_dicts;

    /// container for building postings lists during index construction
    GinIndexPostingsBuilderContainer current_postings;

    /// The following is for segmentation of Gin index
    GinIndexSegment current_segment{};
    UInt64 current_size{0};
    UInt64 max_digestion_size{0};

    /// File streams for segment, term dictionaries and postings lists
    std::unique_ptr<WriteBufferFromFileBase> segment_file_stream;
    std::unique_ptr<WriteBufferFromFileBase> term_dict_file_stream;
    std::unique_ptr<WriteBufferFromFileBase> postings_file_stream;

    static constexpr auto GIN_SEGMENT_ID_FILE_TYPE = ".gin_sid";
    static constexpr auto GIN_SEGMENT_FILE_TYPE = ".gin_seg";
    static constexpr auto GIN_DICTIONARY_FILE_TYPE = ".gin_dict";
    static constexpr auto GIN_POSTINGS_FILE_TYPE = ".gin_post";
};

using GinIndexStorePtr = std::shared_ptr<GinIndexStore>;
using GinIndexStores = std::unordered_map<std::string, GinIndexStorePtr>;

/// Postings lists from 'store' which are retrieved from Gin index files for the terms in query strings
struct PostingsCacheForStore
{
    /// Which store to retrieve postings lists
    GinIndexStorePtr store;

    /// map of <query, postings lists>
    std::unordered_map<String, PostingsCachePtr> cache;

    /// Get postings lists for query string, return nullptr if not found
    PostingsCachePtr getPostings(const String &query_string) const
    {
        auto it {cache.find(query_string)};

        if (it == cache.cend())
        {
            return nullptr;
        }
        return it->second;
    }
};

/// GinIndexStore Factory, which is a singleton for storing GinIndexStores
class GinIndexStoreFactory : private boost::noncopyable
{
public:
    /// Get singleton of GinIndexStoreFactory
    static GinIndexStoreFactory& instance();

    /// Get GinIndexStore by using index name, disk and part_path (which are combined to create key in stores)
    GinIndexStorePtr get(const String& name, DataPartStoragePtr storage_);

    /// Remove all GinIndexStores which are under the same part_path
    void remove(const String& part_path);

private:
    GinIndexStores stores;
    std::mutex stores_mutex;
};

/// Gin Index Store Reader which helps to read segments, term dictionaries and postings list
class GinIndexStoreDeserializer : private boost::noncopyable
{
public:
    GinIndexStoreDeserializer(const GinIndexStorePtr & store_);

    /// Read all segment information from .gin_seg files
    void readSegments();

    /// Read term dictionary for given segment id
    void readTermDictionary(UInt32 segment_id);

    /// Read postings lists for the term
    SegmentedPostingsListContainer readSegmentedPostingsLists(const String& token);

    /// Read postings lists for terms(which are created by tokenzing query string)
    PostingsCachePtr loadPostingsIntoCache(const std::vector<String>& terms);

private:
    /// Initialize Gin index files
    void initFileStreams();

private:

    /// The store for the reader
    GinIndexStorePtr store;

    /// File streams for reading Gin Index
    std::unique_ptr<ReadBufferFromFileBase> segment_file_stream;
    std::unique_ptr<ReadBufferFromFileBase> term_dict_file_stream;
    std::unique_ptr<ReadBufferFromFileBase> postings_file_stream;

    /// Current segment, used in building index
    GinIndexSegment current_segment;
};
using GinIndexStoreReaderPtr = std::unique_ptr<GinIndexStoreDeserializer>;

}
