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

/// GinIndexStore manages the inverted index for a data part, and it is made up of one or more immutable
/// index segments.
///
/// There are 4 types of index files in a store:
///  1. Segment ID file(.gin_sid): it contains one byte for version followed by the next available segment ID.
///  2. Segment Metadata file(.gin_seg): it contains index segment metadata.
///     - Its file format is an array of GinIndexSegment as defined in this file.
///     - postings_start_offset points to the file(.gin_post) starting position for the segment's postings list.
///     - term_dict_start_offset points to the file(.gin_dict) starting position for the segment's term dictionaries.
///  3. Term Dictionary file(.gin_dict): it contains term dictionaries.
///     - It contains an array of (FST_size, FST_blob) which has size and actual data of FST.
///  4. Postings Lists(.gin_post): it contains postings lists data.
///     - It contains an array of serialized postings lists.
///
/// During the searching in the segment, the segment's meta data can be found in .gin_seg file. From the meta data,
/// the starting position of its term dictionary is used to locate its FST. Then FST is read into memory.
/// By using the term and FST, the offset("output" in FST) of the postings list for the term
/// in FST is found. The offset plus the postings_start_offset is the file location in .gin_post file
/// for its postings list.

namespace DB
{
enum : uint8_t
{
    GIN_VERSION_0 = 0,
    GIN_VERSION_1 = 1, /// Initial version
};

static constexpr auto CURRENT_GIN_FILE_FORMAT_VERSION = GIN_VERSION_1;

/// GinIndexPostingsList which uses 32-bit Roaring
using GinIndexPostingsList = roaring::Roaring;

using GinIndexPostingsListPtr = std::shared_ptr<GinIndexPostingsList>;

/// Gin Index Postings List Builder.
class GinIndexPostingsBuilder
{
public:
    constexpr static int MIN_SIZE_FOR_ROARING_ENCODING = 16;

    GinIndexPostingsBuilder(UInt64 limit);

    /// Check whether a row_id is already added
    bool contains(UInt32 row_id) const;

    /// Add a row_id into the builder
    void add(UInt32 row_id);

    /// Check whether the builder is using roaring bitmap
    bool useRoaring() const;

    /// Check whether the postings list has been flagged to contain all row ids
    bool containsAllRows() const;

    /// Serialize the content of builder to given WriteBuffer, returns the bytes of serialized data
    UInt64 serialize(WriteBuffer &buffer) const;

    /// Deserialize the postings list data from given ReadBuffer, return a pointer to the GinIndexPostingsList created by deserialization
    static GinIndexPostingsListPtr deserialize(ReadBuffer &buffer);
private:
    /// When the list length is no greater than MIN_SIZE_FOR_ROARING_ENCODING, array 'rowid_lst' is used
    std::array<UInt32, MIN_SIZE_FOR_ROARING_ENCODING> rowid_lst;

    /// When the list length is greater than MIN_SIZE_FOR_ROARING_ENCODING, Roaring bitmap 'rowid_bitmap' is used
    roaring::Roaring rowid_bitmap;

    /// rowid_lst_length stores the number of row IDs in 'rowid_lst' array, can also be a flag(0xFF) indicating that roaring bitmap is used
    UInt8 rowid_lst_length{0};

    static constexpr UInt8 UsesBitMap = 0xFF;
    /// Clear the postings list and reset it with MATCHALL flags when the size of the postings list is beyond the limit
    UInt64 size_limit;
};

/// Container for postings lists for each segment
using SegmentedPostingsListContainer = std::unordered_map<UInt32, GinIndexPostingsListPtr>;

/// Postings lists and terms built from query string
using PostingsCache = std::unordered_map<std::string, SegmentedPostingsListContainer>;
using PostingsCachePtr = std::shared_ptr<PostingsCache>;

/// Gin Index Segment information, which contains:
struct GinIndexSegment
{
    ///  Segment ID retrieved from next available ID from file .gin_sid
    UInt32 segment_id = 0;

    /// Next row ID for this segment
    UInt32 next_row_id = 1;

    /// .gin_post file offset of this segment's postings lists
    UInt64 postings_start_offset = 0;

    /// .term_dict file offset of this segment's term dictionaries
    UInt64 term_dict_start_offset = 0;
};

using GinIndexSegments = std::vector<GinIndexSegment>;

using GinIndexPostingsBuilderPtr = std::shared_ptr<GinIndexPostingsBuilder>;

/// Container for all term's Gin Index Postings List Builder
using GinIndexPostingsBuilderContainer = std::unordered_map<std::string, GinIndexPostingsBuilderPtr>;
struct SegmentTermDictionary
{
    /// .gin_post file offset of this segment's postings lists
    UInt64 postings_start_offset;

    /// .gin_dict file offset of this segment's term dictionaries
    UInt64 term_dict_start_offset;

    /// Finite State Transducer, which can be viewed as a map of <term, offset>, where offset is the
    /// offset to the term's posting list in postings list file
    FST::FiniteStateTransducer offsets;
};

using SegmentTermDictionaryPtr = std::shared_ptr<SegmentTermDictionary>;

/// Term dictionaries indexed by segment ID
using SegmentTermDictionaries = std::unordered_map<UInt32, SegmentTermDictionaryPtr>;

/// Gin Index Store which has Gin Index meta data for the corresponding Data Part
class GinIndexStore
{
public:
    explicit GinIndexStore(const String & name_, DataPartStoragePtr storage_);

    GinIndexStore(const String& name_, DataPartStoragePtr storage_, MutableDataPartStoragePtr data_part_storage_builder_, UInt64 max_digestion_size_);

    /// Check existence by checking the existence of file .gin_sid
    bool exists() const;

    /// Get a range of next 'numIDs' available row IDs
    UInt32 getNextRowIDRange(size_t numIDs);

    /// Get next available segment ID by updating file .gin_sid
    UInt32 getNextSegmentID();

    /// Get total number of segments in the store
    UInt32 getNumOfSegments();

    /// Get current postings list builder
    const GinIndexPostingsBuilderContainer& getPostings() const { return current_postings; }

    /// Set postings list builder for given term
    void setPostingsBuilder(const String & term, GinIndexPostingsBuilderPtr builder) { current_postings[term] = builder; }
    /// Check if we need to write segment to Gin index files
    bool needToWrite() const;

    /// Accumulate the size of text data which has been digested
    void incrementCurrentSizeBy(UInt64 sz) { current_size += sz; }

    UInt32 getCurrentSegmentID() const { return current_segment.segment_id;}

    /// Do last segment writing
    void finalize();

    /// method for writing segment data to Gin index files
    void writeSegment();

    const String & getName() const {return name;}

private:
    friend class GinIndexStoreDeserializer;

    /// Initialize all indexing files for this store
    void initFileStreams();

    /// Get a range of next available segment IDs by updating file .gin_sid
    UInt32 getNextSegmentIDRange(const String &file_name, size_t n);

    String name;
    DataPartStoragePtr storage;
    MutableDataPartStoragePtr data_part_storage_builder;

    UInt32 cached_segment_num = 0;

    std::mutex gin_index_store_mutex;

    /// Terms dictionaries which are loaded from .gin_dict files
    SegmentTermDictionaries term_dicts;

    /// container for building postings lists during index construction
    GinIndexPostingsBuilderContainer current_postings;

    /// The following is for segmentation of Gin index
    GinIndexSegment current_segment{};
    UInt64 current_size = 0;
    const UInt64 max_digestion_size = 0;

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

/// GinIndexStores indexed by part file path
using GinIndexStores = std::unordered_map<std::string, GinIndexStorePtr>;

/// PostingsCacheForStore contains postings lists from 'store' which are retrieved from Gin index files for the terms in query strings
/// PostingsCache is per query string(one query can have multiple query strings): when skipping index(row ID ranges) is used for the part during the
/// query, the postings cache is created and associated with the store where postings lists are read
/// for the tokenized query string. The postings caches are released automatically when the query is done.
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
    GinIndexStorePtr get(const String& name, DataPartStoragePtr storage);

    /// Remove all Gin index files which are under the same part_path
    void remove(const String& part_path);

private:
    GinIndexStores stores;
    std::mutex stores_mutex;
};

/// Term dictionary information, which contains:

/// Gin Index Store Reader which helps to read segments, term dictionaries and postings list
class GinIndexStoreDeserializer : private boost::noncopyable
{
public:
    explicit GinIndexStoreDeserializer(const GinIndexStorePtr & store_);

    /// Read all segment information from .gin_seg files
    void readSegments();

    /// Read all term dictionaries from .gin_dict files
    void readSegmentTermDictionaries();

    /// Read term dictionary for given segment id
    void readSegmentTermDictionary(UInt32 segment_id);

    /// Read postings lists for the term
    SegmentedPostingsListContainer readSegmentedPostingsLists(const String& term);

    /// Read postings lists for terms(which are created by tokenzing query string)
    PostingsCachePtr createPostingsCacheFromTerms(const std::vector<String>& terms);

private:
    /// Initialize Gin index files
    void initFileStreams();

    /// The store for the reader
    GinIndexStorePtr store;

    /// File streams for reading Gin Index
    std::unique_ptr<ReadBufferFromFileBase> segment_file_stream;
    std::unique_ptr<ReadBufferFromFileBase> term_dict_file_stream;
    std::unique_ptr<ReadBufferFromFileBase> postings_file_stream;

    /// Current segment, used in building index
    GinIndexSegment current_segment;
};

}
