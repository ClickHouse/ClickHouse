#pragma once

#include <Common/FST.h>
#include <Common/Logger.h>
#include <Compression/ICompressionCodec.h>
#include <Disks/IDisk.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Interpreters/BloomFilter.h>
#include <Storages/MergeTree/IDataPartStorage.h>

#include <roaring.hh>
#include <mutex>
#include <unordered_map>
#include <vector>
#include <absl/container/flat_hash_map.h>

#include "config.h"

#if USE_FASTPFOR
#  include <codecfactory.h>
#endif


/// GinIndexStore manages the Generalized Inverted Index ("gin") (text index) for a data part, and it is made up of one or more
/// immutable index segments.
///
/// There are 4 types of index files in a store:
///  1. Segment ID file(.gin_sid): it contains one byte for version followed by the next available segment ID.
///  2. Segment Metadata file(.gin_seg): it contains index segment metadata.
///     - Its file format is an array of GinSegmentDescriptor as defined in this file.
///     - postings_start_offset points to the file(.gin_post) starting position for the segment's postings list.
///     - dict_start_offset points to the file(.gin_dict) starting position for the segment's dictionaries.
///  3. Dictionary file(.gin_dict): it contains dictionaries.
///     - It contains an array of (FST_size, FST_blob) which has size and actual data of FST.
///  4. Postings Lists(.gin_post): it contains postings lists data.
///     - It contains an array of serialized postings lists.
///
/// During the searching in the segment, the segment's meta data can be found in .gin_seg file. From the meta data,
/// the starting position of its dictionary is used to locate its FST. Then FST is read into memory.
/// By using the token and FST, the offset("output" in FST) of the postings list for the token
/// in FST is found. The offset plus the postings_start_offset is the file location in .gin_post file
/// for its postings list.

namespace DB
{
static constexpr UInt64 UNLIMITED_SEGMENT_DIGESTION_THRESHOLD_BYTES = 0;

/// An in-memory posting list is a 32-bit Roaring Bitmap
using GinPostingsList = roaring::Roaring;
using GinPostingsListPtr = std::shared_ptr<GinPostingsList>;

class GinCompressionFactory
{
public:
    static const CompressionCodecPtr & zstdCodec();
};

#if USE_FASTPFOR
/// This class serializes a posting list into on-disk format by applying DELTA encoding first, then PFOR compression.
/// Internally, the FastPFOR library is used for the PFOR compression.
class GinPostingListDeltaPforSerialization
{
public:
    static UInt64 serialize(WriteBuffer & buffer, const GinPostingsList & rowids);
    static GinPostingsListPtr deserialize(ReadBuffer & buffer);

private:
    static std::shared_ptr<FastPForLib::IntegerCODEC> codec();
    static std::vector<UInt32> encodeDeltaScalar(const GinPostingsList & rowids);
    static void decodeDeltaScalar(std::vector<UInt32> & deltas);

    /// FastPFOR fails to compress below this threshold, compressed data becomes larger than the original array.
    static constexpr size_t FASTPFOR_THRESHOLD = 4;
};
#endif

/// This class serialize a posting list into on-disk format by applying ZSTD compression on top of Roaring Bitmap.
class GinPostingListRoaringZstdSerialization
{
public:
    static UInt64 serialize(WriteBuffer & buffer, const GinPostingsList & rowids);
    static GinPostingsListPtr deserialize(ReadBuffer & buffer);

private:
    static constexpr size_t MIN_SIZE_FOR_ROARING_ENCODING = 16;
    static constexpr size_t ROARING_ENCODING_COMPRESSION_CARDINALITY_THRESHOLD = 5000;
    static constexpr UInt64 ARRAY_CONTAINER_MASK = 0x1;
    static constexpr UInt64 ROARING_CONTAINER_MASK = 0x0;
    static constexpr UInt64 ROARING_COMPRESSED_MASK = 0x1;
    static constexpr UInt64 ROARING_UNCOMPRESSED_MASK = 0x0;
};

/// Build a postings list for a token
class GinPostingsListBuilder
{
public:
    /// Check whether a row_id is already added
    bool contains(UInt32 row_id) const;

    /// Add a row_id into the builder
    void add(UInt32 row_id);

    /// Serializes the content of builder into given WriteBuffer.
    /// Returns the number of bytes written into WriteBuffer.
    UInt64 serialize(WriteBuffer & buffer);

    /// Deserializes the postings list data from given ReadBuffer.
    /// Returns a pointer to the GinIndexPostingsList created by deserialization.
    static GinPostingsListPtr deserialize(ReadBuffer & buffer);

private:
    enum class Serialization : UInt8
    {
        ROARING_ZSTD = 1,
        DELTA_PFOR = 2,
    };

    GinPostingsList rowids;
};

using GinPostingsListBuilderPtr = std::shared_ptr<GinPostingsListBuilder>;

/// Gin index segment descriptor, which contains:
struct GinSegmentDescriptor
{
    /// Segment ID retrieved from next available ID from file .gin_sid
    UInt32 segment_id = 0;

    /// Start row ID for this segment
    UInt32 next_row_id = 1;

    /// .gin_bflt file offset of this segment's bloom filter
    UInt64 bloom_filter_start_offset = 0;

    /// .gin_dict file offset of this segment's dictionaries
    UInt64 dict_start_offset = 0;

    /// .gin_post file offset of this segment's postings lists
    UInt64 postings_start_offset = 0;
};

/// This class encapsulates an instance of `BloomFilter` class.
/// The main responsibility is handling the serialization.
class GinDictionaryBloomFilter
{
public:
    GinDictionaryBloomFilter(UInt64 unique_count_, size_t bits_per_rows_, size_t num_hashes_);

    /// Adds token to bloom filter
    void add(std::string_view token);

    /// Does the token exist according to the bloom filter?
    bool contains(std::string_view token);

    /// Serialize into WriteBuffer
    UInt64 serialize(WriteBuffer & write_buffer);
    /// Deserialize from ReadBuffer

    static std::unique_ptr<GinDictionaryBloomFilter> deserialize(ReadBuffer & read_buffer);

private:
    /// Estimated number of entries
    const UInt64 unique_count;
    /// Bit size of the bloom filter
    const UInt64 bits_per_row;
    /// Number of hash functions used by the bloom filter
    const UInt64 num_hashes;

    /// Encapsulated BloomFilter instance
    BloomFilter bloom_filter;
};

struct GinDictionary
{
    /// .gin_bflt file offset of this segment's bloom filter
    UInt64 bloom_filter_start_offset;

    /// .gin_post file offset of this segment's postings lists
    UInt64 postings_start_offset;

    /// .gin_dict file offset of this segment's dictionaries
    UInt64 dict_start_offset;

    /// (Minimized) Finite State Transducer, which can be viewed as a map of <token, offset>, where offset is the
    /// offset to the token's posting list in postings list file
    std::unique_ptr<FST::FiniteStateTransducer> fst;
    std::mutex fst_mutex;

    /// Bloom filter created from the dictionary
    std::unique_ptr<GinDictionaryBloomFilter> bloom_filter;
};

using GinDictionaryPtr = std::shared_ptr<GinDictionary>;

/// Gin index store which has gin index meta data for the corresponding column data part
class GinIndexStore
{
public:
    static constexpr auto GIN_SEGMENT_ID_FILE_TYPE = ".gin_sid";
    static constexpr auto GIN_SEGMENT_DESCRIPTOR_FILE_TYPE = ".gin_seg";
    static constexpr auto GIN_BLOOM_FILTER_FILE_TYPE = ".gin_bflt";
    static constexpr auto GIN_DICTIONARY_FILE_TYPE = ".gin_dict";
    static constexpr auto GIN_POSTINGS_FILE_TYPE = ".gin_post";

    enum class Format : uint8_t
    {
        v1 = 1, /// Initial version, supports adaptive compression
    };

    class Statistics
    {
    public:
        explicit Statistics(const GinIndexStore & store);

        String toString() const;

        Statistics operator-(const Statistics & other)
        {
            segment_descriptor_file_size -= other.segment_descriptor_file_size;
            bloom_filter_file_size -= other.bloom_filter_file_size;
            dictionary_file_size -= other.dictionary_file_size;
            posting_lists_file_size -= other.posting_lists_file_size;
            return *this;
        }

    private:
        size_t num_tokens;
        size_t current_size_bytes;
        size_t segment_descriptor_file_size;
        size_t bloom_filter_file_size;
        size_t dictionary_file_size;
        size_t posting_lists_file_size;
    };

    /// All token's postings list builder
    using GinTokenPostingsLists = absl::flat_hash_map<String, GinPostingsListBuilderPtr>;

    GinIndexStore(const String & name_, DataPartStoragePtr storage_);
    GinIndexStore(
        const String & name_,
        DataPartStoragePtr storage_,
        MutableDataPartStoragePtr data_part_storage_builder_,
        UInt64 segment_digestion_threshold_bytes_,
        double bloom_filter_false_positive_rate_);

    /// Check existence by checking the existence of file .gin_sid
    bool exists() const;

    /// Get a range of next n available row IDs
    UInt32 getNextRowIdRange(size_t n);

    /// Get next available segment ID by updating file .gin_sid
    UInt32 getNextSegmentId();

    /// Get total number of segments in the store
    UInt32 getNumOfSegments();

    /// Get version
    Format getVersion();

    /// Get current postings list builder
    const GinTokenPostingsLists & getTokenPostingsLists() const { return token_postings_lists; }

    /// Set postings list builder for given token
    void setPostingsListBuilder(const String & token, GinPostingsListBuilderPtr builder) { token_postings_lists[token] = builder; }

    /// Check if we need to write segment to Gin index files
    bool needToWriteCurrentSegment() const;

    /// Accumulate the size of text data which has been digested
    void incrementCurrentSizeBy(UInt64 bytes) { current_size_bytes += bytes; }

    UInt32 getCurrentSegmentId() const { return current_segment.segment_id; }

    /// Do last segment writing
    void finalize();
    void cancel() noexcept;

    /// Method for writing segment data to Gin index files
    void writeSegment();

    const String & getName() const { return name; }

    Statistics getStatistics();

private:
    /// FST size less than 100KiB does not worth to compress.
    static constexpr auto FST_SIZE_COMPRESSION_THRESHOLD = 100_KiB;
    /// Current version of GinIndex to store FST
    static constexpr auto CURRENT_GIN_FILE_FORMAT_VERSION = Format::v1;

    friend class GinIndexStoreDeserializer;

    /// Initialize all indexing files for this store
    void initFileStreams();

    /// Initialize segment ID by either reading from file .gin_sid or setting to default value
    void initSegmentId();

    /// Stores segment ID to disk
    void writeSegmentId();

    const String name;
    const DataPartStoragePtr storage;
    MutableDataPartStoragePtr data_part_storage_builder;

    UInt32 cached_segment_num = 0;

    std::mutex mutex;

    /// Not thread-safe, protected by mutex
    UInt32 next_available_segment_id = 0;

    /// Dictionaries indexed by segment ID
    using GinSegmentDictionaries = std::unordered_map<UInt32, GinDictionaryPtr>;

    /// Token's dictionaries which are loaded from .gin_dict files
    GinSegmentDictionaries segment_dictionaries;

    /// Container for building postings lists during index construction
    GinTokenPostingsLists token_postings_lists;

    /// For the segmentation of Gin indexes
    GinSegmentDescriptor current_segment;
    UInt64 current_size_bytes = 0;

    /// File streams for segment, bloom filter, dictionaries and postings lists
    std::unique_ptr<WriteBufferFromFileBase> segment_descriptor_file_stream;
    std::unique_ptr<WriteBufferFromFileBase> bloom_filter_file_stream;
    std::unique_ptr<WriteBufferFromFileBase> dict_file_stream;
    std::unique_ptr<WriteBufferFromFileBase> postings_file_stream;

    const UInt64 segment_digestion_threshold_bytes = 0;
    const double bloom_filter_false_positive_rate = 0.0;

    LoggerPtr logger = getLogger("TextIndex");
};

using GinIndexStorePtr = std::shared_ptr<GinIndexStore>;

/// Map of <segment_id, postings_list>
using GinSegmentPostingsLists = std::unordered_map<UInt32, GinPostingsListPtr>;

/// Postings lists and tokens built from query string
using GinPostingsListsCache = std::unordered_map<String, GinSegmentPostingsLists>;
using GinPostingsListsCachePtr = std::shared_ptr<GinPostingsListsCache>;

/// Gin index store reader which helps to read segments, dictionaries and postings list
class GinIndexStoreDeserializer : private boost::noncopyable
{
public:
    explicit GinIndexStoreDeserializer(const GinIndexStorePtr & store_);

    /// Read segment information from .gin_seg files
    void readSegments();

    /// Prepare segments for reading
    void prepareSegmentsForReading();

    /// Prepare segment for given segment id
    void prepareSegmentForReading(UInt32 segment_id);

    /// Read FST for given segment dictionary from .gin_dict files
    void readSegmentFST(UInt32 segment_id, GinDictionary & dictionary);

    /// Read postings lists for the token
    GinSegmentPostingsLists readSegmentPostingsLists(const String & token);

    /// Read postings lists for tokens (which are created by tokenzing query string)
    GinPostingsListsCachePtr createPostingsListsCacheFromTokens(const std::vector<String> & tokens);

private:
    /// Initialize gin index files
    void initFileStreams();

    /// The store for the reader
    GinIndexStorePtr store;

    /// File streams for reading Gin Index
    std::unique_ptr<ReadBufferFromFileBase> segment_descriptor_file_stream;
    std::unique_ptr<ReadBufferFromFileBase> bloom_filter_file_stream;
    std::unique_ptr<ReadBufferFromFileBase> dict_file_stream;
    std::unique_ptr<ReadBufferFromFileBase> postings_file_stream;

    LoggerPtr logger = getLogger("TextIndex");
};

/// GinPostingsListsCacheForStore contains postings lists from 'store' which are retrieved from Gin index files for the tokens in query strings
/// GinPostingsListsCache is per query string (one query can have multiple query strings): when skipping index (row ID ranges) is used for the part during the
/// query, the postings cache is created and associated with the store where postings lists are read
/// for the tokenized query string. The postings caches are released automatically when the query is done.
struct GinPostingsListsCacheForStore
{
    /// Which store to retrieve postings lists
    GinIndexStorePtr store;

    /// Map of <query, postings lists>
    std::unordered_map<String, GinPostingsListsCachePtr> cache;

    /// Get postings lists for query string, return nullptr if not found
    GinPostingsListsCachePtr getPostingsLists(const String & query_string) const;
};

/// A singleton for storing GinIndexStores
class GinIndexStoreFactory : private boost::noncopyable
{
public:
    /// Get singleton of GinIndexStoreFactory
    static GinIndexStoreFactory & instance();

    /// Get GinIndexStore by using index name, disk and part_path (which are combined to create key in stores)
    GinIndexStorePtr get(const String & name, DataPartStoragePtr storage);

    /// Remove all Gin index files which are under the same part_path
    void remove(const String & part_path);

    /// GinIndexStores indexed by part file path
    using GinIndexStores = std::unordered_map<String, GinIndexStorePtr>;

private:
    GinIndexStores stores;
    std::mutex mutex;
};

bool isGinFile(const String & file_name);
}
