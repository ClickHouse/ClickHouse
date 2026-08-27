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
///     - Its file format is an array of GinIndexSegment as defined in this file.
///     - postings_start_offset points to the file(.gin_post) starting position for the segment's postings list.
///     - dict_start_offset points to the file(.gin_dict) starting position for the segment's dictionaries.
///  3. Dictionary file(.gin_dict): it contains dictionaries.
///     - It contains an array of (FST_size, FST_blob) which has size and actual data of FST.
///  4. Postings Lists(.gin_post): it contains postings lists data.
///     - It contains an array of serialized postings lists.
///
/// During the searching in the segment, the segment's meta data can be found in .gin_seg file. From the meta data,
/// the starting position of its dictionary is used to locate its FST. Then FST is read into memory.
/// By using the term and FST, the offset("output" in FST) of the postings list for the term
/// in FST is found. The offset plus the postings_start_offset is the file location in .gin_post file
/// for its postings list.

namespace DB
{
static constexpr UInt64 UNLIMITED_SEGMENT_DIGESTION_THRESHOLD_BYTES = 0;

/// GinIndexPostingsList which uses 32-bit Roaring
using GinIndexPostingsList = roaring::Roaring;
using GinIndexPostingsListPtr = std::shared_ptr<GinIndexPostingsList>;

class GinIndexCompressionFactory
{
public:
    static const CompressionCodecPtr & zstdCodec();
};

#if USE_FASTPFOR
/// This class is responsible to serialize the posting list into on-disk format by applying DELTA encoding first, then PFOR compression.
/// Internally, the FastPFOR library is used for the PFOR compression.
class GinIndexPostingListDeltaPforSerialization
{
public:
    static UInt64 serialize(WriteBuffer & buffer, const roaring::Roaring & rowids);
    static GinIndexPostingsListPtr deserialize(ReadBuffer & buffer);

private:
    static std::shared_ptr<FastPForLib::IntegerCODEC> codec();
    static std::vector<UInt32> encodeDeltaScalar(const roaring::Roaring & rowids);
    static void decodeDeltaScalar(std::vector<UInt32> & deltas);

    /// FastPFOR fails to compress below this threshold, compressed data becomes larger than the original array.
    static constexpr size_t FASTPFOR_THRESHOLD = 4;
};
#endif

/// This class is responsible to serialize the posting list into on-disk format by applying ZSTD compression on top of Roaring Bitmap.
class GinIndexPostingListRoaringZstdSerialization
{
public:
    static UInt64 serialize(WriteBuffer & buffer, const roaring::Roaring & rowids);
    static GinIndexPostingsListPtr deserialize(ReadBuffer & buffer);

private:
    static constexpr size_t MIN_SIZE_FOR_ROARING_ENCODING = 16;
    static constexpr size_t ROARING_ENCODING_COMPRESSION_CARDINALITY_THRESHOLD = 5000;
    static constexpr UInt64 ARRAY_CONTAINER_MASK = 0x1;
    static constexpr UInt64 ROARING_CONTAINER_MASK = 0x0;
    static constexpr UInt64 ROARING_COMPRESSED_MASK = 0x1;
    static constexpr UInt64 ROARING_UNCOMPRESSED_MASK = 0x0;
};

/// Build a postings list for a term
class GinIndexPostingsBuilder
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
    static GinIndexPostingsListPtr deserialize(ReadBuffer & buffer);

private:
    enum class Serialization : UInt8
    {
        ROARING_ZSTD = 1,
        DELTA_PFOR = 2,
    };

    roaring::Roaring rowids;
};

using GinIndexPostingsBuilderPtr = std::shared_ptr<GinIndexPostingsBuilder>;

/// Gin index segment descriptor, which contains:
struct GinIndexSegment
{
    ///  Segment ID retrieved from next available ID from file .gin_sid
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
class GinSegmentDictionaryBloomFilter
{
public:
    GinSegmentDictionaryBloomFilter(UInt64 unique_count_, size_t bits_per_rows_, size_t num_hashes_);

    /// Adds token to bloom filter
    void add(std::string_view token);

    /// Does the token exist according to the bloom filter?
    bool contains(std::string_view token);

    /// Serialize into WriteBuffer
    UInt64 serialize(WriteBuffer & write_buffer);
    /// Deserialize from ReadBuffer

    static std::unique_ptr<GinSegmentDictionaryBloomFilter> deserialize(ReadBuffer & read_buffer);

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

struct GinSegmentDictionary
{
    /// .gin_bflt file offset of this segment's bloom filter
    UInt64 bloom_filter_start_offset;

    /// .gin_post file offset of this segment's postings lists
    UInt64 postings_start_offset;

    /// .gin_dict file offset of this segment's dictionaries
    UInt64 dict_start_offset;

    /// (Minimized) Finite State Transducer, which can be viewed as a map of <term, offset>, where offset is the
    /// offset to the term's posting list in postings list file
    std::unique_ptr<FST::FiniteStateTransducer> fst;
    std::mutex fst_mutex;

    /// Bloom filter created from the segment's dictionary
    std::unique_ptr<GinSegmentDictionaryBloomFilter> bloom_filter;
};

using GinSegmentDictionaryPtr = std::shared_ptr<GinSegmentDictionary>;

/// Gin index store which has gin index meta data for the corresponding column data part
class GinIndexStore
{
public:
    static constexpr auto GIN_SEGMENT_ID_FILE_TYPE = ".gin_sid";
    static constexpr auto GIN_SEGMENT_METADATA_FILE_TYPE = ".gin_seg";
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
            metadata_file_size -= other.metadata_file_size;
            bloom_filter_file_size -= other.bloom_filter_file_size;
            dictionary_file_size -= other.dictionary_file_size;
            posting_lists_file_size -= other.posting_lists_file_size;
            return *this;
        }

    private:
        size_t num_terms;
        size_t current_size;
        size_t metadata_file_size;
        size_t bloom_filter_file_size;
        size_t dictionary_file_size;
        size_t posting_lists_file_size;
    };

    /// Container for all term's Gin Index Postings List Builder
    using GinIndexPostingsBuilderContainer = absl::flat_hash_map<String, GinIndexPostingsBuilderPtr>;

    GinIndexStore(const String & name_, DataPartStoragePtr storage_);
    GinIndexStore(
        const String & name_,
        DataPartStoragePtr storage_,
        MutableDataPartStoragePtr data_part_storage_builder_,
        UInt64 segment_digestion_threshold_bytes_,
        double bloom_filter_false_positive_rate_);

    /// Check existence by checking the existence of file .gin_sid
    bool exists() const;

    /// Get a range of next 'numIDs'-many available row IDs
    UInt32 getNextRowIDRange(size_t numIDs);

    /// Get next available segment ID by updating file .gin_sid
    UInt32 getNextSegmentID();

    /// Get total number of segments in the store
    UInt32 getNumOfSegments();

    /// Get version
    Format getVersion();

    /// Get current postings list builder
    const GinIndexPostingsBuilderContainer & getPostingsListBuilder() const { return current_postings; }

    /// Set postings list builder for given term
    void setPostingsBuilder(const String & term, GinIndexPostingsBuilderPtr builder) { current_postings[term] = builder; }

    /// Check if we need to write segment to Gin index files
    bool needToWriteCurrentSegment() const;

    /// Accumulate the size of text data which has been digested
    void incrementCurrentSizeBy(UInt64 sz) { current_size += sz; }

    UInt32 getCurrentSegmentID() const { return current_segment.segment_id; }

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

    /// Stores segment id into disk
    void writeSegmentId();

    /// Get a range of next available segment IDs
    UInt32 getNextSegmentIDRange(size_t n);

    String name;
    DataPartStoragePtr storage;
    MutableDataPartStoragePtr data_part_storage_builder;

    UInt32 cached_segment_num = 0;

    std::mutex mutex;

    /// Not thread-safe, protected by mutex
    UInt32 next_available_segment_id = 0;

    /// Dictionaries indexed by segment ID
    using GinSegmentDictionaries = std::unordered_map<UInt32, GinSegmentDictionaryPtr>;

    /// Term's dictionaries which are loaded from .gin_dict files
    GinSegmentDictionaries segment_dictionaries;

    /// Container for building postings lists during index construction
    GinIndexPostingsBuilderContainer current_postings;

    /// For the segmentation of Gin indexes
    GinIndexSegment current_segment;
    UInt64 current_size = 0;
    const UInt64 segment_digestion_threshold_bytes = 0;
    const double bloom_filter_false_positive_rate = 0.0;

    /// File streams for segment, bloom filter, dictionaries and postings lists
    std::unique_ptr<WriteBufferFromFileBase> metadata_file_stream;
    std::unique_ptr<WriteBufferFromFileBase> bloom_filter_file_stream;
    std::unique_ptr<WriteBufferFromFileBase> dict_file_stream;
    std::unique_ptr<WriteBufferFromFileBase> postings_file_stream;

    LoggerPtr logger = getLogger("TextIndex");
};

using GinIndexStorePtr = std::shared_ptr<GinIndexStore>;

/// Container for postings lists for each segment
using GinSegmentedPostingsListContainer = std::unordered_map<UInt32, GinIndexPostingsListPtr>;

/// Postings lists and terms built from query string
using GinPostingsCache = std::unordered_map<String, GinSegmentedPostingsListContainer>;
using GinPostingsCachePtr = std::shared_ptr<GinPostingsCache>;

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
    void readSegmentFST(UInt32 segment_id, GinSegmentDictionaryPtr segment_dictionary);

    /// Read postings lists for the term
    GinSegmentedPostingsListContainer readSegmentedPostingsLists(const String & term);

    /// Read postings lists for terms (which are created by tokenzing query string)
    GinPostingsCachePtr createPostingsCacheFromTerms(const std::vector<String> & terms);

private:
    /// Initialize gin index files
    void initFileStreams();

    /// The store for the reader
    GinIndexStorePtr store;

    /// File streams for reading Gin Index
    std::unique_ptr<ReadBufferFromFileBase> metadata_file_stream;
    std::unique_ptr<ReadBufferFromFileBase> bloom_filter_file_stream;
    std::unique_ptr<ReadBufferFromFileBase> dict_file_stream;
    std::unique_ptr<ReadBufferFromFileBase> postings_file_stream;

    LoggerPtr logger = getLogger("TextIndex");
};

/// PostingsCacheForStore contains postings lists from 'store' which are retrieved from Gin index files for the terms in query strings
/// GinPostingsCache is per query string (one query can have multiple query strings): when skipping index (row ID ranges) is used for the part during the
/// query, the postings cache is created and associated with the store where postings lists are read
/// for the tokenized query string. The postings caches are released automatically when the query is done.
struct PostingsCacheForStore
{
    /// Which store to retrieve postings lists
    GinIndexStorePtr store;

    /// map of <query, postings lists>
    std::unordered_map<String, GinPostingsCachePtr> cache;

    /// Get postings lists for query string, return nullptr if not found
    GinPostingsCachePtr getPostings(const String & query_string) const;
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
