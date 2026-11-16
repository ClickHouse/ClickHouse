#pragma once

#include <Processors/Formats/Impl/ORC/ColumnReader.h>
#include <Processors/Formats/Impl/ORC/SearchArgument.h>
#include <Processors/Formats/Impl/ORC/Vector.h>
#include <Common/ListWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>

#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

/// The reader of the ORC files: https://orc.apache.org/specification/ORCv1/

namespace DB::ORC
{

/// Random access to the file.
class InputStream
{
public:
    virtual ~InputStream() = default;

    virtual UInt64 getLength() const = 0;
    /// Reads exactly `length` bytes at `offset`. Must be thread-safe if `readAsync` is used.
    virtual void read(void * buf, UInt64 length, UInt64 offset) = 0;
    /// Reads in the background. The default implementation reads synchronously.
    virtual std::future<void> readAsync(void * buf, UInt64 length, UInt64 offset);
    virtual const std::string & getName() const = 0;
};

struct ReaderOptions
{
    /// Two ranges are read with one request if the gap between them is at most this size...
    UInt64 hole_size_limit = 8192;
    /// ... and the combined range is at most this size.
    UInt64 range_size_limit = 32 * 1024 * 1024;
};

struct RowReaderOptions
{
    /// The ids of the columns to read, with the same semantics as in the reference implementation
    /// (see `selectColumnsByTypeIds`). If not set, all columns are read.
    std::optional<VectorWithMemoryTracking<UInt64>> include_types;
    /// Read only the stripes that start in `[range_offset, range_offset + range_length)`.
    UInt64 range_offset = 0;
    UInt64 range_length = std::numeric_limits<UInt64>::max();
    /// For the dictionary-encoded string columns, fill `EncodedStringVectorBatch`.
    bool enable_lazy_decoding = false;
    /// The time zone to show the values of the TIMESTAMP (not TIMESTAMP_INSTANT) columns in.
    std::string timezone_name = "GMT";
    std::shared_ptr<SearchArgument> search_argument;
};

class RowReader;

class Reader
{
public:
    Reader(std::unique_ptr<InputStream> stream_, const ReaderOptions & options_);
    ~Reader();

    const Type & getType() const { return *schema; }
    UInt64 getNumberOfRows() const { return footer.number_of_rows; }
    UInt64 getNumberOfStripes() const { return footer.stripes.size(); }
    const Proto::StripeInformation & getStripe(size_t index) const { return footer.stripes.at(index); }
    static UInt64 getStripeLength(const Proto::StripeInformation & stripe) { return stripe.index_length + stripe.data_length + stripe.footer_length; }
    /// The number of rows in the stripes before the stripe `index`.
    UInt64 getStripeFirstRow(size_t index) const { return stripe_first_rows.at(index); }

    const Proto::Footer & getFooter() const { return footer; }
    const Proto::PostScript & getPostScript() const { return postscript; }
    CompressionKind getCompression() const { return postscript.compression; }
    /// Whether the bloom filters of the file were written by a version of the C++ writer
    /// that hashed the integers incorrectly (ORC-1024), so they must not be used.
    bool hasBadBloomFilters() const;
    UInt64 getCompressionBlockSize() const { return postscript.compression_block_size; }

    /// The statistics of the whole file, one per column.
    const VectorWithMemoryTracking<Proto::ColumnStatistics> & getStatistics() const { return footer.statistics; }
    /// The statistics of the stripes, read on the first call. Can be empty.
    const Proto::Metadata & getMetadata();

    /// Reads the stripe footer. Not thread-safe.
    Proto::StripeFooter readStripeFooter(size_t stripe_index);

    std::unique_ptr<RowReader> createRowReader(const RowReaderOptions & options);

    /// Starts reading the data of the selected columns of the stripes in the background.
    void preBuffer(const VectorWithMemoryTracking<UInt32> & stripes, const VectorWithMemoryTracking<UInt64> & include_types);
    /// Releases the prefetched data that ends before `boundary`.
    void releaseBuffer(UInt64 boundary);

    /// A piece of the file in memory.
    struct Slice
    {
        std::shared_ptr<const PODArray<char>> holder;
        std::string_view data;
    };

    /// Reads the ranges `(offset, length)` of the file, from the prefetched data if possible.
    /// The adjacent ranges are coalesced according to the options.
    VectorWithMemoryTracking<Slice> readRanges(const VectorWithMemoryTracking<std::pair<UInt64, UInt64>> & ranges);

    /// Decompresses a whole metadata stream (a footer, an index).
    std::string decompress(std::string_view data, const std::string & name) const;

private:
    void readTail();

    struct CachedRange
    {
        UInt64 offset;
        UInt64 length;
        std::shared_ptr<PODArray<char>> buffer;
        std::future<void> future;
        bool ready = false;
    };

    /// Returns the ranges of the streams of the selected columns in the stripe, and the stripe footer.
    VectorWithMemoryTracking<std::pair<UInt64, UInt64>> getStripeRanges(size_t stripe_index, const Proto::StripeFooter & stripe_footer, const VectorWithMemoryTracking<bool> & selected) const;

    std::unique_ptr<InputStream> stream;
    ReaderOptions options;
    Proto::PostScript postscript;
    UInt64 postscript_length = 0;
    Proto::Footer footer;
    VectorWithMemoryTracking<UInt64> stripe_first_rows;
    std::optional<Proto::Metadata> metadata;
    std::unique_ptr<Type> schema;

    std::mutex cache_mutex;
    ListWithMemoryTracking<CachedRange> cache;
};

/// Reads the rows of the selected stripes and columns in batches.
class RowReader
{
public:
    RowReader(Reader & reader_, const RowReaderOptions & options_);
    ~RowReader();

    std::unique_ptr<ColumnVectorBatch> createRowBatch() const;

    /// Fills the batch with the next rows, at most `max_rows`. Returns false at the end.
    bool next(ColumnVectorBatch & batch, size_t max_rows);

    /// The type of the file with only the selected columns. The batches have this structure.
    const Type & getSelectedType() const { return *selected_type; }

    /// The row number in the file of the first row of the batch filled by the last successful `next`.
    UInt64 getRowNumber() const { return batch_first_row; }

private:
    class StripeStreamsImpl;

    void startNextStripe();
    void loadStripeIndex();
    UInt64 numberOfRowGroups() const;
    void pickRowGroups();
    void seekToRowGroup(size_t row_group);
    UInt64 advanceToNextRowGroup(UInt64 current_row) const;
    UInt64 computeBatchSize(UInt64 requested, UInt64 current_row) const;
    bool evaluateStatistics(const VectorWithMemoryTracking<Proto::ColumnStatistics> & stats) const;
    const DateLUTImpl & getWriterTimezone(const Proto::StripeFooter & stripe_footer) const;

    Reader & reader;
    RowReaderOptions options;
    VectorWithMemoryTracking<bool> selected_columns;
    std::unique_ptr<Type> selected_type;
    const DateLUTImpl & reader_timezone;

    UInt64 current_stripe = 0;
    UInt64 last_stripe = 0;
    UInt64 rows_in_current_stripe = 0;
    UInt64 current_row_in_stripe = 0;
    UInt64 batch_first_row = 0;
    bool file_statistics_evaluated = false;
    bool file_statistics_result = true;

    std::unique_ptr<StripeStreamsImpl> stripe_streams;
    std::unique_ptr<ColumnReader> column_reader;

    /// For the predicate pushdown, by the column id.
    UnorderedMapWithMemoryTracking<UInt64, Proto::RowIndex> row_indexes;
    UnorderedMapWithMemoryTracking<UInt64, VectorWithMemoryTracking<std::shared_ptr<BloomFilter>>> bloom_filters;
    /// For each row group of the current stripe: zero if it is skipped, otherwise the row (in the
    /// stripe) of the next skipped row group or the number of rows in the stripe.
    VectorWithMemoryTracking<UInt64> next_skipped_rows;
};

}
