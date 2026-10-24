#pragma once

#include <Processors/Formats/Impl/ORC/Statistics.h>
#include <Processors/Formats/Impl/ORC/Stream.h>
#include <Processors/Formats/Impl/ORC/Vector.h>

#include <memory>
#include <string>

class DateLUTImpl;

namespace DB::ORC
{

class OutputStream
{
public:
    virtual ~OutputStream() = default;
    virtual void write(const void * buf, size_t length) = 0;
};

struct WriterOptions
{
    CompressionKind compression = CompressionKind::ZSTD;
    UInt64 compression_block_size = 256 * 1024;
    /// The number of rows between the row index entries; 0 means no row index.
    UInt64 row_index_stride = 10000;
    /// Use the dictionary encoding for a string column if the number of distinct values in the
    /// first row group is at most this fraction of the number of values. 0 disables the dictionaries.
    double dictionary_key_size_threshold = 0.0;
    /// The time zone of the writer, which is written into the stripe footers.
    std::string timezone_name = "GMT";
    /// A stripe is written when the estimated size of its data reaches this size.
    UInt64 stripe_size = 64 * 1024 * 1024;
};

class ColumnWriter;
struct WriterContext;

class Writer
{
public:
    Writer(const Type & schema_, OutputStream & out_, const WriterOptions & options_);
    ~Writer();

    std::unique_ptr<ColumnVectorBatch> createRowBatch(size_t capacity) const;

    void add(ColumnVectorBatch & batch);

    /// Writes the remaining data and the file footer.
    void close();

private:
    void writeStripe();
    void write(const std::string & data);

    std::unique_ptr<Type> schema;
    OutputStream & out;
    WriterOptions options;
    std::unique_ptr<Compressor> compressor;
    std::unique_ptr<WriterContext> context;
    std::unique_ptr<ColumnWriter> column_writer;

    Proto::Footer footer;
    Proto::Metadata metadata;
    UInt64 current_offset = 0;
    UInt64 stripe_rows = 0;
    UInt64 index_rows = 0;
    UInt64 total_rows = 0;
};

}
