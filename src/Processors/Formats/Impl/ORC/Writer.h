#pragma once

#include <Processors/Formats/Impl/ORC/Statistics.h>
#include <Processors/Formats/Impl/ORC/Stream.h>
#include <Processors/Formats/Impl/ORC/Type.h>
#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>

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
    /// `column_types` are the ClickHouse types (without `LowCardinality`) of the columns of the file,
    /// which are the fields of `schema` (a struct). The values are converted as follows:
    /// the unsigned integers are written as the signed ones of the same width (but `Date` as is);
    /// `DateTime` and `DateTime64` as TIMESTAMP; `FixedString`, `IPv6` and the big integers
    /// as their raw bytes; `Variant` as UNION with the branches in the order of its variants.
    Writer(const Type & schema_, const DataTypes & column_types, OutputStream & out_, const WriterOptions & options_);
    ~Writer();

    /// Adds the rows of the columns (in the order of the fields of the schema).
    void add(const Columns & columns, size_t num_rows);

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
