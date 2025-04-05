#include "Read.h"

#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeUUID.h>
#include <IO/CompressionMethod.h>
#include <IO/VarInt.h>
#include <Interpreters/castColumn.h>
#include <lz4.h>
#if USE_SNAPPY
#include <snappy.h>
#endif

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int COLUMN_QUERIED_MORE_THAN_ONCE;
    extern const int FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME;
    extern const int CANNOT_DECOMPRESS;
}

namespace DB::Parquet
{

namespace
{

/// ClickHouse's in-memory column data format matches Parquet's PLAIN encoding format.
/// We can directly memcpy/decompress into the column if Encoding == PLAIN.
struct FixedSizeValueDecoder : public ValueDecoder
{
    size_t value_size;

    explicit FixedSizeValueDecoder(size_t value_size_) : value_size(value_size_) {}

    bool canReadDirectlyIntoColumn(parq::Encoding::type encoding, size_t num_values, IColumn & col, char ** out_ptr, size_t * out_bytes) const override
    {
        chassert(col.sizeOfValueIfFixed() == value_size);
        if (encoding == parq::Encoding::PLAIN)
        {
            const auto span = col.insertRawUninitialized(num_values);
            *out_ptr = span.data();
            *out_bytes = span.size();
            return true;
        }
        return false;
    }

    void decodePage(parq::Encoding::type encoding, const char * /*data*/, size_t /*bytes*/, size_t /*num_values_in*/, size_t /*num_values_out*/, IColumn &, const UInt8 * /*filter*/) const override
    {
        //TODO (PLAIN, DELTA_BINARY_PACKED, BYTE_STREAM_SPLIT)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Encoding {} for fixed-size types not implemented", thriftToString(encoding));
    }
};

/// Reads INT32 and converts to INT16/INT8. Signed vs unsigned makes no difference.
template <typename T>
struct ShortIntDecoder : public ValueDecoder
{
    void decodePage(parq::Encoding::type encoding, const char * data, size_t bytes, size_t num_values_in, size_t num_values_out, IColumn & col, const UInt8 * filter) const override
    {
        if (encoding == parq::Encoding::PLAIN)
        {
            if (num_values_in * 4 > bytes)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Page data too short");
            const auto span = col.insertRawUninitialized(num_values_out);
            /// `col` may be signed or unsigned, but we cast it to unsigned here (to avoid pointless
            /// extra template instantiations). AFAIU, this is not UB because we go through char*.
            T * out = reinterpret_cast<T *>(span.data());
            chassert(span.size() == num_values_out * sizeof(T));
            if (!filter)
            {
                chassert(num_values_in == num_values_out);
                for (size_t i = 0; i < num_values_in; ++i)
                {
                    /// Read the low bytes of an unaligned 4-byte value.
                    T x;
                    memcpy(&x, data + i * 4, sizeof(T));
                    out[i] = x;
                }
            }
            else
            {
                size_t out_i = 0;
                for (size_t i = 0; i < num_values_in; ++i)
                {
                    if (filter[i])
                    {
                        T x;
                        memcpy(&x, data + i * 4, sizeof(T));
                        out[out_i++] = x;
                    }
                }
            }
        }
        else if (encoding == parq::Encoding::DELTA_BINARY_PACKED)
        {
            //TODO
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "DELTA_BINARY_PACKED for 16/8-bit ints is not implemented");
        }
        else
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected integer encoding: {}", thriftToString(encoding));
        }
    }
};

/// Used for dictionary indices and repetition/definition levels.
/// For dictionary indices, we add 2 to each value to make it compatible with ColumnLowCardinality,
/// which reserves the first two dictionary slots for NULL and default value.
/// Input, output, and filter must all be padded - we may read/write up to 7 elements past the end.
template <typename T, UInt8 ADD, bool FILTERED>
void decodeBitPackedRLE(size_t limit, const size_t bit_width, size_t num_values_in, const char * data, size_t bytes, const UInt8 * filter, T * out)
{
    static_assert(sizeof(T) <= 4, "");
    chassert(bit_width > 0 && bit_width <= 32);
    chassert(limit + ADD <= size_t(std::numeric_limits<T>::max()) + 1);
    const size_t byte_width = (bit_width + 7) / 8;
    const UInt32 value_mask = (1ul << bit_width) - 1;
    const char * end = data + bytes;
    size_t idx = 0;
    size_t filter_idx = 0;
    while (data < end)
    {
        UInt64 len;
        data = readVarUInt(len, data, end - data);
        if (len & 1)
        {
            /// Bit-packed run.
            size_t groups = len >> 1;
            len = groups << 3;
            size_t nbytes = groups * bit_width;
            if (len > num_values_in - idx + 7 || nbytes > size_t(end - data))
                throw Exception(ErrorCodes::INCORRECT_DATA, "Too many RLE-encoded values (bp)");

            /// TODO: For bit_width <= 8, this can probably be made much faster by unrolling 8
            ///       iterations of the loop and using pdep instruction (on x86).
            /// TODO: May make sense to have specialized versions of this loop for some specific
            ///       values of bit_width. E.g. bit_width=1 is very common as def levels for nullables.
            for (size_t bit_idx = 0; bit_idx < (nbytes << 3); bit_idx += bit_width)
            {
                if constexpr (FILTERED)
                {
                    if (!filter[filter_idx++])
                        continue;
                }

                size_t x;
                memcpy(&x, data + (bit_idx >> 3), 8);
                x = (x >> (bit_idx & 7)) & value_mask;

                if (x >= limit)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Dict index or rep/def level out of bounds (bp)");
                x += ADD;
                out[idx++] = x;
            }
            data += nbytes;
        }
        else
        {
            len >>= 1;
            if (len > num_values_in - idx || byte_width > size_t(end - data))
                throw Exception(ErrorCodes::INCORRECT_DATA, "Too many RLE-encoded values (rle)");

            UInt32 x;
            memcpy(&x, data, 4);
            x &= value_mask;

            if (x >= limit)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Dict index or rep/def level out of bounds (rle)");
            x += ADD;

            for (size_t i = 0; i < len; ++i)
            {
                if constexpr (FILTERED)
                {
                    if (!filter[filter_idx++])
                        continue;
                }

                out[idx++] = x;
            }

            data += byte_width;
        }
    }
    if constexpr (!FILTERED)
        filter_idx = idx;
    if (filter_idx < num_values_in || filter_idx > num_values_in + 7)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected number of RLE-encoded values");
}

template <typename T>
struct DictionaryIndexDecoder : public ValueDecoder
{
    size_t limit = 0; // throw if any value is greater than this

    explicit DictionaryIndexDecoder(size_t limit_) : limit(limit_) {}

    void decodePage(parq::Encoding::type encoding, const char * data, size_t bytes, size_t num_values_in, size_t num_values_out, IColumn & col, const UInt8 * filter) const override
    {
        if (encoding != parq::Encoding::RLE && encoding != parq::Encoding::RLE_DICTIONARY)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected non-RLE encoding: {}", thriftToString(encoding));

        if (bytes < 1)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Page too short (dict indices bit width)");
        size_t bit_width = *data;
        data += 1;
        bytes -= 1;
        if (bit_width < 1 || bit_width > 32)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid dict indices bit width: {}", bit_width);

        auto & array = assert_cast<ColumnVector<T> &>(col).getData();
        size_t start = array.size();
        array.resize(start + num_values_out);
        if (filter)
            decodeBitPackedRLE<T, /*ADD*/ 2, /*FILTERED*/ true>(
                limit, bit_width, num_values_in, data, bytes, filter, array.data() + start);
        else
            decodeBitPackedRLE<T, /*ADD*/ 2, /*FILTERED*/ false>(
                limit, bit_width, num_values_in, data, bytes, nullptr, array.data() + start);
    }
};

struct StringDecoder : public ValueDecoder
{
    void decodePage(parq::Encoding::type encoding, const char * data, size_t bytes, size_t num_values_in, size_t /*num_values_out*/, IColumn & col, const UInt8 * filter) const override
    {
        const char * end = data + bytes;
        /// Keep in mind that ColumnString stores a '\0' after each string.
        auto & col_str = assert_cast<ColumnString &>(col);
        if (encoding == parq::Encoding::PLAIN)
        {
            // 4 byte length stored as little endian, followed by bytes.
            col_str.getChars().reserve(col_str.getChars().size() + (bytes - num_values_in * (4 - 1)));
            for (size_t idx = 0; idx < num_values_in; ++idx)
            {
                UInt32 x;
                memcpy(&x, data, 4); /// omitting range check because input is padded
                size_t len = x;
                if (4 + len > size_t(end - data))
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Encoded string is out of bounds");
                if (!filter || filter[idx])
                    col_str.insertData(data + 4, len);
                data += 4 + len;
            }
        }
        else if (encoding == parq::Encoding::DELTA_LENGTH_BYTE_ARRAY)
        {
            //TODO
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "DELTA_LENGTH_BYTE_ARRAY not implemented");
        }
        else if (encoding == parq::Encoding::DELTA_BYTE_ARRAY)
        {
            //TODO
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "DELTA_BYTE_ARRAY not implemented");
        }
        else
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected BYTE_ARRAY encoding: {}", thriftToString(encoding));
    }
};

void decompress(const char * data, size_t compressed_size, size_t uncompressed_size, parq::CompressionCodec::type codec, char * out)
{
    CompressionMethod method = CompressionMethod::None;
    switch (codec)
    {
        case parq::CompressionCodec::UNCOMPRESSED:
            chassert(false);
            break;
        case parq::CompressionCodec::SNAPPY:
#if USE_SNAPPY
        {
            /// Can't use CompressionMethod::Snappy because it dispatches to HadoopSnappyReadBuffer,
            /// which expects some additional header before the compressed block.
            size_t actual_uncompressed_size;
            if (!snappy::GetUncompressedLength(data, compressed_size, &actual_uncompressed_size))
                throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Malformed snappy compressed page (couldn't get uncompressed length)");
            if (actual_uncompressed_size != uncompressed_size)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected uncompressed page size");
            if (!snappy::RawUncompress(data, compressed_size, out))
                throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Malformed snappy compressed page");
            return;
        }
#else
            throw Exception(ErrorCodes::FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME, "Cannot decompress Snappy: ClickHouse was compiled without Snappy support");
#endif
        case parq::CompressionCodec::GZIP:
            method = CompressionMethod::Gzip;
            break;
        case parq::CompressionCodec::LZO:
            /// Arrow also doesn't support it.
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "LZO decompression not implemented");
        case parq::CompressionCodec::BROTLI:
            method = CompressionMethod::Brotli;
            break;
        case parq::CompressionCodec::LZ4:
            /// LZ4 framed.
            method = CompressionMethod::Lz4;
            break;
        case parq::CompressionCodec::ZSTD:
            method = CompressionMethod::Zstd;
            break;
        case parq::CompressionCodec::LZ4_RAW:
        {
            /// LZ4 block.
            if (compressed_size > INT32_MAX || uncompressed_size > INT32_MAX)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Compressed page is too long");
            int n = LZ4_decompress_safe(data, out, int(compressed_size), int(uncompressed_size));
            if (n < 0)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed compressed page");
            if (size_t(n) != uncompressed_size)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected uncompressed page size");
            return;
        }
    }
    if (method == CompressionMethod::None)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected compression codec in parquet: {}", thriftToString(codec));

    auto mem_buf = std::make_unique<ReadBuffer>(const_cast<char *>(data), compressed_size, 0);
    std::unique_ptr<ReadBuffer> decompressor = wrapReadBufferWithCompressionMethod(
        std::move(mem_buf),
        method,
        /*zstd_window_log_max*/ 0,
        uncompressed_size,
        out);
    size_t pos = 0;
    while (pos < uncompressed_size)
    {
        decompressor->set(out + pos, uncompressed_size - pos);
        decompressor->next();
        chassert(decompressor->position() == out + pos);
        size_t n = decompressor->available();
        chassert(n <= uncompressed_size - pos);
        pos += n;
    }
}

}

void Reader::init(const ReadOptions & options_, const Block & sample_block_, std::shared_ptr<const KeyCondition> key_condition_)
{
    options = options_;
    sample_block = &sample_block_;
    key_condition = key_condition_;
}

void Reader::readFileMetaData()
{
    /// Parquet file ends with:
    ///  * serialized FileMetaData struct,
    ///  * [4 bytes] size of serialized FileMetaData struct,
    ///  * "PAR1" magic bytes.

    size_t file_size = prefetcher.getFileSize();
    if (file_size <= 8)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Parquet file too short: {} bytes", file_size);

    /// Read the last 64 KiB in hopes that FileMetaData is smaller than that.
    /// This is usually enough for files smaller than a few hundred MB.
    size_t initial_read_size = std::min(file_size, 64ul << 10);
    PODArray<char> buf(initial_read_size);
    prefetcher.readSync(buf.data(), initial_read_size, file_size - initial_read_size);

    if (memcmp(buf.data() + initial_read_size - 4, "PAR1", 4) != 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Not a parquet file (wrong magic bytes at the end of file)");

    int32_t metadata_size_i32;
    memcpy(&metadata_size_i32, buf.data() + initial_read_size - 8, 4);
    if (metadata_size_i32 <= 0 || size_t(metadata_size_i32) + 8 > file_size)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Bad metadata size in parquet file: {} bytes", metadata_size_i32);

    size_t metadata_size = size_t(metadata_size_i32);
    size_t buf_offset = 0;
    if (metadata_size + 8 > initial_read_size)
    {
        size_t remaining_bytes_to_read = metadata_size + 8 - initial_read_size;
        buf.resize(metadata_size);
        memmove(buf.data() + remaining_bytes_to_read, buf.data(), initial_read_size - 8);
        prefetcher.readSync(buf.data(), remaining_bytes_to_read, file_size - metadata_size - 8);
    }
    else
    {
        buf_offset = initial_read_size - 8 - metadata_size;
    }

    file_metadata = {};
    deserializeThriftStruct(file_metadata, buf.data() + buf_offset, metadata_size);
}

size_t Reader::estimateColumnMemoryUsage(const ColumnChunk & column) const
{
    //TODO: estimate better, the encoded size may be considerably smaller because of fancy encodings
    if (column.pages.empty())
    {
        return column.meta->meta_data.total_uncompressed_size;
    }
    else
    {
        size_t pages_compressed_size = 0;
        for (const auto & page : column.pages)
            pages_compressed_size += page.meta->compressed_page_size;
        return size_t(1. * column.meta->meta_data.total_uncompressed_size / column.meta->meta_data.total_compressed_size * pages_compressed_size);
    }
}

void Reader::prefilterAndInitRowGroups()
{
    if (file_metadata.row_groups.empty())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Parquet file has no row groups");

    SchemaConverter schemer(file_metadata, options, sample_block);
    schemer.checkHasColumns();
    size_t top_level_columns = size_t(file_metadata.schema.at(0).num_children);
    for (size_t i = 0; i < top_level_columns; ++i)
        schemer.processSchemaElement("", /*requested*/ false, /*type_hint*/ nullptr);
    primitive_columns = std::move(schemer.primitive_columns);
    total_primitive_column_count = schemer.primitive_column_idx;
    output_columns = std::move(schemer.output_columns);

    if (primitive_columns.empty())
        throw Exception(ErrorCodes::INCORRECT_DATA, "No columns to read");

    if (key_condition)
    {
        //TODO: assign PrimitiveColumnInfo:: use_bloom and use_column_index; possibly:
        /// Expect that either all or none of the column chunks have indexes written.
        /// (If that's not the case, nothing breaks, we may just pick suboptimal options here.)
    }

    //TODO: fill out use_prewhere in PrimitiveColumnInfo based on SelectQueryInfo;
    //      check that prewhere involves at least one column (otherwise manager won't run it)
    /// Note that we can't disable use_prewhere here, even if there's no performance benefit to it.
    /// If the query pipeline relies on us to do PREWHERE, we must do it, otherwise the unfiltered
    /// results will be shown to the user.

    bool have_row_filtering = std::any_of(primitive_columns.begin(), primitive_columns.end(), [](const auto & c) { return c.use_column_index || c.use_prewhere; });

    for (size_t row_group_idx = 0; row_group_idx < file_metadata.row_groups.size(); ++row_group_idx)
    {
        const auto * meta = &file_metadata.row_groups[row_group_idx];
        if (meta->num_rows <= 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Row group {} has <= 0 rows: {}", row_group_idx, meta->num_rows);
        if (meta->columns.size() != total_primitive_column_count)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Row group {} has unexpected number of columns: {} != {}", row_group_idx, meta->columns.size(), total_primitive_column_count);

        //TODO: filtering
        RowGroup & row_group = row_groups.emplace_back();
        row_group.meta = meta;
        row_group.row_group_idx = row_group_idx;
        row_group.filter.rows_total = meta->num_rows;
        row_group.filter.rows_pass = meta->num_rows;
        row_group.columns.resize(primitive_columns.size());

        for (size_t column_idx = 0; column_idx < primitive_columns.size(); ++column_idx)
        {
            ColumnChunk & column = row_group.columns[column_idx];
            size_t parquet_column_idx = primitive_columns[column_idx].column_idx;
            column.meta = &meta->columns.at(parquet_column_idx);

            if (have_row_filtering &&
                column.meta->__isset.offset_index_offset && column.meta->__isset.offset_index_length &&
                column.meta->offset_index_offset >= 0 && column.meta->offset_index_length > 0)
            {
                column.offset_index_prefetch = prefetcher.registerRange(
                    size_t(column.meta->offset_index_offset),
                    size_t(column.meta->offset_index_length), /*likely_to_be_used*/ true);
            }

            if (primitive_columns[column_idx].use_bloom_filter &&
                column.meta->meta_data.__isset.bloom_filter_offset &&
                column.meta->meta_data.bloom_filter_offset >= 0)
            {
                //TODO: support missing length
                if (column.meta->meta_data.__isset.bloom_filter_length &&
                    column.meta->meta_data.bloom_filter_length > 0)
                {
                    column.bloom_filter_prefetch = prefetcher.registerRange(
                        size_t(column.meta->meta_data.bloom_filter_offset),
                        size_t(column.meta->meta_data.bloom_filter_length),
                        /*likely_to_be_used*/ true);
                }
            }

            if (primitive_columns[column_idx].use_column_index &&
                column.meta->__isset.column_index_offset && column.meta->__isset.column_index_length &&
                column.meta->column_index_offset >= 0 && column.meta->column_index_length > 0)
            {
                column.column_index_prefetch = prefetcher.registerRange(
                    size_t(column.meta->column_index_offset),
                    size_t(column.meta->column_index_length), /*likely_to_be_used*/ true);
            }

            column.data_prefetch = prefetcher.registerRange(
                column.meta->meta_data.__isset.dictionary_page_offset
                    ? column.meta->meta_data.dictionary_page_offset
                    : column.meta->meta_data.data_page_offset,
                column.meta->meta_data.total_compressed_size,
                /*likely_to_be_used*/ true);
        }
    }

    prefetcher.finalizeRanges();
}

SchemaConverter::SchemaConverter(const parq::FileMetaData & file_metadata_, const ReadOptions & options_, const Block * sample_block_) : file_metadata(file_metadata_), options(options_), sample_block(sample_block_) {}

void SchemaConverter::checkHasColumns()
{
    if (file_metadata.schema.size() < 2)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Parquet file has no columns");
    if (file_metadata.schema.at(0).num_children <= 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Schema root has no children");
}

std::optional<size_t> SchemaConverter::processSchemaElement(String name, bool requested, DataTypePtr type_hint)
{
    if (schema_idx >= file_metadata.schema.size())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid parquet schema tree");
    const parq::SchemaElement & element = file_metadata.schema.at(schema_idx);
    schema_idx += 1;

    if (name != "")
        name += ".";
    name += element.name;

    std::optional<size_t> idx_in_output_block;
    if (sample_block)
    {
        std::optional<size_t> pos = sample_block->findPositionByName(name, options.case_insensitive_column_matching);
        if (pos.has_value())
        {
            if (requested)
                throw Exception(ErrorCodes::COLUMN_QUERIED_MORE_THAN_ONCE, "Requested column {} is part of another requested column", name);

            requested = true;
            type_hint = sample_block->getByPosition(pos.value()).type;
            idx_in_output_block = pos;
        }
    }

    if (!element.__isset.num_children)
    {
        /// Primitive column.
        primitive_column_idx += 1;
        if (!requested)
            return std::nullopt;
        if (!element.__isset.type)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Parquet metadata is missing physical type for column {}", element.name);

        size_t primitive_idx = primitive_columns.size();
        PrimitiveColumnInfo & primitive = primitive_columns.emplace_back();
        primitive.column_idx = primitive_column_idx - 1;

        size_t output_idx = output_columns.size();
        OutputColumnInfo & output = output_columns.emplace_back();
        output.primitive_start = primitive_idx;
        output.primitive_end = primitive_idx + 1;
        output.final_type = type_hint;
        output.idx_in_output_block = idx_in_output_block;

        if (element.repetition_type == parq::FieldRepetitionType::OPTIONAL && !type_hint)
            primitive.output_nullable = true;

        const IDataType * primitive_type_hint = type_hint.get();
        if (primitive_type_hint)
        {
            if (primitive_type_hint->lowCardinality())
            {
                primitive.output_low_cardinality = true;
                primitive_type_hint = assert_cast<const DataTypeLowCardinality &>(*primitive_type_hint).getDictionaryType().get();
            }
            if (primitive_type_hint->isNullable())
            {
                primitive.output_nullable = true;
                primitive_type_hint = assert_cast<const DataTypeNullable &>(*primitive_type_hint).getNestedType().get();
            }
        }

        DataTypePtr inferred_type;
        processPrimitiveColumn(element, primitive_type_hint, primitive.decoder, primitive.decoded_type, inferred_type);

        output.decoded_type = primitive.decoded_type;
        if (!type_hint)
        {
            if (element.repetition_type == parq::FieldRepetitionType::OPTIONAL)
                inferred_type = std::make_shared<DataTypeNullable>(inferred_type);
            output.final_type = inferred_type;
        }

        return output_idx;
    }
    else if (element.__isset.repetition_type && element.repetition_type == parq::FieldRepetitionType::REPEATED)
    {
        /// Array or map.
        //TODO
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "arrays not implemented");
    }
    else
    {
        /// Tuple.
        //TODO
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "tuples not implemented");
    }
}

void SchemaConverter::processPrimitiveColumn(
    const parq::SchemaElement & element, const IDataType * /*type_hint*/,
    std::unique_ptr<ValueDecoder> & out_decoder, DataTypePtr & out_decoded_type,
    DataTypePtr & out_inferred_type)
{
    /// Inputs:
    ///  * Parquet Type ("physical type"),
    ///  * Parquet ConvertedType (deprecated, but we have to support it),
    ///  * Parquet LogicalType,
    ///  * ClickHouse type hint (e.g. if the user specified column types explicitly).
    ///
    /// Outputs:
    ///  * out_decoder - how to decode the column (it then separately further dispatches to
    ///    different code paths depending on page encoding and nullability),
    ///  * out_decoded_type - data type of decoding result, chosen for decoding convenience
    ///    (e.g. matching the parquet physical type),
    ///  * out_inferred_type - data type most closely matching the parquet logical type, used for
    ///    schema inference.
    /// After parsing, columns are converted (using castColumn) from out_decoded_type to the final
    /// data type. E.g. maybe out_decoded_type is Int32 based on parquet physical type, but
    /// out_inferred_type is Int16 based on schema inference, and castColumn does the conversion.

    parq::Type::type type = element.type;
    parq::ConvertedType::type converted =
        element.__isset.converted_type ? element.converted_type : parq::ConvertedType::type(-1);
    const parq::LogicalType & logical = element.logicalType;
    using CONV = parq::ConvertedType;
    chassert(!out_inferred_type);

    if (logical.__isset.STRING || logical.__isset.JSON || logical.__isset.BSON ||
        logical.__isset.ENUM || converted == CONV::UTF8 || converted == CONV::JSON ||
        converted == CONV::BSON || converted == CONV::ENUM)
    {
        if (type != parq::Type::BYTE_ARRAY && type != parq::Type::FIXED_LEN_BYTE_ARRAY)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected non-string physical type for string logical type: {}", thriftToString(element));
        /// Fall through to dispatch by physical type only.
    }
    else if (logical.__isset.INTEGER || (converted >= CONV::UINT_8 && converted <= CONV::INT_64))
    {
        const parq::IntType & integer = logical.INTEGER;
        bool is_signed = integer.isSigned;
        size_t bits = integer.bitWidth;
        if (!logical.__isset.INTEGER)
        {
            switch (converted)
            {
                case CONV::UINT_8: is_signed = false; bits = 8; break;
                case CONV::UINT_16: is_signed = false; bits = 16; break;
                case CONV::UINT_32: is_signed = false; bits = 32; break;
                case CONV::UINT_64: is_signed = false; bits = 64; break;
                case CONV::INT_8: is_signed = true; bits = 8; break;
                case CONV::INT_16: is_signed = true; bits = 16; break;
                case CONV::INT_32: is_signed = true; bits = 32; break;
                case CONV::INT_64: is_signed = true; bits = 64; break;
                default:
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected integer logical type: {}", thriftToString(element));
            }
        }
        if (!is_signed && bits == 8)
            out_inferred_type = std::make_shared<DataTypeUInt8>();
        else if (!is_signed && bits == 16)
            out_inferred_type = std::make_shared<DataTypeUInt16>();
        else if (!is_signed && bits == 32)
            out_inferred_type = std::make_shared<DataTypeUInt32>();
        else if (!is_signed && bits == 64)
            out_inferred_type = std::make_shared<DataTypeUInt64>();
        else if (is_signed && bits == 8)
            out_inferred_type = std::make_shared<DataTypeInt8>();
        else if (is_signed && bits == 16)
            out_inferred_type = std::make_shared<DataTypeInt16>();
        else if (is_signed && bits == 32)
            out_inferred_type = std::make_shared<DataTypeInt32>();
        else if (is_signed && bits == 64)
            out_inferred_type = std::make_shared<DataTypeInt64>();
        else
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected integer logical type: {}", thriftToString(element));

        /// Can't leave the signed->unsigned conversion to castColumn.
        /// E.g. if parquet type is UINT64, and the requested clickhouse type is Int128,
        /// casting Int64 -> UInt64 -> Int128 produces different result from Int64 -> Int128.
        if (type == parq::Type::INT32)
        {
            out_decoded_type = out_inferred_type;
            if (bits == 8)
                out_decoder = std::make_unique<ShortIntDecoder<UInt8>>();
            else if (bits == 16)
                out_decoder = std::make_unique<ShortIntDecoder<UInt16>>();
            else
                out_decoder = std::make_unique<FixedSizeValueDecoder>(4);
        }
        else if (type == parq::Type::INT64)
        {
            out_decoder = std::make_unique<FixedSizeValueDecoder>(8);
            out_decoded_type = is_signed
                ? std::static_pointer_cast<IDataType>(std::make_shared<DataTypeInt64>())
                : std::static_pointer_cast<IDataType>(std::make_shared<DataTypeUInt64>());
        }
        else
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected non-int physical type for int logical type: {}", thriftToString(element));
        }

        return;
    }
    else if (logical.__isset.TIME || converted == CONV::TIME_MILLIS || converted == CONV::TIME_MICROS)
    {
        /// ClickHouse doesn't have data types for time of day.
        /// Fall through to dispatch by physical type only (as plain integer).
    }
    else if (logical.__isset.TIMESTAMP || converted == CONV::TIMESTAMP_MILLIS || converted == CONV::TIMESTAMP_MICROS)
    {
        UInt32 scale;
        if (logical.TIMESTAMP.unit.__isset.MILLIS || converted == CONV::TIMESTAMP_MILLIS)
            scale = 3;
        else if (logical.TIMESTAMP.unit.__isset.MICROS || converted == CONV::TIMESTAMP_MICROS)
            scale = 6;
        else if (logical.TIMESTAMP.unit.__isset.NANOS)
            scale = 9;
        else
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected timestamp units: {}", thriftToString(element));

        if (type != parq::Type::INT64)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected physical type for timestamp logical type: {}", thriftToString(element));

        /// Can't leave int -> DateTime64 conversion to castColumn as it interprets the integer as seconds.
        out_decoder = std::make_unique<FixedSizeValueDecoder>(8);
        out_inferred_type = std::make_shared<DataTypeDateTime64>(scale);
        out_decoded_type = out_inferred_type;

        return;
    }
    else if (logical.__isset.DATE || converted == CONV::DATE)
    {
        if (type != parq::Type::INT32)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected physical type for date logical type: {}", thriftToString(element));

        out_decoder = std::make_unique<FixedSizeValueDecoder>(4);
        out_inferred_type = std::make_shared<DataTypeDate32>();
        out_decoded_type = out_inferred_type;

        return;
    }
    else if (logical.__isset.DECIMAL || converted == CONV::DECIMAL)
    {
        //TODO
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "decimal not implemented yet");
        // if physical is INT32:
        //     require that precision <= 9
        //     parse as Decimal32(scale) (memcpy)
        // else if physical is INT64:
        //     require that precision <= 18
        //     parse as Decimal64(scale) (memcpy)
        // else if physical is FIXED_LEN_BYTE_ARRAY:
        //     pick type:
        //         precision <= 9, length <= 4: Decimal32
        //         precision <= 18, length <= 8: Decimal64
        //         precision <= 38, length <= 16: Decimal128
        //         precision <= 76, length <= 32: Decimal256
        //         else: error
        //     parse (not memcpy, parquet data is big-endian, reverse bytes after reading; can do it in place though)
        // return
    }
    else if (logical.__isset.MAP || logical.__isset.LIST || converted == CONV::MAP ||
             converted == CONV::MAP_KEY_VALUE || converted == CONV::LIST)
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected compound logical type for leaf column: {}", thriftToString(element));
    }
    else if (logical.__isset.UNKNOWN)
    {
        //TODO: DataTypeNothing (for now fall through to dispatch by physical type)
    }
    else if (logical.__isset.UUID)
    {
        if (type != parq::Type::FIXED_LEN_BYTE_ARRAY || element.type_length != 16)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected physical type for UUID column: {}", thriftToString(element));
        //TODO: check if byte order is correct
        out_decoder = std::make_unique<FixedSizeValueDecoder>(16);
        out_inferred_type = std::make_shared<DataTypeUUID>();
        out_decoded_type = out_inferred_type;
    }
    else if (logical.__isset.FLOAT16)
    {
        /// TODO: Support. For now, fall through to reading as FixedString(2).
    }
    else if (converted == CONV::INTERVAL)
    {
        /// TODO: Support. For now, fall through to reading as FixedString(12).
    }
    else if (element.__isset.logicalType || element.__isset.converted_type)
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected logical/converted type: {}", thriftToString(element));
    }

    // If we didn't `return` above, dispatch by physical type.
    switch (type)
    {
        case parq::Type::BOOLEAN:
            //TODO
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "BOOLEAN not implemented");
            //return;
        case parq::Type::INT32:
            out_decoder = std::make_unique<FixedSizeValueDecoder>(4);
            out_inferred_type = std::make_shared<DataTypeInt32>();
            out_decoded_type = out_inferred_type;
            return;
        case parq::Type::INT64:
            out_decoder = std::make_unique<FixedSizeValueDecoder>(8);
            out_inferred_type = std::make_shared<DataTypeInt64>();
            out_decoded_type = out_inferred_type;
            return;
        case parq::Type::INT96:
            //TODO
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "INT96 not implemented");
            //return;
        case parq::Type::FLOAT:
            out_decoder = std::make_unique<FixedSizeValueDecoder>(4);
            out_inferred_type = std::make_shared<DataTypeFloat32>();
            out_decoded_type = out_inferred_type;
            return;
        case parq::Type::DOUBLE:
            out_decoder = std::make_unique<FixedSizeValueDecoder>(8);
            out_inferred_type = std::make_shared<DataTypeFloat64>();
            out_decoded_type = out_inferred_type;
            return;
        case parq::Type::BYTE_ARRAY:
            out_decoder = std::make_unique<StringDecoder>();
            out_inferred_type = std::make_shared<DataTypeString>();
            out_decoded_type = out_inferred_type;
            return;
        case parq::Type::FIXED_LEN_BYTE_ARRAY:
            out_decoder = std::make_unique<FixedSizeValueDecoder>(size_t(element.type_length));
            out_inferred_type = std::make_shared<DataTypeFixedString>(size_t(element.type_length));
            out_decoded_type = out_inferred_type;
            return;
    }

    throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected physical type: {}", thriftToString(element));
}

void Reader::parsePrimitiveColumn(ColumnChunk & column_chunk, const PrimitiveColumnInfo & column_info, const RowSet & filter)
{
    //TODO: handle selective page reading

    MutableColumnPtr dictionary_column;
    MutableColumnPtr index_column;
    MutableColumnPtr full_column;
    std::unique_ptr<ValueDecoder> index_decoder;

    /// TODO: Instead of using ColumnLowCardinality for dictionary-encoded data, at least for strings
    ///       add a custom dictionary type to avoid a memcpy: instead of copying strings from
    ///       plain-encoded dictionary page into ColumnString, make an array of ranges pointing into
    ///       the decompressed page buffer.
    auto make_low_cardinality_column = [&]
    {
        chassert(!full_column);
        auto uniq = DataTypeLowCardinality::createColumnUnique(*column_info.decoded_type, std::move(dictionary_column));
        auto lc = ColumnLowCardinality::create(std::move(uniq), std::move(index_column), /*is_shared*/ false);
        dictionary_column.reset();
        index_column.reset();
        return lc;
    };

    size_t output_num_values_estimate = column_chunk.meta->meta_data.num_values;
    if (filter.rows_pass < filter.rows_total)
    {
        if (column_info.max_rep == 0)
            output_num_values_estimate = filter.rows_pass;
        else
            /// There are arrays, so we can't know exactly how many primitive values there are in
            /// rows that pass the filter. Make a guess using average array length.
            output_num_values_estimate = size_t(1.2 * filter.rows_pass / filter.rows_total * output_num_values_estimate);
    }

    PaddedPODArray<char> decompressed_buffer;
    PaddedPODArray<UInt8> rep;
    PaddedPODArray<UInt8> def;

    const auto data = prefetcher.getRangeData(column_chunk.data_prefetch);
    size_t row_idx = size_t(-1); // we're just before the first row
    size_t pos = 0;
    while (pos < data.size())
    {
        decompressed_buffer.clear();
        parq::PageHeader header;
        pos += deserializeThriftStruct(header, data.data() + pos, data.size() - pos);
        size_t compressed_page_size = size_t(header.compressed_page_size);
        if (pos + compressed_page_size > data.size() || header.compressed_page_size < 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Page size out of bounds: offset {} + size {} > column chunk size {}", pos, compressed_page_size, data.size());

        const char * compressed_page_data = data.data() + pos;
        pos += compressed_page_size;

        if (header.type == parq::PageType::INDEX_PAGE)
            continue;

        //TODO: skip page if all filtered out (remember to update row_idx)
        //TODO: apply filter

        /// Pages of all 3 types boil down to encoded values + optional rep/def levels.
        /// Extract that information.

        parq::CompressionCodec::type codec = column_chunk.meta->meta_data.codec;
        const char * encoded_values = compressed_page_data; // compressed with `codec`
        size_t encoded_values_compressed_size = compressed_page_size;
        size_t encoded_values_uncompressed_size = header.uncompressed_page_size;
        const char * encoded_rep = nullptr; // uncompressed
        const char * encoded_def = nullptr; // uncompressed
        size_t def_count = 0; // aka num_values
        size_t encoded_rep_size = 0;
        size_t encoded_def_size = 0;
        parq::Encoding::type values_encoding = parq::Encoding::PLAIN;
        parq::Encoding::type def_encoding = parq::Encoding::RLE;
        parq::Encoding::type rep_encoding = parq::Encoding::RLE;
        bool may_have_null_values = column_info.max_def > 0;

        if (header.type == parq::PageType::DATA_PAGE)
        {
            def_count = header.data_page_header.num_values;
            values_encoding = header.data_page_header.encoding;
            def_encoding = header.data_page_header.definition_level_encoding;
            rep_encoding = header.data_page_header.repetition_level_encoding;

            /// What exactly does "null values" means in context of rep/def levels,
            /// especially if a row straddles page boundary?
            /// Presumably it's just def < max_def (i.e. null or empty array).
            if (header.data_page_header.__isset.statistics &&
                header.data_page_header.statistics.__isset.null_count &&
                header.data_page_header.statistics.null_count == 0)
            {
                may_have_null_values = false;
            }

            if (column_info.max_def == 0)
            {
                /// No rep/def levels, the whole page is values.
            }
            else
            {
                /// Rep/def levels and values are compressed together. Decompress and split.
                /// Format (lengths are 4 bytes):
                /// <def length> <def> [<rep length> <rep>] <values>
                const char * ptr;
                size_t size = size_t(header.uncompressed_page_size);
                if (codec == parq::CompressionCodec::UNCOMPRESSED)
                {
                    if (size != compressed_page_size)
                        throw Exception(ErrorCodes::INCORRECT_DATA, "No compression, but compressed and uncompressed page size are different");
                    ptr = compressed_page_data;
                }
                else
                {
                    decompressed_buffer.resize(size);
                    decompress(compressed_page_data, compressed_page_size, size, codec, decompressed_buffer.data());
                    ptr = decompressed_buffer.data();
                    codec = parq::CompressionCodec::UNCOMPRESSED;
                }

                if (size < 4)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Decompressed data is too short (def size)");
                UInt32 n;
                memcpy(&n, ptr, 4);
                if (n > size - 4)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Decompressed data is too short (def)");
                encoded_def = ptr + 4;
                encoded_def_size = n;
                ptr += 4 + encoded_def_size;
                size -= 4 + encoded_def_size;

                if (column_info.max_rep > 0)
                {
                    if (size < 4)
                        throw Exception(ErrorCodes::INCORRECT_DATA, "Decompressed data is too short (rep size)");
                    memcpy(&n, ptr, 4);
                    if (n > size - 4)
                        throw Exception(ErrorCodes::INCORRECT_DATA, "Decompressed data is too short (rep)");
                    encoded_rep = ptr + 4;
                    encoded_rep_size = n;
                    ptr += 4 + encoded_rep_size;
                    size -= 4 + encoded_rep_size;
                }

                encoded_values = ptr;
                encoded_values_compressed_size = size;
                encoded_values_uncompressed_size = size;
            }
        }
        else if (header.type == parq::PageType::DATA_PAGE_V2)
        {
            def_count = header.data_page_header_v2.num_values;
            values_encoding = header.data_page_header_v2.encoding;
            if (header.data_page_header_v2.num_nulls == 0)
                may_have_null_values = false;
            encoded_def_size = header.data_page_header_v2.definition_levels_byte_length;
            encoded_rep_size = header.data_page_header_v2.repetition_levels_byte_length;

            if (header.data_page_header_v2.__isset.is_compressed &&
                !header.data_page_header_v2.is_compressed)
            {
                codec = parq::CompressionCodec::UNCOMPRESSED;
            }

            if (encoded_def_size + encoded_rep_size > compressed_page_size)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Page data is too short (def+rep)");
            encoded_def = compressed_page_data;
            encoded_rep = compressed_page_data + encoded_def_size;
            size_t uncompressed_part = encoded_def_size + encoded_rep_size;
            encoded_values = compressed_page_data + uncompressed_part;
            encoded_values_compressed_size = compressed_page_size - uncompressed_part;
            encoded_values_uncompressed_size = size_t(header.uncompressed_page_size) - uncompressed_part;
        }
        else if (header.type == parq::PageType::DICTIONARY_PAGE)
        {
            def_count = header.dictionary_page_header.num_values;
            values_encoding = header.dictionary_page_header.encoding;
            may_have_null_values = false;
        }
        else
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected page type: {}", thriftToString(header));
        }

        if (values_encoding == parq::Encoding::PLAIN_DICTIONARY)
            values_encoding = header.type == parq::PageType::DICTIONARY_PAGE
                ? parq::Encoding::PLAIN : parq::Encoding::RLE_DICTIONARY;

        /// Determine what we're decoding: dictionary, indices into dictionary, or full column.
        IColumn * destination = nullptr;
        const ValueDecoder * decoder = column_info.decoder.get();
        if (header.type == parq::PageType::DICTIONARY_PAGE)
        {
            if (dictionary_column || full_column)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected extra dictionary page");
            dictionary_column = column_info.decoded_type->createColumn();
            dictionary_column->reserve(def_count + 2);
            dictionary_column->insertManyDefaults(2);
            destination = dictionary_column.get();
        }
        else if (values_encoding == parq::Encoding::RLE_DICTIONARY)
        {
            if (!dictionary_column)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected dictionary-encoded page without dictionary");
            if (full_column)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected dictionary-encoded page after non-dictionary-encoded pages");

            if (!index_column)
            {
                size_t dict_size = dictionary_column->size();
                chassert(dict_size >= 2);
                if (dict_size <= (1 << 8))
                {
                    index_column = ColumnUInt8::create();
                    index_decoder = std::make_unique<DictionaryIndexDecoder<UInt8>>(dict_size - 2);
                }
                else if (dict_size <= (1 << 16))
                {
                    index_column = ColumnUInt16::create();
                    index_decoder = std::make_unique<DictionaryIndexDecoder<UInt16>>(dict_size - 2);
                }
                else if (dict_size <= (1ul << 32))
                {
                    index_column = ColumnUInt32::create();
                    index_decoder = std::make_unique<DictionaryIndexDecoder<UInt32>>(dict_size - 2);
                }
                else
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpectedly large dictionary");

                index_column->reserve(output_num_values_estimate);
            }

            destination = index_column.get();
            decoder = index_decoder.get();
        }
        else
        {
            if (index_column)
            {
                /// Switching from dictionary to non-dictionary encoding.
                /// Convert the dictionary-encoded data to full column.
                //TODO: test this code path
                auto lc = make_low_cardinality_column();
                full_column = IColumn::mutate(lc->convertToFullColumn());

                full_column->reserve(output_num_values_estimate);

                dictionary_column.reset();
                index_column.reset();
            }

            if (!full_column)
            {
                full_column = column_info.decoded_type->createColumn();
                full_column->reserve(output_num_values_estimate);
            }

            destination = full_column.get();
        }

        /// Finally decode the values.

        //TODO: deal with rep/def levels
        bool need_expand = false;
        const UInt8 * page_value_filter = nullptr;
        size_t num_encoded_values = def_count;
        size_t num_filtered_values = def_count;
        (void)encoded_rep;
        (void)encoded_def;
        (void)def_encoding;
        (void)rep_encoding;
        (void)row_idx;
        if (column_info.max_rep > 0)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "arrays not implemented");
        if (may_have_null_values)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "nulls not implemented");

        char * dest_ptr;
        size_t dest_size;
        if (!page_value_filter && decoder->canReadDirectlyIntoColumn(values_encoding, num_encoded_values, *destination, &dest_ptr, &dest_size))
        {
            if (dest_size > encoded_values_uncompressed_size)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Page data is too short (values)");
            if (codec == parq::CompressionCodec::UNCOMPRESSED)
                memcpy(dest_ptr, encoded_values, dest_size);
            else
                /// Decompress directly into IColumn, avoid memcpy, yay.
                decompress(encoded_values, encoded_values_compressed_size, encoded_values_uncompressed_size, codec, dest_ptr);
        }
        else
        {
            if (codec != parq::CompressionCodec::UNCOMPRESSED)
            {
                chassert(decompressed_buffer.empty());
                decompressed_buffer.resize(encoded_values_uncompressed_size);
                decompress(encoded_values, encoded_values_compressed_size, encoded_values_uncompressed_size, codec, decompressed_buffer.data());
                encoded_values = decompressed_buffer.data();
            }

            decoder->decodePage(values_encoding, encoded_values, encoded_values_uncompressed_size, num_encoded_values, num_filtered_values, *destination, page_value_filter);
        }

        if (need_expand)
        {
            //TODO
        }
    }

    if (index_column)
    {
        auto lc = make_low_cardinality_column();
        //TODO: assign zip_bombness; use SizeStatistics.unencoded_byte_array_data_bytes when available

        if (column_chunk.zip_bombness == 1)
            column_chunk.column = lc->convertToFullColumn();
        else
            column_chunk.column = std::move(lc);
    }
    else if (full_column)
    {
        column_chunk.column = std::move(full_column);
    }
    else
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Column chunk has no data pages");
    }
}

size_t Reader::decideNumRowsPerChunk(RowGroup & row_group)
{
    size_t num_chunks = 1;
    for (auto & c : row_group.columns)
        num_chunks = std::max(num_chunks, c.zip_bombness);
    return (row_group.filter.rows_pass + num_chunks - 1) / num_chunks;
}

ColumnPtr Reader::formOutputColumn(RowGroup & row_group, size_t output_column_idx, size_t start_row, size_t num_rows)
{
    //TODO: arrays etc

    const OutputColumnInfo & output_info = output_columns[output_column_idx];
    chassert(output_info.primitive_start + 1 == output_info.primitive_end);
    size_t primitive_idx = output_info.primitive_start;
    const PrimitiveColumnInfo & primitive_info = primitive_columns.at(primitive_idx);

    ColumnPtr col = row_group.columns.at(primitive_idx).column;
    ColumnPtr res;

    if (!primitive_info.output_low_cardinality)
    {
        const auto * lc = typeid_cast<const ColumnLowCardinality *>(col.get());
        if (lc)
        {
            ColumnPtr indexes = lc->getIndexesPtr();
            auto indexes_range = indexes->cloneEmpty();
            indexes_range->insertRangeFrom(*indexes, start_row, num_rows);
            res = lc->getDictionary().getNestedColumn()->index(*indexes_range, 0);
        }
    }

    if (!res)
    {
        if (start_row == 0 && num_rows == col->size())
        {
            res = col;
        }
        else
        {
            auto range = col->cloneEmpty();
            range->insertRangeFrom(*col, start_row, num_rows);
            res = std::move(range);
        }
    }

    /// (Comparing pointers is ok here.)
    if (output_info.idx_in_output_block.has_value() && output_info.decoded_type != output_info.final_type)
    {
        //TODO: Also match this behavior from ArrowColumnToCHColumn:
        // "Cast key column to target type, because it can happen
        //  that parsed type cannot be ClickHouse Map key type."

        /// TODO: Try InternalCastFunctionCache.
        const String & name = sample_block->getByPosition(output_info.idx_in_output_block.value()).name;
        res = castColumn({res, output_info.decoded_type, name}, output_info.final_type);
    }

    return res;
}

}
