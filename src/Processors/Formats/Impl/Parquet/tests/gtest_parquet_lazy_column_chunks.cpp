#include <gtest/gtest.h>
#include <config.h>
#if USE_PARQUET

#include <Processors/Formats/Impl/Parquet/Reader.h>
#include <Processors/Formats/Impl/Parquet/ThriftUtil.h>
#include <Processors/Formats/Impl/ParquetMetadataCache.h>

#include <Common/Exception.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromString.h>

#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <optional>
#include <numeric>
#include <random>
#include <thread>

using namespace DB;
using namespace DB::Parquet;

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
}

/// Footers parsed without column chunks (`deserializeFileMetaDataWithoutColumnChunks`) plus the
/// column chunks parsed on demand (`deserializeColumnChunks`) must give what the generated
/// `FileMetaData::read` gives, including on footers that serializeThriftStruct never writes.
namespace
{

using apache::thrift::protocol::TType;
using MemoryBuffer = apache::thrift::transport::TMemoryBuffer;
using Protocol = apache::thrift::protocol::TCompactProtocolT<MemoryBuffer>;
using WriteFn = std::function<void(Protocol &)>;

parq::KeyValue keyValue(const std::string & key, const std::string & value)
{
    parq::KeyValue kv;
    kv.__set_key(key);
    kv.__set_value(value);
    return kv;
}

parq::ColumnChunk makeColumnChunk(size_t row_group, size_t column)
{
    const auto base = int64_t(1000 * row_group + 100 * column);

    parq::Statistics stats;
    stats.__set_min_value(std::string(200, char('a' + column)));
    stats.__set_max_value(std::string(300, char('b' + column)));
    stats.__set_null_count(int64_t(column));
    stats.__set_distinct_count(7);
    stats.__set_is_max_value_exact(true);

    parq::PageEncodingStats page_stats;
    page_stats.__set_page_type(parq::PageType::DATA_PAGE);
    page_stats.__set_encoding(parq::Encoding::PLAIN);
    page_stats.__set_count(3);

    parq::SizeStatistics size_stats;
    size_stats.__set_unencoded_byte_array_data_bytes(base + 9);
    size_stats.__set_repetition_level_histogram({1, 2});
    size_stats.__set_definition_level_histogram({3, 4, 5});

    parq::BoundingBox bbox;
    bbox.__set_xmin(1);
    bbox.__set_xmax(2);
    bbox.__set_ymin(3);
    bbox.__set_ymax(4);
    parq::GeospatialStatistics geo;
    geo.__set_bbox(bbox);
    geo.__set_geospatial_types({1, 3});

    parq::ColumnMetaData meta;
    meta.__set_type(parq::Type::BYTE_ARRAY);
    meta.__set_encodings({parq::Encoding::PLAIN, parq::Encoding::RLE_DICTIONARY});
    meta.__set_path_in_schema({"c" + std::to_string(column)});
    meta.__set_codec(parq::CompressionCodec::ZSTD);
    meta.__set_num_values(base + 1);
    meta.__set_total_uncompressed_size(base + 2);
    meta.__set_total_compressed_size(base + 3);
    std::string kv_value = "v" + std::to_string(base);
    kv_value.resize(30, '.');
    meta.__set_key_value_metadata({keyValue("k", kv_value), keyValue("k2", std::string(30, 'w'))});
    meta.__set_data_page_offset(base + 4);
    meta.__set_dictionary_page_offset(base + 5);
    meta.__set_statistics(stats);
    meta.__set_encoding_stats({page_stats, page_stats});
    meta.__set_bloom_filter_offset(base + 7);
    meta.__set_bloom_filter_length(8);
    meta.__set_size_statistics(size_stats);
    meta.__set_geospatial_statistics(geo);

    parq::EncryptionWithColumnKey column_key;
    column_key.__set_path_in_schema({std::string(40, 'p'), std::string(41, 'q')});
    column_key.__set_key_metadata(std::string(50, 'k'));
    parq::ColumnCryptoMetaData crypto;
    crypto.__set_ENCRYPTION_WITH_COLUMN_KEY(column_key);

    parq::ColumnChunk chunk;
    chunk.__set_file_path("part-" + std::to_string(row_group) + ".parquet");
    chunk.__set_file_offset(base);
    chunk.__set_meta_data(meta);
    chunk.__set_offset_index_offset(base + 10);
    chunk.__set_offset_index_length(11);
    chunk.__set_column_index_offset(base + 12);
    chunk.__set_column_index_length(13);
    chunk.__set_crypto_metadata(crypto);
    chunk.__set_encrypted_column_metadata("encrypted");
    return chunk;
}

std::vector<parq::SchemaElement> makeSchema(size_t num_columns)
{
    std::vector<parq::SchemaElement> schema(num_columns + 1);
    schema[0].__set_name("schema");
    schema[0].__set_num_children(int32_t(num_columns));
    for (size_t i = 0; i < num_columns; ++i)
    {
        schema[i + 1].__set_name("c" + std::to_string(i));
        schema[i + 1].__set_type(parq::Type::BYTE_ARRAY);
        schema[i + 1].__set_repetition_type(parq::FieldRepetitionType::OPTIONAL);
    }
    return schema;
}

parq::FileMetaData makeFileMetaData(size_t num_row_groups = 3, size_t num_columns = 5)
{
    parq::FileMetaData metadata;
    metadata.__set_version(2);
    metadata.__set_schema(makeSchema(num_columns));
    metadata.__set_num_rows(int64_t(10 * num_row_groups));

    std::vector<parq::RowGroup> row_groups(num_row_groups);
    for (size_t i = 0; i < num_row_groups; ++i)
    {
        std::vector<parq::ColumnChunk> columns;
        for (size_t j = 0; j < num_columns; ++j)
            columns.push_back(makeColumnChunk(i, j));
        parq::SortingColumn sorting_column;
        sorting_column.__set_column_idx(1);
        sorting_column.__set_descending(true);
        sorting_column.__set_nulls_first(false);

        row_groups[i].__set_columns(columns);
        row_groups[i].__set_total_byte_size(int64_t(100 + i));
        row_groups[i].__set_num_rows(10);
        row_groups[i].__set_sorting_columns({sorting_column});
        row_groups[i].__set_file_offset(int64_t(4 + i));
        row_groups[i].__set_total_compressed_size(int64_t(50 + i));
        row_groups[i].__set_ordinal(int16_t(i));
    }
    metadata.__set_row_groups(row_groups);

    metadata.__set_key_value_metadata({keyValue("ARROW:schema", std::string(100, 'x')), keyValue("geo", "{}")});
    metadata.__set_created_by("parquet-cpp-arrow version 23.0.0");
    parq::ColumnOrder order;
    order.__set_TYPE_ORDER(parq::TypeDefinedOrder());
    metadata.__set_column_orders(std::vector<parq::ColumnOrder>(num_columns, order));
    parq::AesGcmV1 aes;
    aes.__set_aad_prefix(std::string(40, 'a'));
    aes.__set_aad_file_unique(std::string(41, 'u'));
    aes.__set_supply_aad_prefix(true);
    parq::EncryptionAlgorithm algorithm;
    algorithm.__set_AES_GCM_V1(aes);
    metadata.__set_encryption_algorithm(algorithm);
    metadata.__set_footer_signing_key_metadata("signing");
    return metadata;
}

template <typename T>
std::string serialize(const T & obj)
{
    WriteBufferFromOwnString out;
    serializeThriftStruct(obj, out);
    return out.str();
}

parq::FileMetaData parseFull(const std::string & bytes)
{
    parq::FileMetaData res;
    deserializeThriftStruct(res, bytes.data(), bytes.size());
    return res;
}

std::shared_ptr<FileFooter> makeFooter(const parq::FileMetaData & metadata)
{
    auto footer = std::make_shared<FileFooter>();
    footer->serialized = serialize(metadata);
    deserializeFileMetaDataWithoutColumnChunks(
        footer->metadata, footer->serialized.data(), footer->serialized.size(), footer->column_chunk_offsets);
    return footer;
}

/// By Parquet column index, like Reader::getColumnChunks.
std::vector<FileColumnChunksPtr> parseColumns(const FileFooter & footer, const std::vector<size_t> & column_indices)
{
    std::vector<FileColumnChunks> parsed;
    deserializeColumnChunks(footer.serialized.data(), footer.serialized.size(), footer.column_chunk_offsets, column_indices, parsed);
    std::vector<FileColumnChunksPtr> res(column_indices.back() + 1);
    for (size_t k = 0; k < column_indices.size(); ++k)
        res[column_indices[k]] = std::make_shared<const FileColumnChunks>(std::move(parsed[k]));
    return res;
}

/// Independent of FileColumnChunks::memory_bytes and ParquetMetadataCacheCell::memory_bytes.
size_t expectedColumnMemory(const FileColumnChunks & column)
{
    size_t res = column.chunks.capacity() * sizeof(parq::ColumnChunk);
    for (const auto & chunk : column.chunks)
    {
        const auto & meta = chunk.meta_data;
        res += chunk.file_path.capacity() + chunk.encrypted_column_metadata.capacity();
        res += meta.path_in_schema.capacity() * sizeof(std::string);
        for (const auto & path : meta.path_in_schema)
            res += path.capacity();
        res += meta.key_value_metadata.capacity() * sizeof(parq::KeyValue);
        for (const auto & kv : meta.key_value_metadata)
            res += kv.key.capacity() + kv.value.capacity();
        const auto & column_key = chunk.crypto_metadata.ENCRYPTION_WITH_COLUMN_KEY;
        res += column_key.path_in_schema.capacity() * sizeof(std::string);
        for (const auto & path : column_key.path_in_schema)
            res += path.capacity();
        res += column_key.key_metadata.capacity();
        res += meta.encodings.capacity() * sizeof(parq::Encoding::type) + meta.encoding_stats.capacity() * sizeof(parq::PageEncodingStats)
            + meta.geospatial_statistics.geospatial_types.capacity() * sizeof(int32_t);
        res += meta.statistics.min_value.capacity() + meta.statistics.max_value.capacity() + meta.statistics.min.capacity()
            + meta.statistics.max.capacity();
        res += (meta.size_statistics.repetition_level_histogram.capacity() + meta.size_statistics.definition_level_histogram.capacity())
            * sizeof(int64_t);
    }
    return res;
}

size_t expectedCellWeight(const ParquetMetadataCacheCell & cell)
{
    const FileFooter & footer = *cell.footer;
    size_t res = sizeof(FileFooter) + footer.serialized.capacity() + footer.column_chunk_offsets.capacity() * sizeof(std::vector<UInt32>);
    for (const auto & offsets : footer.column_chunk_offsets)
        res += offsets.capacity() * sizeof(UInt32);
    const auto & metadata = footer.metadata;
    res += metadata.schema.capacity() * sizeof(parq::SchemaElement);
    for (const auto & element : metadata.schema)
        res += element.name.capacity() + element.logicalType.GEOMETRY.crs.capacity() + element.logicalType.GEOGRAPHY.crs.capacity();
    res += metadata.row_groups.capacity() * sizeof(parq::RowGroup);
    for (const auto & row_group : metadata.row_groups)
        res += row_group.sorting_columns.capacity() * sizeof(parq::SortingColumn);
    res += metadata.key_value_metadata.capacity() * sizeof(parq::KeyValue);
    for (const auto & kv : metadata.key_value_metadata)
        res += kv.key.capacity() + kv.value.capacity();
    res += metadata.column_orders.capacity() * sizeof(parq::ColumnOrder);
    res += metadata.created_by.capacity() + metadata.footer_signing_key_metadata.capacity();
    const auto & encryption = metadata.encryption_algorithm;
    res += encryption.AES_GCM_V1.aad_prefix.capacity() + encryption.AES_GCM_V1.aad_file_unique.capacity();
    res += encryption.AES_GCM_CTR_V1.aad_prefix.capacity() + encryption.AES_GCM_CTR_V1.aad_file_unique.capacity();
    res += cell.column_chunks.capacity() * sizeof(FileColumnChunksPtr);
    for (const auto & column : cell.column_chunks)
        if (column)
            res += sizeof(FileColumnChunks) + expectedColumnMemory(*column);
    return res + 200; /// ParquetMetadataCacheCell::SIZE_IN_MEMORY_OVERHEAD
}

struct Skeleton
{
    parq::FileMetaData metadata;
    std::vector<std::vector<UInt32>> offsets;
};

Skeleton parseSkeleton(const std::string & bytes)
{
    Skeleton res;
    deserializeFileMetaDataWithoutColumnChunks(res.metadata, bytes.data(), bytes.size(), res.offsets);
    return res;
}

/// The skeleton with all column chunks parsed and put back.
parq::FileMetaData parseLazy(const std::string & bytes)
{
    Skeleton skeleton = parseSkeleton(bytes);
    size_t max_columns = 0;
    for (const auto & offsets : skeleton.offsets)
        max_columns = std::max(max_columns, offsets.empty() ? 0 : offsets.size() - 1);
    std::vector<size_t> all_columns(max_columns);
    std::iota(all_columns.begin(), all_columns.end(), 0);

    std::vector<FileColumnChunks> chunks;
    deserializeColumnChunks(bytes.data(), bytes.size(), skeleton.offsets, all_columns, chunks);
    for (size_t i = 0; i < skeleton.offsets.size(); ++i)
    {
        auto & columns = skeleton.metadata.row_groups.at(i).columns;
        for (size_t j = 0; j + 1 < skeleton.offsets[i].size(); ++j)
            columns.push_back(chunks.at(j).chunks.at(i));
    }
    return skeleton.metadata;
}

template <typename F>
int errorCode(F && f)
{
    try
    {
        f();
        return 0;
    }
    catch (const Exception & e)
    {
        return e.code();
    }
}

/// Hand-written footers: each row group writes its `columns` field once per list in `column_lists`.
WriteFn rowGroup(
    std::vector<std::vector<WriteFn>> column_lists,
    TType columns_element_type = TType::T_STRUCT,
    std::optional<TType> sorting_columns_element_type = std::nullopt)
{
    return [=](Protocol & w)
    {
        w.writeStructBegin("RowGroup");
        for (const auto & columns : column_lists)
        {
            w.writeFieldBegin("columns", TType::T_LIST, 1);
            w.writeListBegin(columns_element_type, uint32_t(columns.size()));
            for (const auto & write_column : columns)
                write_column(w);
            w.writeListEnd();
            w.writeFieldEnd();
        }
        w.writeFieldBegin("total_byte_size", TType::T_I64, 2);
        w.writeI64(100);
        w.writeFieldEnd();
        w.writeFieldBegin("num_rows", TType::T_I64, 3);
        w.writeI64(10);
        w.writeFieldEnd();
        if (sorting_columns_element_type)
        {
            parq::SortingColumn sorting_column;
            sorting_column.__set_column_idx(1);
            sorting_column.__set_descending(true);
            sorting_column.__set_nulls_first(false);
            w.writeFieldBegin("sorting_columns", TType::T_LIST, 4);
            w.writeListBegin(*sorting_columns_element_type, 1);
            sorting_column.write(&w);
            w.writeListEnd();
            w.writeFieldEnd();
        }
        w.writeFieldStop();
        w.writeStructEnd();
    };
}

/// Writes the `row_groups` field once per list in `row_group_lists`.
std::string writeFileMetaData(const std::vector<std::vector<WriteFn>> & row_group_lists)
{
    auto buffer = std::make_shared<MemoryBuffer>();
    Protocol w(buffer);
    const auto schema = makeSchema(5);
    w.writeStructBegin("FileMetaData");
    w.writeFieldBegin("version", TType::T_I32, 1);
    w.writeI32(1);
    w.writeFieldEnd();
    w.writeFieldBegin("schema", TType::T_LIST, 2);
    w.writeListBegin(TType::T_STRUCT, uint32_t(schema.size()));
    for (const auto & element : schema)
        element.write(&w);
    w.writeListEnd();
    w.writeFieldEnd();
    w.writeFieldBegin("num_rows", TType::T_I64, 3);
    w.writeI64(30);
    w.writeFieldEnd();
    for (const auto & row_groups : row_group_lists)
    {
        w.writeFieldBegin("row_groups", TType::T_LIST, 4);
        w.writeListBegin(TType::T_STRUCT, uint32_t(row_groups.size()));
        for (const auto & write_row_group : row_groups)
            write_row_group(w);
        w.writeListEnd();
        w.writeFieldEnd();
    }
    w.writeFieldStop();
    w.writeStructEnd();
    return buffer->getBufferAsString();
}

WriteFn chunk(size_t row_group, size_t column)
{
    return [chunk = makeColumnChunk(row_group, column)](Protocol & w) { chunk.write(&w); };
}

/// All required fields of ColumnMetaData except `encodings` (field 2).
void writeColumnMetaDataRequiredFieldsExceptEncodings(Protocol & w)
{
    w.writeFieldBegin("type", TType::T_I32, 1);
    w.writeI32(parq::Type::INT32);
    w.writeFieldEnd();
    w.writeFieldBegin("path_in_schema", TType::T_LIST, 3);
    w.writeListBegin(TType::T_STRING, 1);
    w.writeString("c1");
    w.writeListEnd();
    w.writeFieldEnd();
    for (int16_t id : std::initializer_list<int16_t>{4, 5, 6, 7, 9})
    {
        w.writeFieldBegin("", id == 4 ? TType::T_I32 : TType::T_I64, id);
        if (id == 4)
            w.writeI32(0);
        else
            w.writeI64(4);
        w.writeFieldEnd();
    }
}

void writeEncodings(Protocol & w)
{
    w.writeFieldBegin("encodings", TType::T_LIST, 2);
    w.writeListBegin(TType::T_I32, 1);
    w.writeI32(parq::Encoding::PLAIN);
    w.writeListEnd();
    w.writeFieldEnd();
}

/// A column chunk with an unknown field 100 = list<list<...list<i32>...>>, `levels` lists deep, the
/// innermost one empty. With `in_encoding_stats`, the field is in the only element of
/// `meta_data.encoding_stats`, where `skip` counts one more level (the list) than the generated reader.
WriteFn deeplyNestedChunk(size_t levels, bool in_encoding_stats)
{
    return [=](Protocol & w)
    {
        auto write_nested = [&]
        {
            w.writeFieldBegin("", TType::T_LIST, 100);
            for (size_t i = 1; i < levels; ++i)
                w.writeListBegin(TType::T_LIST, 1);
            w.writeListBegin(TType::T_I32, 0);
            w.writeFieldEnd();
        };

        w.writeStructBegin("ColumnChunk");
        w.writeFieldBegin("file_offset", TType::T_I64, 2);
        w.writeI64(4);
        w.writeFieldEnd();
        if (in_encoding_stats)
        {
            w.writeFieldBegin("meta_data", TType::T_STRUCT, 3);
            w.writeStructBegin("ColumnMetaData");
            writeEncodings(w);
            writeColumnMetaDataRequiredFieldsExceptEncodings(w);
            w.writeFieldBegin("encoding_stats", TType::T_LIST, 13);
            w.writeListBegin(TType::T_STRUCT, 1);
            w.writeStructBegin("PageEncodingStats");
            for (int16_t id : std::initializer_list<int16_t>{1, 2, 3})
            {
                w.writeFieldBegin("", TType::T_I32, id);
                w.writeI32(0);
                w.writeFieldEnd();
            }
            write_nested();
            w.writeFieldStop();
            w.writeStructEnd();
            w.writeListEnd();
            w.writeFieldEnd();
            w.writeFieldStop();
            w.writeStructEnd();
            w.writeFieldEnd();
        }
        else
        {
            write_nested();
        }
        w.writeFieldStop();
        w.writeStructEnd();
    };
}

/// `meta_data.encodings` declared as list<string> with the element bytes 02 00 00: `skip` reads one
/// 2-byte string, the generated reader reads 02 as an i32 and then the zeros as the ends of
/// ColumnMetaData and ColumnChunk, so it stops 2 bytes before the end that `skip` recorded.
WriteFn chunkWithMisdeclaredEncodings()
{
    return [](Protocol & w)
    {
        w.writeStructBegin("ColumnChunk");
        w.writeFieldBegin("file_offset", TType::T_I64, 2);
        w.writeI64(4);
        w.writeFieldEnd();
        w.writeFieldBegin("meta_data", TType::T_STRUCT, 3);
        w.writeStructBegin("ColumnMetaData");
        writeColumnMetaDataRequiredFieldsExceptEncodings(w);
        w.writeFieldBegin("encodings", TType::T_LIST, 2);
        w.writeListBegin(TType::T_STRING, 1);
        const uint8_t element[] = {0x02, 0x00, 0x00};
        w.getTransport()->write(element, sizeof(element));
        w.writeListEnd();
        w.writeFieldEnd();
        w.writeFieldStop();
        w.writeStructEnd();
        w.writeFieldEnd();
        w.writeFieldStop();
        w.writeStructEnd();
    };
}

void writeRandomValue(Protocol & w, TType type, std::mt19937_64 & rng, size_t depth)
{
    static constexpr TType types[] = {TType::T_BOOL, TType::T_BYTE, TType::T_I16, TType::T_I32, TType::T_I64, TType::T_DOUBLE,
        TType::T_STRING, TType::T_UUID, TType::T_STRUCT, TType::T_LIST, TType::T_SET, TType::T_MAP};
    auto random_type = [&] { return types[rng() % (depth < 3 ? std::size(types) : 8)]; };
    switch (type)
    {
        case TType::T_BOOL: w.writeBool(rng() % 2); break;
        case TType::T_BYTE: w.writeByte(int8_t(rng())); break;
        case TType::T_I16: w.writeI16(int16_t(rng())); break;
        case TType::T_I32: w.writeI32(int32_t(rng())); break;
        case TType::T_I64: w.writeI64(int64_t(rng() >> (rng() % 64))); break;
        case TType::T_DOUBLE: w.writeDouble(1.5); break;
        case TType::T_STRING: w.writeBinary(std::string(rng() % 20, 'x')); break;
        case TType::T_UUID: w.writeUUID(apache::thrift::TUuid{}); break;
        case TType::T_STRUCT:
        {
            w.writeStructBegin("");
            for (size_t i = rng() % 4; i > 0; --i)
            {
                const TType field_type = random_type();
                w.writeFieldBegin("", field_type, int16_t(rng() % 300));
                writeRandomValue(w, field_type, rng, depth + 1);
                w.writeFieldEnd();
            }
            w.writeFieldStop();
            w.writeStructEnd();
            break;
        }
        case TType::T_LIST:
        case TType::T_SET:
        {
            const TType element_type = random_type();
            const auto size = uint32_t(rng() % 20);
            w.writeListBegin(element_type, size);
            for (uint32_t i = 0; i < size; ++i)
                writeRandomValue(w, element_type, rng, depth + 1);
            w.writeListEnd();
            break;
        }
        case TType::T_MAP:
        {
            const TType key_type = random_type();
            const TType value_type = random_type();
            const auto size = uint32_t(rng() % 3);
            w.writeMapBegin(key_type, value_type, size);
            for (uint32_t i = 0; i < size; ++i)
            {
                writeRandomValue(w, key_type, rng, depth + 1);
                writeRandomValue(w, value_type, rng, depth + 1);
            }
            w.writeMapEnd();
            break;
        }
        default: break;
    }
}

/// `file_offset` plus fields that ColumnChunk does not have, of every type.
WriteFn chunkWithUnknownFields(uint64_t seed)
{
    return [=](Protocol & w)
    {
        static constexpr TType field_types[] = {TType::T_BOOL, TType::T_STRUCT, TType::T_LIST, TType::T_SET, TType::T_MAP, TType::T_UUID};
        std::mt19937_64 rng(seed); // NOLINT(bugprone-random-generator-seed,cert-msc32-c,cert-msc51-cpp): deterministic seed for reproducible test
        w.writeStructBegin("ColumnChunk");
        w.writeFieldBegin("file_offset", TType::T_I64, 2);
        w.writeI64(int64_t(seed % 1000));
        w.writeFieldEnd();
        for (int16_t id = 100; id < 112; ++id)
        {
            const TType type = field_types[rng() % std::size(field_types)];
            w.writeFieldBegin("", type, int16_t(id + rng() % 2 * 200));
            writeRandomValue(w, type, rng, 0);
            w.writeFieldEnd();
        }
        w.writeFieldStop();
        w.writeStructEnd();
    };
}

}

TEST(ParquetLazyColumnChunks, EquivalentToFullParse)
{
    const auto metadata = makeFileMetaData();
    const std::string bytes = serialize(metadata);
    const auto full = parseFull(bytes);
    ASSERT_EQ(full, metadata);

    const auto skeleton = parseSkeleton(bytes);
    auto full_without_chunks = full;
    for (auto & row_group : full_without_chunks.row_groups)
        row_group.columns.clear();
    EXPECT_EQ(skeleton.metadata, full_without_chunks);

    ASSERT_EQ(skeleton.offsets.size(), 3);
    for (size_t i = 0; i < 3; ++i)
    {
        const auto & offsets = skeleton.offsets[i];
        ASSERT_EQ(offsets.size(), 6);
        if (i > 0)
            EXPECT_LT(skeleton.offsets[i - 1].back(), offsets.front());
        /// Each recorded range holds exactly the serialization of its column chunk.
        for (size_t j = 0; j < 5; ++j)
            EXPECT_EQ(bytes.substr(offsets[j], offsets[j + 1] - offsets[j]), serialize(metadata.row_groups[i].columns[j]));
    }

    std::vector<FileColumnChunks> chunks;
    deserializeColumnChunks(bytes.data(), bytes.size(), skeleton.offsets, {1, 3}, chunks);
    ASSERT_EQ(chunks.size(), 2);
    for (size_t k = 0; k < 2; ++k)
    {
        ASSERT_EQ(chunks[k].chunks.size(), 3);
        for (size_t i = 0; i < 3; ++i)
            EXPECT_EQ(chunks[k].chunks[i], full.row_groups[i].columns[k == 0 ? 1 : 3]);
        EXPECT_EQ(chunks[k].memory_bytes, expectedColumnMemory(chunks[k]));
    }

    EXPECT_EQ(parseLazy(bytes), full);
}

TEST(ParquetLazyColumnChunks, ReaderParsesOnlyItsColumns)
{
    auto metadata = makeFileMetaData();
    metadata.__set_created_by("DuckDB");
    metadata.row_groups[0].columns[1].meta_data.__set_dictionary_page_offset(0);
    for (auto & row_group : metadata.row_groups)
        row_group.columns[3].__isset.offset_index_offset = false;
    const std::string footer = serialize(metadata);

    /// The two writer fixups of Reader::fixUpColumnChunk.
    auto expected = metadata;
    expected.row_groups[0].columns[1].meta_data.__isset.dictionary_page_offset = false;
    for (auto & row_group : expected.row_groups)
    {
        auto & meta = row_group.columns[3].meta_data;
        meta.data_page_offset = meta.dictionary_page_offset;
        meta.__isset.dictionary_page_offset = false;
        meta.dictionary_page_offset = 0;
    }

    std::string file = "PAR1" + footer;
    const auto footer_size = int32_t(footer.size());
    file.append(reinterpret_cast<const char *>(&footer_size), 4);
    file += "PAR1";

    /// The whole footer in the first read, and in a second one.
    for (size_t footer_read_size : {0, 16})
    {
        ReadBufferFromMemory in(file.data(), file.size());
        Prefetcher prefetcher;
        prefetcher.init(&in, ReadOptions{}, /*parser_shared_resources_=*/ nullptr);
        auto file_footer = Reader::readFileFooter(prefetcher, footer_read_size);
        EXPECT_EQ(file_footer->serialized, footer);

        Reader reader;
        reader.setFileFooter(file_footer);
        for (size_t column_idx : {3, 1, 3})
        {
            Reader::PrimitiveColumnInfo column_info;
            column_info.column_idx = column_idx;
            reader.primitive_columns.push_back(std::move(column_info));
        }
        reader.parseColumnChunks();

        for (size_t i = 0; i < 3; ++i)
        {
            EXPECT_EQ(reader.numColumnChunks(i), 5);
            EXPECT_TRUE(reader.file_metadata().row_groups[i].columns.empty());
            EXPECT_EQ(reader.getColumnChunk(i, 1), expected.row_groups[i].columns[1]);
            EXPECT_EQ(reader.getColumnChunk(i, 3), expected.row_groups[i].columns[3]);
        }
    }
}

TEST(ParquetLazyColumnChunks, DuplicatedFields)
{
    /// The generated reader keeps the last `row_groups` and the last `columns`.
    const std::string bytes = writeFileMetaData({
        {rowGroup({{chunk(0, 0), chunk(0, 1)}})},
        {rowGroup({{chunk(1, 0)}, {chunk(1, 1), chunk(1, 2), chunk(1, 3)}}), rowGroup({{chunk(2, 0), chunk(2, 1)}})},
    });
    const auto full = parseFull(bytes);
    ASSERT_EQ(full.row_groups.size(), 2);
    ASSERT_EQ(full.row_groups[0].columns.size(), 3);
    EXPECT_EQ(full.row_groups[0].columns[0], makeColumnChunk(1, 1));
    EXPECT_EQ(parseLazy(bytes), full);
}

TEST(ParquetLazyColumnChunks, ListElementTypeIgnored)
{
    /// The generated reader reads list elements as structs whatever type the list declares.
    const std::string bytes = writeFileMetaData({{
        rowGroup({{chunk(0, 0), chunk(0, 1)}}, TType::T_I32, TType::T_I32),
        rowGroup({{chunk(1, 0), chunk(1, 1)}}, TType::T_STRING),
    }});
    const auto full = parseFull(bytes);
    ASSERT_EQ(full.row_groups.size(), 2);
    ASSERT_EQ(full.row_groups[0].sorting_columns.size(), 1);
    EXPECT_EQ(full.row_groups[1].columns[1], makeColumnChunk(1, 1));
    EXPECT_EQ(parseLazy(bytes), full);
}

TEST(ParquetLazyColumnChunks, UnknownFieldsOfEveryType)
{
    /// The generated reader skips unknown fields with `skip`, the skeleton skips whole column chunks.
    for (uint64_t seed = 0; seed < 300; ++seed)
    {
        const std::string bytes = writeFileMetaData(
            {{rowGroup({{chunkWithUnknownFields(seed), chunk(0, 1), chunkWithUnknownFields(seed + 1000)}})}});
        const auto full = parseFull(bytes);
        ASSERT_EQ(full.row_groups.at(0).columns.size(), 3);
        ASSERT_EQ(parseLazy(bytes), full) << seed;
    }
}

TEST(ParquetLazyColumnChunks, NestingLimit)
{
    for (bool in_encoding_stats : {false, true})
    {
        SCOPED_TRACE(in_encoding_stats);
        auto file = [&](size_t levels)
        {
            return writeFileMetaData({{rowGroup({{chunk(0, 0), deeplyNestedChunk(levels, in_encoding_stats)}})}});
        };

        size_t deepest = 0;
        while (errorCode([&] { parseFull(file(deepest + 1)); }) == 0)
            ++deepest;
        ASSERT_GT(deepest, 10);

        const auto deepest_accepted = file(deepest);
        EXPECT_EQ(parseLazy(deepest_accepted), parseFull(deepest_accepted));

        const auto one_deeper = file(deepest + 1);
        EXPECT_EQ(errorCode([&] { parseFull(one_deeper); }), ErrorCodes::INCORRECT_DATA);
        EXPECT_EQ(errorCode([&] { parseLazy(one_deeper); }), ErrorCodes::INCORRECT_DATA);

        const auto very_deep = file(100000);
        EXPECT_EQ(errorCode([&] { parseFull(very_deep); }), ErrorCodes::INCORRECT_DATA);
        EXPECT_EQ(errorCode([&] { parseSkeleton(very_deep); }), ErrorCodes::INCORRECT_DATA);
    }
}

TEST(ParquetLazyColumnChunks, ColumnChunkEndMismatch)
{
    const std::string bytes = writeFileMetaData({{rowGroup({{chunk(0, 0), chunkWithMisdeclaredEncodings(), chunk(0, 2)}})}});
    const auto skeleton = parseSkeleton(bytes);
    const auto & offsets = skeleton.offsets.at(0);
    ASSERT_EQ(offsets.size(), 4);

    /// On its own the generated reader accepts the column chunk, 2 bytes short of its recorded end.
    auto buffer = std::make_shared<MemoryBuffer>(
        reinterpret_cast<uint8_t *>(const_cast<char *>(bytes.data() + offsets[1])), offsets[2] - offsets[1], MemoryBuffer::OBSERVE);
    Protocol protocol(buffer);
    parq::ColumnChunk alone;
    alone.read(&protocol);
    EXPECT_EQ(buffer->available_read(), 2);

    std::vector<FileColumnChunks> chunks;
    EXPECT_EQ(errorCode([&] { deserializeColumnChunks(bytes.data(), bytes.size(), skeleton.offsets, {1}, chunks); }), ErrorCodes::INCORRECT_DATA);
    EXPECT_EQ(errorCode([&] { deserializeColumnChunks(bytes.data(), bytes.size(), skeleton.offsets, {0, 2}, chunks); }), 0);
    EXPECT_EQ(chunks.at(1).chunks.at(0), makeColumnChunk(0, 2));
}

TEST(ParquetLazyColumnChunks, CorruptedFooters)
{
    const std::string original = serialize(makeFileMetaData());
    std::mt19937_64 rng(42); // NOLINT(bugprone-random-generator-seed,cert-msc32-c,cert-msc51-cpp): deterministic seed for reproducible test
    size_t both_ok = 0;
    size_t both_failed = 0;
    for (size_t iteration = 0; iteration < 20000; ++iteration)
    {
        std::string bytes = original;
        if (rng() % 8 == 0)
        {
            bytes.resize(rng() % bytes.size());
        }
        else
        {
            for (size_t flips = 1 + rng() % 4; flips > 0; --flips)
                bytes[rng() % bytes.size()] ^= char(1 + rng() % 255);
        }

        parq::FileMetaData full;
        parq::FileMetaData lazy;
        const int full_code = errorCode([&] { full = parseFull(bytes); });
        const int lazy_code = errorCode([&] { lazy = parseLazy(bytes); });
        ASSERT_TRUE(full_code == 0 || full_code == ErrorCodes::INCORRECT_DATA) << iteration << ": " << full_code;
        ASSERT_TRUE(lazy_code == 0 || lazy_code == ErrorCodes::INCORRECT_DATA) << iteration << ": " << lazy_code;
        if (full_code == 0 && lazy_code == 0)
        {
            ++both_ok;
            ASSERT_EQ(full, lazy) << iteration;
        }
        else if (full_code != 0 && lazy_code != 0)
            ++both_failed;
    }
    EXPECT_GT(both_ok, 0);
    EXPECT_GT(both_failed, 0);
}

TEST(ParquetLazyColumnChunks, MetadataCacheSharesColumnChunks)
{
    auto metadata = makeFileMetaData();
    parq::GeometryType geometry;
    geometry.__set_crs(std::string(60, 'g'));
    parq::LogicalType geometry_type;
    geometry_type.__set_GEOMETRY(geometry);
    metadata.schema[1].__set_logicalType(geometry_type);
    parq::GeographyType geography;
    geography.__set_crs(std::string(61, 'h'));
    parq::LogicalType geography_type;
    geography_type.__set_GEOGRAPHY(geography);
    metadata.schema[2].__set_logicalType(geography_type);
    const auto footer = makeFooter(metadata);
    /// Capacity retained by a repeated list field is charged too.
    footer->metadata.schema.reserve(footer->metadata.schema.size() + 8);
    footer->metadata.row_groups.reserve(footer->metadata.row_groups.size() + 8);
    footer->metadata.column_orders.reserve(footer->metadata.column_orders.size() + 8);
    ParquetMetadataCache cache("SLRU", 1ul << 30, 1000, 0.5);
    const auto key = ParquetMetadataCache::createKey("file.parquet", "etag");
    size_t loads = 0;
    auto load = [&]
    {
        ++loads;
        return std::shared_ptr<const FileFooter>(footer);
    };

    const auto [inserted, miss_is_hit] = cache.getOrSetMetadata(key, load);
    EXPECT_FALSE(miss_is_hit);
    EXPECT_EQ(inserted->footer, footer);
    EXPECT_TRUE(inserted->column_chunks.empty());
    const auto [cell, hit] = cache.getOrSetMetadata(key, load);
    EXPECT_TRUE(hit);
    EXPECT_EQ(cell, inserted);
    EXPECT_EQ(loads, 1);
    EXPECT_EQ(cache.sizeInBytes(), expectedCellWeight(*cell));

    const auto columns_1_3 = parseColumns(*footer, {1, 3});
    cache.addColumnChunks(key, cell, columns_1_3);
    const auto cell_1_3 = cache.get(key);
    ASSERT_NE(cell_1_3, cell);
    EXPECT_EQ(cell_1_3->footer, footer);
    EXPECT_EQ(cell_1_3->column_chunks, columns_1_3);
    EXPECT_EQ(cache.sizeInBytes(), expectedCellWeight(*cell_1_3));

    /// A reader takes the cached columns it reads, parses the others and drops the cached ones it doesn't read.
    Reader reader;
    reader.setFileFooter(cell_1_3->footer, cell_1_3->column_chunks);
    for (size_t column_idx : {4, 1})
    {
        Reader::PrimitiveColumnInfo column_info;
        column_info.column_idx = column_idx;
        reader.primitive_columns.push_back(std::move(column_info));
    }
    reader.parseColumnChunks();
    const auto & reader_columns = reader.getColumnChunks();
    ASSERT_EQ(reader_columns.size(), 5);
    EXPECT_EQ(reader_columns[1], columns_1_3[1]);
    EXPECT_EQ(reader_columns[3], nullptr);
    ASSERT_NE(reader_columns[4], nullptr);
    for (size_t i = 0; i < 3; ++i)
        EXPECT_EQ(reader.getColumnChunk(i, 4), metadata.row_groups[i].columns[4]);

    /// Only the missing columns are added.
    cache.addColumnChunks(key, cell_1_3, reader_columns);
    const auto cell_1_3_4 = cache.get(key);
    ASSERT_EQ(cell_1_3_4->column_chunks.size(), 5);
    EXPECT_EQ(cell_1_3_4->column_chunks[1], columns_1_3[1]);
    EXPECT_EQ(cell_1_3_4->column_chunks[3], columns_1_3[3]);
    EXPECT_EQ(cell_1_3_4->column_chunks[4], reader_columns[4]);
    cache.addColumnChunks(key, cell_1_3_4, parseColumns(*footer, {1}));
    EXPECT_EQ(cache.get(key), cell_1_3_4);
    EXPECT_EQ(cache.sizeInBytes(), expectedCellWeight(*cell_1_3_4));

    /// The bloom filter length fallback adds every column, keeping the parsed ones and taking the cached ones.
    Reader reader_all;
    reader_all.setFileFooter(cell_1_3_4->footer, cell_1_3_4->column_chunks);
    Reader::PrimitiveColumnInfo column_info_0;
    column_info_0.column_idx = 0;
    reader_all.primitive_columns.push_back(std::move(column_info_0));
    reader_all.parseColumnChunks();
    const auto column_0 = reader_all.getColumnChunks().at(0);
    reader_all.ensureColumnChunks({0, 1, 2, 3, 4});
    const auto & all_columns = reader_all.getColumnChunks();
    ASSERT_EQ(all_columns.size(), 5);
    EXPECT_EQ(all_columns[0], column_0);
    for (size_t j : {1, 3, 4})
        EXPECT_EQ(all_columns[j], cell_1_3_4->column_chunks[j]);
    for (size_t i = 0; i < 3; ++i)
        EXPECT_EQ(reader_all.getColumnChunk(i, 2), metadata.row_groups[i].columns[2]);

    /// An add onto a cell that is no longer resident does nothing.
    cache.clear();
    cache.addColumnChunks(key, cell_1_3_4, parseColumns(*footer, {0}));
    EXPECT_FALSE(cache.contains(key));

    const auto reloaded = cache.getOrSetMetadata(key, load).first;
    EXPECT_EQ(loads, 2);
    cache.addColumnChunks(key, cell_1_3_4, parseColumns(*footer, {0}));
    EXPECT_EQ(cache.get(key), reloaded);
    EXPECT_TRUE(reloaded->column_chunks.empty());

    cache.addColumnChunks(key, reloaded, parseColumns(*footer, {2}));
    const auto added_by_other_reader = cache.get(key);
    ASSERT_NE(added_by_other_reader, reloaded);
    cache.addColumnChunks(key, reloaded, parseColumns(*footer, {0}));
    EXPECT_EQ(cache.get(key), added_by_other_reader);
    EXPECT_EQ(cache.sizeInBytes(), expectedCellWeight(*added_by_other_reader));
}

TEST(ParquetLazyColumnChunks, MetadataCacheConcurrentAdds)
{
    const auto metadata = makeFileMetaData();
    const auto footer = makeFooter(metadata);
    ParquetMetadataCache cache("SLRU", 1ul << 30, 1000, 0.5);
    const auto key = ParquetMetadataCache::createKey("file.parquet", "etag");

    std::atomic<size_t> loads = 0;
    std::atomic<size_t> cached_chunks_seen = 0;
    std::atomic<size_t> wrong_chunks = 0;
    auto check_cell = [&](const ParquetMetadataCacheCell & cell)
    {
        for (size_t j = 0; j < cell.column_chunks.size(); ++j)
        {
            for (size_t i = 0; cell.column_chunks[j] && i < metadata.row_groups.size(); ++i)
            {
                ++cached_chunks_seen;
                if (cell.column_chunks[j]->chunks.at(i) != metadata.row_groups[i].columns[j])
                    ++wrong_chunks;
            }
        }
    };

    std::atomic<bool> stop = false;
    std::thread clearer([&]
    {
        while (!stop)
        {
            cache.clear();
            std::this_thread::sleep_for(std::chrono::microseconds(200));
        }
    });
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    std::vector<std::thread> readers;
    for (size_t t = 0; t < 8; ++t)
    {
        readers.emplace_back([&, t]
        {
            std::mt19937 rng(static_cast<std::mt19937::result_type>(t));
            for (size_t iteration = 0;
                 iteration < 200 || ((loads.load() <= 1 || cached_chunks_seen.load() == 0) && std::chrono::steady_clock::now() < deadline);
                 ++iteration)
            {
                const auto cell = cache.getOrSetMetadata(key, [&]
                {
                    ++loads;
                    return std::shared_ptr<const FileFooter>(footer);
                }).first;
                check_cell(*cell);
                std::vector<FileColumnChunksPtr> columns(5);
                for (size_t j = 0; j < 5; ++j)
                {
                    if (rng() % 2 == 0)
                        continue;
                    if (j < cell->column_chunks.size() && cell->column_chunks[j])
                        columns[j] = cell->column_chunks[j];
                    else
                        columns[j] = parseColumns(*footer, {j})[j];
                }
                cache.addColumnChunks(key, cell, columns);
            }
        });
    }
    for (auto & thread : readers)
        thread.join();
    stop = true;
    clearer.join();

    if (const auto cell = cache.get(key))
    {
        check_cell(*cell);
        EXPECT_EQ(cache.sizeInBytes(), expectedCellWeight(*cell));
    }
    EXPECT_EQ(wrong_chunks.load(), 0);
    /// Both the clears and the adds took effect while the readers ran.
    EXPECT_GT(loads.load(), 1);
    EXPECT_GT(cached_chunks_seen.load(), 0);
}

#endif
