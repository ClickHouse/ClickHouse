#include "ParquetLeafColReader.h"

#include <utility>

#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnUnique.h>
#include <Columns/ColumnsNumber.h>
#include <Common/logger_useful.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/NumberTraits.h>
#include <IO/ReadBufferFromMemory.h>

#include <arrow/util/bit_util.h>
#include <parquet/column_page.h>
#include <parquet/column_reader.h>
#include <parquet/metadata.h>
#include <parquet/schema.h>
#include <parquet/types.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
    extern const int PARQUET_EXCEPTION;
}

namespace
{

template <typename TypeVisitor>
void visitColStrIndexType(size_t data_size, TypeVisitor && visitor)
{
    // refer to: DataTypeLowCardinality::createColumnUniqueImpl
    if (data_size < (1ull << 8))
    {
        visitor(static_cast<ColumnUInt8 *>(nullptr));
    }
    else if (data_size < (1ull << 16))
    {
        visitor(static_cast<ColumnUInt16 *>(nullptr));
    }
    else if (data_size < (1ull << 32))
    {
        visitor(static_cast<ColumnUInt32 *>(nullptr));
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "unsupported data size {}", data_size);
    }
}

void reserveColumnStrRows(MutableColumnPtr & col, UInt64 rows_num)
{
    col->reserve(rows_num);

    /// Never reserve for too big size according to SerializationString::deserializeBinaryBulk
    if (rows_num < 256 * 1024 * 1024)
    {
        try
        {
            static_cast<ColumnString *>(col.get())->getChars().reserve(rows_num);
        }
        catch (Exception & e)
        {
            e.addMessage("(limit = " + toString(rows_num) + ")");
            throw;
        }
    }
};


template <typename TColumn>
ColumnPtr readDictPage(
    const parquet::DictionaryPage & page,
    const parquet::ColumnDescriptor & col_des,
    const DataTypePtr & /* data_type */);

template <>
ColumnPtr readDictPage<ColumnString>(
    const parquet::DictionaryPage & page,
    const parquet::ColumnDescriptor & /* col_des */,
    const DataTypePtr & /* data_type */)
{
    auto col = ColumnString::create();
    col->getOffsets().resize(page.num_values() + 1);
    col->getChars().reserve(page.num_values());
    ParquetDataBuffer buffer(page.data(), page.size());

    // will be read as low cardinality column
    // in which case, the null key is set to first position, so the first string should be empty
    col->getChars().push_back(0);
    col->getOffsets()[0] = 1;
    for (auto i = 1; i <= page.num_values(); i++)
    {
        buffer.readString(*col, i);
    }
    return col;
}

template <>
ColumnPtr readDictPage<ColumnDecimal<DateTime64>>(
    const parquet::DictionaryPage & page,
    const parquet::ColumnDescriptor & col_des,
    const DataTypePtr & data_type)
{

    const auto & datetime_type = assert_cast<const DataTypeDateTime64 &>(*data_type);
    auto dict_col = ColumnDecimal<DateTime64>::create(page.num_values(), datetime_type.getScale());
    auto * col_data = dict_col->getData().data();
    ParquetDataBuffer buffer(page.data(), page.size(), datetime_type.getScale());
    if (col_des.physical_type() == parquet::Type::INT64)
    {
        buffer.readBytes(dict_col->getData().data(), page.num_values() * sizeof(Int64));
    }
    else
    {
        for (auto i = 0; i < page.num_values(); i++)
        {
            buffer.readDateTime64FromInt96(col_data[i]);
        }
    }
    return dict_col;
}

template <is_col_over_big_decimal TColumnDecimal>
ColumnPtr readDictPage(
    const parquet::DictionaryPage & page,
    const parquet::ColumnDescriptor & col_des,
    const DataTypePtr & /* data_type */)
{
    auto dict_col = TColumnDecimal::create(page.num_values(), col_des.type_scale());
    auto * col_data = dict_col->getData().data();
    ParquetDataBuffer buffer(page.data(), page.size());
    for (auto i = 0; i < page.num_values(); i++)
    {
        buffer.readOverBigDecimal(col_data + i, col_des.type_length());
    }
    return dict_col;
}

template <is_col_int_decimal TColumnDecimal> requires (!std::is_same_v<typename TColumnDecimal::ValueType, DateTime64>)
ColumnPtr readDictPage(
    const parquet::DictionaryPage & page,
    const parquet::ColumnDescriptor & col_des,
    const DataTypePtr & /* data_type */)
{
    auto dict_col = TColumnDecimal::create(page.num_values(), col_des.type_scale());
    ParquetDataBuffer buffer(page.data(), page.size());
    buffer.readBytes(dict_col->getData().data(), page.num_values() * sizeof(typename TColumnDecimal::ValueType));
    return dict_col;
}

template <is_col_vector TColumnVector>
ColumnPtr readDictPage(
    const parquet::DictionaryPage & page,
    const parquet::ColumnDescriptor & /* col_des */,
    const DataTypePtr & /* data_type */)
{
    auto dict_col = TColumnVector::create(page.num_values());
    ParquetDataBuffer buffer(page.data(), page.size());
    buffer.readBytes(dict_col->getData().data(), page.num_values() * sizeof(typename TColumnVector::ValueType));
    return dict_col;
}


template <typename TColumn>
std::unique_ptr<ParquetDataValuesReader> createPlainReader(
    const parquet::ColumnDescriptor & col_des,
    RleValuesReaderPtr def_level_reader,
    ParquetDataBuffer buffer);

template <is_col_over_big_decimal TColumnDecimal>
std::unique_ptr<ParquetDataValuesReader> createPlainReader(
    const parquet::ColumnDescriptor & col_des,
    RleValuesReaderPtr def_level_reader,
    ParquetDataBuffer buffer)
{
    return std::make_unique<ParquetFixedLenPlainReader<TColumnDecimal>>(
        col_des.max_definition_level(),
        col_des.type_length(),
        std::move(def_level_reader),
        std::move(buffer));
}

template <typename TColumn>
std::unique_ptr<ParquetDataValuesReader> createPlainReader(
    const parquet::ColumnDescriptor & col_des,
    RleValuesReaderPtr def_level_reader,
    ParquetDataBuffer buffer)
{
    if (std::is_same_v<TColumn, ColumnDecimal<DateTime64>> && col_des.physical_type() == parquet::Type::INT96)
        return std::make_unique<ParquetPlainValuesReader<TColumn, ParquetReaderTypes::TimestampInt96>>(
            col_des.max_definition_level(), std::move(def_level_reader), std::move(buffer));
    return std::make_unique<ParquetPlainValuesReader<TColumn>>(
        col_des.max_definition_level(), std::move(def_level_reader), std::move(buffer));
}


} // anonymous namespace


template <typename TColumn>
ParquetLeafColReader<TColumn>::ParquetLeafColReader(
    const parquet::ColumnDescriptor & col_descriptor_,
    DataTypePtr base_type_,
    std::unique_ptr<parquet::ColumnChunkMetaData> meta_,
    std::unique_ptr<parquet::PageReader> reader_)
    : col_descriptor(col_descriptor_)
    , base_data_type(base_type_)
    , col_chunk_meta(std::move(meta_))
    , parquet_page_reader(std::move(reader_))
    , log(&Poco::Logger::get("ParquetLeafColReader"))
{
}

template <typename TColumn>
ColumnWithTypeAndName ParquetLeafColReader<TColumn>::readBatch(UInt64 rows_num, const String & name)
{
    reading_rows_num = rows_num;
    auto readPageIfEmpty = [&]()
    {
        while (!cur_page_values) readPage();
    };

    // make sure the dict page has been read, and the status is updated
    readPageIfEmpty();
    resetColumn(rows_num);

    while (rows_num)
    {
        // if dictionary page encountered, another page should be read
        readPageIfEmpty();

        auto read_values = static_cast<UInt32>(std::min(rows_num, static_cast<UInt64>(cur_page_values)));
        data_values_reader->readBatch(column, *null_map, read_values);

        cur_page_values -= read_values;
        rows_num -= read_values;
    }

    return releaseColumn(name);
}

template <>
void ParquetLeafColReader<ColumnString>::resetColumn(UInt64 rows_num)
{
    if (reading_low_cardinality)
    {
        assert(dictionary);
        visitColStrIndexType(dictionary->size(), [&]<typename TColVec>(TColVec *)
        {
            column = TColVec::create();
        });

        // only first position is used
        null_map = std::make_unique<LazyNullMap>(1);
        column->reserve(rows_num);
    }
    else
    {
        null_map = std::make_unique<LazyNullMap>(rows_num);
        column = ColumnString::create();
        reserveColumnStrRows(column, rows_num);
    }
}

template <typename TColumn>
void ParquetLeafColReader<TColumn>::resetColumn(UInt64 rows_num)
{
    assert(!reading_low_cardinality);

    column = base_data_type->createColumn();
    column->reserve(rows_num);
    null_map = std::make_unique<LazyNullMap>(rows_num);
}

template <typename TColumn>
void ParquetLeafColReader<TColumn>::degradeDictionary()
{
    // if last batch read all dictionary indices, then degrade is not needed this time
    if (!column)
    {
        dictionary = nullptr;
        return;
    }
    assert(dictionary && !column->empty());

    null_map = std::make_unique<LazyNullMap>(reading_rows_num);
    auto col_existing = std::move(column);
    column = ColumnString::create();
    reserveColumnStrRows(column, reading_rows_num);

    ColumnString & col_dest = *static_cast<ColumnString *>(column.get());
    const ColumnString & col_dict_str = *static_cast<const ColumnString *>(dictionary.get());

    visitColStrIndexType(dictionary->size(), [&]<typename TColVec>(TColVec *)
    {
        const TColVec & col_src = *static_cast<const TColVec *>(col_existing.get());

        // It will be easier to create a ColumnLowCardinality and call convertToFullColumn() on it,
        // while the performance loss is ignorable, the implementation can be updated next time.
        col_dest.getOffsets().resize(col_src.size());
        for (size_t i = 0; i < col_src.size(); i++)
        {
            auto src_idx = col_src.getData()[i];
            if (0 == src_idx)
            {
                null_map->setNull(i);
            }
            auto dict_chars_cursor = col_dict_str.getOffsets()[src_idx - 1];
            auto str_len = col_dict_str.getOffsets()[src_idx] - dict_chars_cursor;
            auto dst_chars_cursor = col_dest.getChars().size();
            col_dest.getChars().resize(dst_chars_cursor + str_len);

            memcpySmallAllowReadWriteOverflow15(
                &col_dest.getChars()[dst_chars_cursor], &col_dict_str.getChars()[dict_chars_cursor], str_len);
            col_dest.getOffsets()[i] = col_dest.getChars().size();
        }
    });
    dictionary = nullptr;
    LOG_DEBUG(log, "degraded dictionary to normal column");
}

template <typename TColumn>
ColumnWithTypeAndName ParquetLeafColReader<TColumn>::releaseColumn(const String & name)
{
    DataTypePtr data_type = base_data_type;
    if (reading_low_cardinality)
    {
        MutableColumnPtr col_unique;
        if (null_map->getNullableCol())
        {
            data_type = std::make_shared<DataTypeNullable>(data_type);
            col_unique = ColumnUnique<TColumn>::create(dictionary->assumeMutable(), true);
        }
        else
        {
            col_unique = ColumnUnique<TColumn>::create(dictionary->assumeMutable(), false);
        }
        column = ColumnLowCardinality::create(std::move(col_unique), std::move(column), true);
        data_type = std::make_shared<DataTypeLowCardinality>(data_type);
    }
    else
    {
        if (null_map->getNullableCol())
        {
            column = ColumnNullable::create(std::move(column), null_map->getNullableCol()->assumeMutable());
            data_type = std::make_shared<DataTypeNullable>(data_type);
        }
    }
    ColumnWithTypeAndName res = {std::move(column), data_type, name};
    column = nullptr;
    null_map = nullptr;

    return res;
}

template <typename TColumn>
void ParquetLeafColReader<TColumn>::readPage()
{
    // refer to: ColumnReaderImplBase::ReadNewPage in column_reader.cc
    // this is where decompression happens
    auto cur_page = parquet_page_reader->NextPage();
    switch (cur_page->type())
    {
        case parquet::PageType::DATA_PAGE:
            readPageV1(*std::static_pointer_cast<parquet::DataPageV1>(cur_page));
            break;
        case parquet::PageType::DATA_PAGE_V2:
            readPageV2(*std::static_pointer_cast<parquet::DataPageV2>(cur_page));
            break;
        case parquet::PageType::DICTIONARY_PAGE:
        {
            const parquet::DictionaryPage & dict_page = *std::static_pointer_cast<parquet::DictionaryPage>(cur_page);
            if (unlikely(
                dict_page.encoding() != parquet::Encoding::PLAIN_DICTIONARY
                && dict_page.encoding() != parquet::Encoding::PLAIN))
            {
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED, "Unsupported dictionary page encoding {}", dict_page.encoding());
            }
            LOG_DEBUG(log, "{} values in dictionary page of column {}", dict_page.num_values(), col_descriptor.name());

            dictionary = readDictPage<TColumn>(dict_page, col_descriptor, base_data_type);
            if (unlikely(dictionary->size() < 2))
            {
                // must not small than ColumnUnique<ColumnString>::numSpecialValues()
                dictionary->assumeMutable()->insertManyDefaults(2);
            }
            if (std::is_same_v<TColumn, ColumnString>)
            {
                reading_low_cardinality = true;
            }
            break;
        }
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported page type: {}", cur_page->type());
    }
}

template <typename TColumn>
void ParquetLeafColReader<TColumn>::initDataReader(
    parquet::Encoding::type enconding_type,
    const uint8_t * buffer,
    std::size_t max_size,
    std::unique_ptr<RleValuesReader> && def_level_reader)
{
    switch (enconding_type)
    {
        case parquet::Encoding::PLAIN:
        {
            if (reading_low_cardinality)
            {
                reading_low_cardinality = false;
                degradeDictionary();
            }

            ParquetDataBuffer parquet_buffer = [&]()
            {
                if constexpr (!std::is_same_v<ColumnDecimal<DateTime64>, TColumn>)
                    return ParquetDataBuffer(buffer, max_size);

                auto scale = assert_cast<const DataTypeDateTime64 &>(*base_data_type).getScale();
                return ParquetDataBuffer(buffer, max_size, scale);
            }();
            data_values_reader = createPlainReader<TColumn>(
                col_descriptor, std::move(def_level_reader), std::move(parquet_buffer));
            break;
        }
        case parquet::Encoding::RLE_DICTIONARY:
        case parquet::Encoding::PLAIN_DICTIONARY:
        {
            if (unlikely(!dictionary))
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "dictionary should be existed");
            }

            // refer to: DictDecoderImpl::SetData in encoding.cc
            auto bit_width = *buffer;
            auto bit_reader = std::make_unique<arrow::bit_util::BitReader>(++buffer, --max_size);
            data_values_reader = createDictReader(
                std::move(def_level_reader), std::make_unique<RleValuesReader>(std::move(bit_reader), bit_width));
            break;
        }
        case parquet::Encoding::BYTE_STREAM_SPLIT:
        case parquet::Encoding::DELTA_BINARY_PACKED:
        case parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY:
        case parquet::Encoding::DELTA_BYTE_ARRAY:
            throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Unsupported encoding: {}", enconding_type);

        default:
            throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Unknown encoding type: {}", enconding_type);
    }
}

template <typename TColumn>
void ParquetLeafColReader<TColumn>::readPageV1(const parquet::DataPageV1 & page)
{
    cur_page_values = page.num_values();

    // refer to: VectorizedColumnReader::readPageV1 in Spark and LevelDecoder::SetData in column_reader.cc
    if (page.definition_level_encoding() != parquet::Encoding::RLE && col_descriptor.max_definition_level() != 0)
    {
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Unsupported encoding: {}", page.definition_level_encoding());
    }

    const auto * buffer =  page.data();
    auto max_size = static_cast<std::size_t>(page.size());

    if (col_descriptor.max_repetition_level() > 0)
    {
        if (max_size < sizeof(int32_t))
        {
            throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Not enough bytes in parquet page buffer, corrupt?");
        }

        auto num_bytes = ::arrow::util::SafeLoadAs<int32_t>(buffer);

        if (num_bytes < 0)
        {
            throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Number of bytes for dl is negative, corrupt?");
        }

        if (num_bytes + 4u > max_size)
        {
            throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Not enough bytes in parquet page buffer, corrupt?");
        }

        // not constructing level reader because we are not using it atm
        num_bytes += 4;
        buffer += num_bytes;
        max_size -= num_bytes;
    }

    assert(col_descriptor.max_definition_level() >= 0);
    std::unique_ptr<RleValuesReader> def_level_reader;
    if (col_descriptor.max_definition_level() > 0)
    {
        auto bit_width = arrow::bit_util::Log2(col_descriptor.max_definition_level() + 1);

        if (max_size < sizeof(int32_t))
        {
            throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Not enough bytes in parquet page buffer, corrupt?");
        }

        auto num_bytes = ::arrow::util::SafeLoadAs<int32_t>(buffer);

        if (num_bytes < 0)
        {
            throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Number of bytes for dl is negative, corrupt?");
        }

        if (num_bytes + 4u > max_size)
        {
            throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Not enough bytes in parquet page buffer, corrupt?");
        }

        auto bit_reader = std::make_unique<arrow::bit_util::BitReader>(buffer + 4, num_bytes);
        num_bytes += 4;
        buffer += num_bytes;
        max_size -= num_bytes;
        def_level_reader = std::make_unique<RleValuesReader>(std::move(bit_reader), bit_width);
    }
    else
    {
        def_level_reader = std::make_unique<RleValuesReader>(page.num_values());
    }

    initDataReader(page.encoding(), buffer, max_size, std::move(def_level_reader));
}

/*
 * As far as I understand, the difference between page v1 and page v2 lies primarily on the below:
 * 1. repetition and definition levels are not compressed;
 * 2. size of repetition and definition levels is present in the header;
 * 3. the encoding is always RLE
 *
 * Therefore, this method leverages the existing `parquet::LevelDecoder::SetDataV2` method to build the repetition level decoder.
 * The data buffer is "offset-ed" by rl bytes length and then dl decoder is built using RLE decoder. Since dl bytes length was present in the header,
 * there is no need to read it and apply an offset like in page v1.
 * */
template <typename TColumn>
void ParquetLeafColReader<TColumn>::readPageV2(const parquet::DataPageV2 & page)
{
    cur_page_values = page.num_values();

    const auto * buffer =  page.data();

    if (page.repetition_levels_byte_length() < 0 || page.definition_levels_byte_length() < 0)
    {
        throw Exception(
            ErrorCodes::PARQUET_EXCEPTION, "Either RL or DL is negative, this should not happen. Most likely corrupt file or parsing issue");
    }

    const int64_t total_levels_length =
        static_cast<int64_t>(page.repetition_levels_byte_length()) +
        page.definition_levels_byte_length();

    if (total_levels_length > page.size())
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Data page too small for levels (corrupt header?)");
    }

    // ARROW-17453: Even if max_rep_level_ is 0, there may still be
    // repetition level bytes written and/or reported in the header by
    // some writers (e.g. Athena)
    buffer += page.repetition_levels_byte_length();

    assert(col_descriptor.max_definition_level() >= 0);
    std::unique_ptr<RleValuesReader> def_level_reader;
    if (col_descriptor.max_definition_level() > 0)
    {
        auto bit_width = arrow::bit_util::Log2(col_descriptor.max_definition_level() + 1);
        auto num_bytes = page.definition_levels_byte_length();
        auto bit_reader = std::make_unique<arrow::bit_util::BitReader>(buffer, num_bytes);
        def_level_reader = std::make_unique<RleValuesReader>(std::move(bit_reader), bit_width);
    }
    else
    {
        def_level_reader = std::make_unique<RleValuesReader>(page.num_values());
    }

    buffer += page.definition_levels_byte_length();

    initDataReader(page.encoding(), buffer, page.size() - total_levels_length, std::move(def_level_reader));
}

template <typename TColumn>
std::unique_ptr<ParquetDataValuesReader> ParquetLeafColReader<TColumn>::createDictReader(
    std::unique_ptr<RleValuesReader> def_level_reader, std::unique_ptr<RleValuesReader> rle_data_reader)
{
    if (reading_low_cardinality && std::same_as<TColumn, ColumnString>)
    {
        std::unique_ptr<ParquetDataValuesReader> res;
        visitColStrIndexType(dictionary->size(), [&]<typename TCol>(TCol *)
        {
            res = std::make_unique<ParquetRleLCReader<TCol>>(
                col_descriptor.max_definition_level(),
                std::move(def_level_reader),
                std::move(rle_data_reader));
        });
        return res;
    }
    return std::make_unique<ParquetRleDictReader<TColumn>>(
        col_descriptor.max_definition_level(),
        std::move(def_level_reader),
        std::move(rle_data_reader),
        *assert_cast<const TColumn *>(dictionary.get()));
}


template class ParquetLeafColReader<ColumnInt32>;
template class ParquetLeafColReader<ColumnUInt32>;
template class ParquetLeafColReader<ColumnInt64>;
template class ParquetLeafColReader<ColumnUInt64>;
template class ParquetLeafColReader<ColumnFloat32>;
template class ParquetLeafColReader<ColumnFloat64>;
template class ParquetLeafColReader<ColumnString>;
template class ParquetLeafColReader<ColumnDecimal<Decimal32>>;
template class ParquetLeafColReader<ColumnDecimal<Decimal64>>;
template class ParquetLeafColReader<ColumnDecimal<Decimal128>>;
template class ParquetLeafColReader<ColumnDecimal<Decimal256>>;
template class ParquetLeafColReader<ColumnDecimal<DateTime64>>;

}
