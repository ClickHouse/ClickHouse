#pragma once
#include "ColumnFilter.h"

#include <iostream>
#include <vector>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeFixedString.h>
#include <Processors/Chunk.h>
#include <Processors/Formats/Impl/Parquet/PageReader.h>
#include <arrow/util/decimal.h>
#include <arrow/util/rle_encoding.h>
#include <parquet/column_page.h>
#include <parquet/column_reader.h>
#include <parquet/file_reader.h>
#include <Common/PODArray.h>

namespace parquet
{
class ColumnDescriptor;
}

namespace DB
{

class SelectiveColumnReader;

using SelectiveColumnReaderPtr = std::shared_ptr<SelectiveColumnReader>;

using PageReaderCreator = std::function<std::unique_ptr<LazyPageReader>()>;

struct ScanSpec
{
    String column_name;
    const parquet::ColumnDescriptor * column_desc = nullptr;
    ColumnFilterPtr filter;
};

class FilterCache
{
public:
    explicit FilterCache(size_t size)
    {
        cache_set.resize(size);
        filter_cache.resize(size);
    }

    inline bool has(size_t size) { return cache_set.test(size); }

    inline bool hasNull() const { return exist_null; }

    inline bool getNull() const { return value_null; }

    inline void setNull(bool value)
    {
        exist_null = true;
        value_null = value;
    }

    inline bool get(size_t size) { return filter_cache.test(size); }

    inline void set(size_t size, bool value)
    {
        cache_set.set(size, true);
        filter_cache.set(size, value);
    }

private:
    boost::dynamic_bitset<> cache_set;
    boost::dynamic_bitset<> filter_cache;
    bool exist_null = false;
    bool value_null = false;
};

struct ParquetData
{
    // raw page data
    const uint8_t * buffer = nullptr;
    // size of raw page data
    size_t buffer_size = 0;

    void checkSize(size_t size) const
    {
        if (size > buffer_size) [[unlikely]]
            throw Exception(ErrorCodes::PARQUET_EXCEPTION, "ParquetData: buffer size is not enough, {} > {}", size, buffer_size);
    }

    // before consume, should check size first
    void consume(size_t size)
    {
        buffer += size;
        buffer_size -= size;
    }

    void checkAndConsume(size_t size)
    {
        checkSize(size);
        consume(size);
    }
};

struct PageOffsets
{
    size_t remain_rows = 0;
    size_t levels_offset = 0;

    void consume(size_t rows)
    {
        if (rows > remain_rows)
        {
            throw Exception(ErrorCodes::PARQUET_EXCEPTION, "read too many rows: {} > {}", rows, remain_rows);
        }
        remain_rows -= rows;
        levels_offset += rows;
    }
};

struct ScanState
{
    std::shared_ptr<parquet::Page> page;
    PaddedPODArray<Int16> def_levels;
    PaddedPODArray<Int16> rep_levels;
    ParquetData data;
    // rows should be skipped before read data
    size_t lazy_skip_rows = 0;

    // for dictionary encoding
    PaddedPODArray<Int32> idx_buffer;
    std::unique_ptr<FilterCache> filter_cache;

    // current column chunk available rows
    PageOffsets offsets;
};

Int32 loadLength(const uint8_t * data);

using ValueConverter = std::function<void(const uint8_t *, uint8_t *)>;

template <typename Type>
struct ValueConverterImpl
{
    static void convert(const uint8_t *, uint8_t *)
    {
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Unsupported type: {}", typeid(Type).name());
    }
};

template <>
struct ValueConverterImpl<Decimal128>
{
    static void convert(const uint8_t * src, uint8_t * dst)
    {
        auto status = arrow::Decimal128::FromBigEndian(src, 16);
        assert(status.ok());
        status.ValueUnsafe().ToBytes(reinterpret_cast<uint8_t *>(dst));
    }
};

template <>
struct ValueConverterImpl<Decimal256>
{
    static void convert(const uint8_t * src, uint8_t * dst)
    {
        auto status = arrow::Decimal256::FromBigEndian(src, 32);
        assert(status.ok());
        status.ValueUnsafe().ToBytes(reinterpret_cast<uint8_t *>(dst));
    }
};


class PlainDecoder
{
public:
    PlainDecoder(ParquetData & data_, PageOffsets & offsets_) : page_data(data_), offsets(offsets_) { }

    template <typename T, typename S>
    void decodeFixedValue(PaddedPODArray<T> & data, const OptionalRowSet & row_set, size_t rows_to_read);

    void decodeFixedString(ColumnFixedString::Chars & data, const OptionalRowSet & row_set, size_t rows_to_read, size_t n);

    template <typename T>
    void
    decodeFixedLengthData(PaddedPODArray<T> & data, const OptionalRowSet & row_set, size_t rows_to_read, ValueConverter value_converter);

    void decodeString(ColumnString::Chars & chars, ColumnString::Offsets & offsets, const OptionalRowSet & row_set, size_t rows_to_read);

    template <typename T, typename S>
    void
    decodeFixedValueSpace(PaddedPODArray<T> & data, const OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t rows_to_read);

    void decodeFixedStringSpace(
        ColumnFixedString::Chars & data, const OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t rows_to_read, size_t n);

    template <typename T>
    void decodeFixedLengthDataSpace(
        PaddedPODArray<T> & data,
        const OptionalRowSet & row_set,
        PaddedPODArray<UInt8> & null_map,
        size_t rows_to_read,
        ValueConverter value_converter);

    void decodeStringSpace(
        ColumnString::Chars & chars,
        ColumnString::Offsets & offsets,
        const OptionalRowSet & row_set,
        PaddedPODArray<UInt8> & null_map,
        size_t rows_to_read);

    size_t calculateStringTotalSize(const ParquetData & data, const OptionalRowSet & row_set, size_t rows_to_read);

    size_t calculateStringTotalSizeSpace(
        const ParquetData & data, const OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t rows_to_read);

private:
    void addOneString(bool null, const ParquetData & data, size_t & offset, const OptionalRowSet & row_set, size_t row, size_t & total_size)
    {
        if (row_set.has_value())
        {
            const auto & sets = row_set.value();
            if (null)
            {
                if (sets.get(row))
                    total_size++;
                return;
            }
            data.checkSize(offset + 4);
            auto len = loadLength(data.buffer + offset);
            offset += 4 + len;
            data.checkSize(offset);
            if (sets.get(row))
                total_size += len + 1;
        }
        else
        {
            if (null)
            {
                total_size++;
                return;
            }
            data.checkSize(offset + 4);
            auto len = loadLength(data.buffer + offset);
            offset += 4 + len;
            data.checkSize(offset);
            total_size += len + 1;
        }
    }

    ParquetData & page_data;
    PageOffsets & offsets;
};

class DictDecoder
{
public:
    DictDecoder(PaddedPODArray<Int32> & idx_buffer_, PageOffsets & offsets_) : idx_buffer(idx_buffer_), offsets(offsets_) { }

    template <class DictValueType>
    void decodeFixedValue(
        PaddedPODArray<DictValueType> & dict, PaddedPODArray<DictValueType> & data, const OptionalRowSet & row_set, size_t rows_to_read);

    void
    decodeFixedString(PaddedPODArray<String> & dict, ColumnFixedString::Chars & chars, const OptionalRowSet & row_set, size_t rows_to_read);

    template <class DictValueType>
    void decodeFixedLengthData(
        PaddedPODArray<DictValueType> & dict, PaddedPODArray<DictValueType> & data, const OptionalRowSet & row_set, size_t rows_to_read);

    void decodeString(
        std::vector<String> & dict,
        ColumnString::Chars & chars,
        ColumnString::Offsets & offsets,
        const OptionalRowSet & row_set,
        size_t rows_to_read);

    template <class DictValueType>
    void decodeFixedValueSpace(
        PaddedPODArray<DictValueType> & dict,
        PaddedPODArray<DictValueType> & data,
        const OptionalRowSet & row_set,
        PaddedPODArray<UInt8> & null_map,
        size_t rows_to_read);

    void decodeStringSpace(
        std::vector<String> & dict,
        ColumnString::Chars & chars,
        ColumnString::Offsets & offsets,
        const OptionalRowSet & row_set,
        PaddedPODArray<UInt8> & null_map,
        size_t rows_to_read);

    void decodeFixedStringSpace(
        PaddedPODArray<String> & dict,
        ColumnFixedString::Chars & chars,
        const OptionalRowSet & row_set,
        PaddedPODArray<UInt8> & null_map,
        size_t rows_to_read);

    template <class DictValueType>
    void decodeFixedLengthDataSpace(
        PaddedPODArray<DictValueType> & dict,
        PaddedPODArray<DictValueType> & data,
        const OptionalRowSet & row_set,
        PaddedPODArray<UInt8> & null_map,
        size_t rows_to_read);

private:
    PaddedPODArray<Int32> & idx_buffer;
    PageOffsets & offsets;
};


class SelectiveColumnReader
{
    friend class OptionalColumnReader;

public:
    SelectiveColumnReader(PageReaderCreator page_reader_creator_, const ScanSpec & scan_spec_)
        : page_reader_creator(std::move(page_reader_creator_)), scan_spec(scan_spec_)
    {
    }
    virtual ~SelectiveColumnReader() = default;
    void initPageReaderIfNeed()
    {
        if (!page_reader)
            page_reader = page_reader_creator();
    }
    /// calculate row mask on decompression buffer
    virtual void computeRowSet(std::optional<RowSet> & row_set, size_t rows_to_read) = 0;
    /// calculate row mask on decompression buffer with null bitmap
    virtual void computeRowSetSpace(
        OptionalRowSet & /* row_set */, PaddedPODArray<UInt8> & /* null_map */, size_t /* null_count */, size_t /* rows_to_read */)
    {
    }
    /// read batch data
    virtual void read(MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read) = 0;
    /// read batch data with nullmap
    virtual void readSpace(
        MutableColumnPtr & /* column */,
        OptionalRowSet & /* row_set */,
        PaddedPODArray<UInt8> & /* null_map */,
        size_t /* null_count */,
        size_t /* rows_to_read */)
    {
    }
    /// init page data
    virtual void readPageIfNeeded();
    /// read next page
    void readAndDecodePage()
    {
        readPageIfNeeded();
        decodePage();
    }
    /// create empty result column
    virtual MutableColumnPtr createColumn() = 0;
    /// get all definition levels of current page
    const PaddedPODArray<Int16> & getDefinitionLevels()
    {
        readAndDecodePage();
        return state.def_levels;
    }
    /// get all repetition levels of current page
    const PaddedPODArray<Int16> & getRepetitionLevels()
    {
        readAndDecodePage();
        return state.rep_levels;
    }
    /// levels offset in current page
    virtual size_t levelsOffset() const { return state.offsets.levels_offset; }
    virtual size_t availableRows() const { return std::max(state.offsets.remain_rows - state.lazy_skip_rows, 0UL); }

    /// skip n rows null value
    void skipNulls(size_t rows_to_skip);
    /// skip n rows
    void skip(size_t rows);

    /// skip values in current page, return the number of rows need to lazy skip
    virtual size_t skipValuesInCurrentPage(size_t rows_to_skip) = 0;

    virtual int16_t maxDefinitionLevel() const { return scan_spec.column_desc->max_definition_level(); }

    virtual int16_t maxRepetitionLevel() const { return scan_spec.column_desc->max_repetition_level(); }

protected:
    void decodePage();
    virtual void skipPageIfNeed();
    bool readPage();
    void readDataPageV1(const parquet::DataPageV1 & page);

    // for dictionary reader
    virtual void readDictPage(const parquet::DictionaryPage &) { }
    virtual void initIndexDecoderIfNeeded() { }
    virtual void createDictDecoder() { }
    virtual void downgradeToPlain() { }

    PageReaderCreator page_reader_creator;
    std::unique_ptr<LazyPageReader> page_reader;
    ScanState state;
    ScanSpec scan_spec;
    std::unique_ptr<PlainDecoder> plain_decoder;
    bool plain = true;
};

template <typename DataType, typename SerializedType>
class NumberColumnDirectReader : public SelectiveColumnReader
{
public:
    NumberColumnDirectReader(PageReaderCreator page_reader_creator_, ScanSpec scan_spec_, DataTypePtr datatype_);
    ~NumberColumnDirectReader() override = default;
    MutableColumnPtr createColumn() override;
    void computeRowSet(OptionalRowSet & row_set, size_t rows_to_read) override;
    void computeRowSetSpace(OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t rows_to_read) override;
    void read(MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read) override;
    void
    readSpace(MutableColumnPtr & column, OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t rows_to_read)
        override;
    size_t skipValuesInCurrentPage(size_t rows_to_skip) override;

private:
    DataTypePtr datatype;
};

template <typename DataType, typename SerializedType>
class NumberDictionaryReader : public SelectiveColumnReader
{
public:
    NumberDictionaryReader(PageReaderCreator page_reader_creator_, ScanSpec scan_spec_, DataTypePtr datatype_);
    ~NumberDictionaryReader() override = default;
    void computeRowSet(OptionalRowSet & row_set, size_t rows_to_read) override;
    void computeRowSetSpace(OptionalRowSet & set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t rows_to_read) override;
    void read(MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read) override;
    void readSpace(MutableColumnPtr & ptr, OptionalRowSet & set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t size) override;
    MutableColumnPtr createColumn() override { return datatype->createColumn(); }
    size_t skipValuesInCurrentPage(size_t rows_to_skip) override;

protected:
    void readDictPage(const parquet::DictionaryPage & page) override;
    void initIndexDecoderIfNeeded() override
    {
        if (dict.empty())
            return;
        uint8_t bit_width = *state.data.buffer;
        state.data.checkSize(1);
        state.data.consume(1);
        idx_decoder = arrow::util::RleDecoder(state.data.buffer, static_cast<int>(state.data.buffer_size), bit_width);
    }
    void nextIdxBatchIfEmpty(size_t rows_to_read);
    void createDictDecoder() override;
    void downgradeToPlain() override;

private:
    DataTypePtr datatype;
    arrow::util::RleDecoder idx_decoder;
    std::unique_ptr<DictDecoder> dict_decoder;
    PaddedPODArray<typename DataType::FieldType> dict;
    PaddedPODArray<typename DataType::FieldType> batch_buffer;
};

template <typename DataType>
class FixedLengthColumnDirectReader : public SelectiveColumnReader
{
public:
    FixedLengthColumnDirectReader(PageReaderCreator page_reader_creator_, ScanSpec scan_spec_, DataTypePtr datatype_);
    void computeRowSet(std::optional<RowSet> & row_set, size_t rows_to_read) override;
    void read(MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read) override;
    void
    readSpace(MutableColumnPtr & column, OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t rows_to_read)
        override;
    MutableColumnPtr createColumn() override { return data_type->createColumn(); }
    size_t skipValuesInCurrentPage(size_t rows_to_skip) override;

private:
    size_t element_size = 0;
    DataTypePtr data_type;
};

template <typename DataType, typename DictValueType>
class FixedLengthColumnDictionaryReader : public SelectiveColumnReader
{
public:
    FixedLengthColumnDictionaryReader(PageReaderCreator page_reader_creator_, ScanSpec scan_spec_, DataTypePtr datatype_);
    void computeRowSet(OptionalRowSet & row_set, size_t rows_to_read) override;
    void read(MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read) override;
    void readSpace(MutableColumnPtr & ptr, OptionalRowSet & set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t size) override;
    MutableColumnPtr createColumn() override { return data_type->createColumn(); }
    size_t skipValuesInCurrentPage(size_t rows_to_skip) override;

protected:
    void readDictPage(const parquet::DictionaryPage & page) override;
    void initIndexDecoderIfNeeded() override
    {
        if (dict.empty())
            return;
        uint8_t bit_width = *state.data.buffer;
        state.data.checkSize(1);
        state.data.consume(1);
        idx_decoder = arrow::util::RleDecoder(state.data.buffer, static_cast<int>(state.data.buffer_size), bit_width);
    }
    void nextIdxBatchIfEmpty(size_t rows_to_read);
    void createDictDecoder() override;
    void downgradeToPlain() override;


private:
    DataTypePtr data_type;
    size_t element_size = 0;
    arrow::util::RleDecoder idx_decoder;
    std::unique_ptr<DictDecoder> dict_decoder;
    PaddedPODArray<DictValueType> dict;
};

void computeRowSetPlainString(const uint8_t * start, OptionalRowSet & row_set, ColumnFilterPtr filter, size_t rows_to_read);


void computeRowSetPlainStringSpace(
    const uint8_t * start, OptionalRowSet & row_set, ColumnFilterPtr filter, size_t rows_to_read, PaddedPODArray<UInt8> & null_map);


class StringDirectReader : public SelectiveColumnReader
{
public:
    StringDirectReader(PageReaderCreator page_reader_creator_, const ScanSpec & scan_spec_)
        : SelectiveColumnReader(std::move(page_reader_creator_), scan_spec_)
    {
    }

    void computeRowSet(OptionalRowSet & row_set, size_t rows_to_read) override;
    void
    computeRowSetSpace(OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t /*null_count*/, size_t rows_to_read) override;
    void read(MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read) override;

    void
    readSpace(MutableColumnPtr & column, OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t rows_to_read)
        override;

    size_t skipValuesInCurrentPage(size_t rows_to_skip) override;

    MutableColumnPtr createColumn() override { return ColumnString::create(); }
};

class StringDictionaryReader : public SelectiveColumnReader
{
public:
    StringDictionaryReader(PageReaderCreator page_reader_creator_, const ScanSpec & scan_spec_)
        : SelectiveColumnReader(std::move(page_reader_creator_), scan_spec_)
    {
    }

    MutableColumnPtr createColumn() override { return ColumnString::create(); }

    void computeRowSet(OptionalRowSet & row_set, size_t rows_to_read) override;

    void computeRowSetSpace(OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t rows_to_read) override;

    void read(MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read) override;

    void
    readSpace(MutableColumnPtr & column, OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t rows_to_read)
        override;

    size_t skipValuesInCurrentPage(size_t rows_to_skip) override;

protected:
    void readDictPage(const parquet::DictionaryPage & page) override;

    void initIndexDecoderIfNeeded() override;

    /// TODO move to DictDecoder
    void nextIdxBatchIfEmpty(size_t rows_to_read);

    void createDictDecoder() override { dict_decoder = std::make_unique<DictDecoder>(state.idx_buffer, state.offsets); }

    void downgradeToPlain() override;

private:
    std::vector<String> dict;
    std::unique_ptr<DictDecoder> dict_decoder;
    arrow::util::RleDecoder idx_decoder;
};

class OptionalColumnReader : public SelectiveColumnReader
{
public:
    OptionalColumnReader(const ScanSpec & scanSpec, const SelectiveColumnReaderPtr child_)
        : SelectiveColumnReader(nullptr, scanSpec), child(child_)
    {
        def_level = child->maxDefinitionLevel();
        rep_level = child->maxRepetitionLevel();
    }

    ~OptionalColumnReader() override = default;

    void readPageIfNeeded() override { child->readPageIfNeeded(); }
    MutableColumnPtr createColumn() override;
    void computeRowSet(OptionalRowSet & row_set, size_t rows_to_read) override;
    void read(MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read) override;
    size_t skipValuesInCurrentPage(size_t rows_to_skip) override;
    int16_t maxDefinitionLevel() const override { return child->maxDefinitionLevel(); }
    int16_t maxRepetitionLevel() const override { return child->maxRepetitionLevel(); }
    size_t availableRows() const override;

private:
    void applyLazySkip();

protected:
    void skipPageIfNeed() override;

private:
    void nextBatchNullMapIfNeeded(size_t rows_to_read);
    void cleanNullMap()
    {
        cur_null_count = 0;
        cur_null_map.resize(0);
    }

    SelectiveColumnReaderPtr child;
    PaddedPODArray<UInt8> cur_null_map;
    size_t cur_null_count = 0;
    int def_level = 0;
    int rep_level = 0;
};

class ListColumnReader : public SelectiveColumnReader
{
public:
    ListColumnReader(int16_t rep_level_, int16_t def_level_, const SelectiveColumnReaderPtr child_)
        : SelectiveColumnReader(nullptr, ScanSpec{}), child(child_), def_level(def_level_), rep_level(rep_level_)
    {
    }

    ~ListColumnReader() override = default;

    void read(MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read) override;

    int16_t maxDefinitionLevel() const override { return def_level; }
    int16_t maxRepetitionLevel() const override { return rep_level; }
    void computeRowSet(std::optional<RowSet> & row_set, size_t rows_to_read) override;
    MutableColumnPtr createColumn() override;
    size_t skipValuesInCurrentPage(size_t rows_to_skip) override;
    size_t availableRows() const override;

private:
    SelectiveColumnReaderPtr child;
    int16_t def_level = 0;
    int16_t rep_level = 0;
};

class MapColumnReader : public SelectiveColumnReader
{
public:
    MapColumnReader(int16_t rep_level_, int16_t def_level_, const SelectiveColumnReaderPtr key_, const SelectiveColumnReaderPtr value_)
        : SelectiveColumnReader(nullptr, ScanSpec{}), key_reader(key_), value_reader(value_), def_level(def_level_), rep_level(rep_level_)
    {
    }

    ~MapColumnReader() override = default;

    void read(MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read) override;

    int16_t maxDefinitionLevel() const override { return def_level; }
    int16_t maxRepetitionLevel() const override { return rep_level; }
    void computeRowSet(std::optional<RowSet> & row_set, size_t rows_to_read) override;
    MutableColumnPtr createColumn() override;
    size_t skipValuesInCurrentPage(size_t rows_to_skip) override;
    size_t availableRows() const override;

private:
    SelectiveColumnReaderPtr key_reader;
    SelectiveColumnReaderPtr value_reader;
    int16_t def_level = 0;
    int16_t rep_level = 0;
};

class StructColumnReader : public SelectiveColumnReader
{
public:
    StructColumnReader(const std::unordered_map<String, SelectiveColumnReaderPtr> & children_, DataTypePtr structType_)
        : SelectiveColumnReader(nullptr, ScanSpec{}), children(children_), structType(structType_)
    {
    }
    ~StructColumnReader() override = default;
    void read(MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read) override;
    void computeRowSet(OptionalRowSet & row_set, size_t rows_to_read) override;
    MutableColumnPtr createColumn() override;
    size_t skipValuesInCurrentPage(size_t rows_to_skip) override;
    size_t availableRows() const override;

private:
    std::unordered_map<String, SelectiveColumnReaderPtr> children;
    DataTypePtr structType;
};
}
