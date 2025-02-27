#pragma once

#include <bit>
#include <iostream>
#include <vector>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Processors/Chunk.h>
#include <Processors/Formats/Impl/Parquet/ColumnFilter.h>
#include <Processors/Formats/Impl/Parquet/DecoderHelper.h>
#include <Processors/Formats/Impl/Parquet/PageReader.h>
#include <arrow/util/bit_stream_utils.h>
#include <arrow/util/decimal.h>
#include <arrow/util/rle_encoding.h>
#include <parquet/column_page.h>
#include <parquet/column_reader.h>
#include <parquet/file_reader.h>
#include <parquet/page_index.h>
#include <Common/PODArray.h>

namespace ErrorCodes
{
extern const int PARQUET_EXCEPTION;
extern const int LOGICAL_ERROR;
}

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

    // next page position in the column chunk
    Int32 page_position = 0;

    Int32 getCurrentPagePosition()
    {
        if (offsets.remain_rows)
            return std::max(page_position - 1, 0);
        else
            return page_position;
    }
};

Int32 loadLength(const uint8_t * data);


template <typename T>
void computeRowSetPlainSpace(
    const T * start, OptionalRowSet & row_set, const ColumnFilterPtr & filter, PaddedPODArray<UInt8> & null_map, size_t rows_to_read)
{
    if (!filter || !row_set.has_value())
        return;
    int count = 0;
    auto & sets = row_set.value();
    for (size_t i = 0; i < rows_to_read; i++)
    {
        if (null_map[i])
        {
            sets.set(i, filter->testNull());
        }
        else
        {
            if constexpr (std::is_same_v<T, Int64>)
                sets.set(i, filter->testInt64(start[count]));
            else if constexpr (std::is_same_v<T, Int32>)
                sets.set(i, filter->testInt32(start[count]));
            else if constexpr (std::is_same_v<T, Int16>)
                sets.set(i, filter->testInt16(start[count]));
            else if constexpr (std::is_same_v<T, Float32>)
                sets.set(i, filter->testFloat32(start[count]));
            else if constexpr (std::is_same_v<T, Float64>)
                sets.set(i, filter->testFloat64(start[count]));
            else
                throw Exception(ErrorCodes::PARQUET_EXCEPTION, "unsupported type");
            count++;
        }
    }
}

template <typename T>
void computeRowSetPlain(const T * start, OptionalRowSet & row_set, const ColumnFilterPtr & filter, size_t rows_to_read)
{
    if (filter && row_set.has_value())
    {
        if constexpr (std::is_same_v<T, Int64>)
            filter->testInt64Values(row_set.value(), rows_to_read, start);
        else if constexpr (std::is_same_v<T, Int32>)
            filter->testInt32Values(row_set.value(), rows_to_read, start);
        else if constexpr (std::is_same_v<T, Int16>)
            filter->testInt16Values(row_set.value(), rows_to_read, start);
        else if constexpr (std::is_same_v<T, Float32>)
            filter->testFloat32Values(row_set.value(), rows_to_read, start);
        else if constexpr (std::is_same_v<T, Float64>)
            filter->testFloat64Values(row_set.value(), rows_to_read, start);
        else
            throw Exception(ErrorCodes::PARQUET_EXCEPTION, "unsupported type");
    }
}

class SelectiveColumnReader
{
    friend class OptionalColumnReader;
    friend class ListColumnReader;
    friend class MapColumnReader;

public:
    SelectiveColumnReader(PageReaderCreator page_reader_creator_, const ScanSpec & scan_spec_)
        : page_reader_creator(page_reader_creator_)
        , scan_spec(scan_spec_)
    {
    }
    virtual ~SelectiveColumnReader() = default;
    void initPageReaderIfNeed()
    {
        if (!page_reader)
        {
            if (!page_reader_creator)
            {
                throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Page reader creator is not set");
            }
            page_reader = page_reader_creator();
        }
    }

    virtual String getName() = 0;

    virtual const std::unordered_set<ColumnFilterKind> & supportedFilterKinds() const = 0;
    /// calculate row mask on decompression buffer
    virtual void computeRowSet(std::optional<RowSet> & row_set, size_t rows_to_read) = 0;
    /// calculate row mask on decompression buffer with null map
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
        loadDictPageIfNeeded();
        readPageIfNeeded();
        if (!page_reader)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Page exhausted");
        }
        decodePage();
    }
    virtual DataTypePtr getResultType() = 0;
    /// create empty result column
    virtual MutableColumnPtr createColumn() = 0;
    /// get all definition levels of current page
    virtual const PaddedPODArray<Int16> & getDefinitionLevels()
    {
        readAndDecodePage();
        return state.def_levels;
    }
    /// get all repetition levels of current page
    virtual const PaddedPODArray<Int16> & getRepetitionLevels()
    {
        readAndDecodePage();
        return state.rep_levels;
    }
    /// levels offset in current page
    virtual size_t levelsOffset() const { return state.offsets.levels_offset; }
    virtual size_t availableRows() const { return std::max(state.offsets.remain_rows - state.lazy_skip_rows, 0UL); }
    /// for nested type, 因为不同的列的page大小不同，需要获得当前reader最小可用的level数
    virtual size_t minimumAvailableLevels() { return getRepetitionLevels().size() - state.offsets.levels_offset; }

    /// skip n rows null value
    void skipNulls(size_t rows_to_skip);

    /// ignore n rows, for nested type. for example, an empty list will take up one row.
    virtual void advance(size_t rows, bool) { state.offsets.consume(rows); }
    /// skip n rows, only nested type need override.
    virtual void skip(size_t rows);

    /// skip values in current page, return the number of rows need to lazy skip
    virtual size_t skipValuesInCurrentPage(size_t /*rows_to_skip*/)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Skip values in current page is not implemented");
    }

    virtual int16_t maxDefinitionLevel() const { return scan_spec.column_desc->max_definition_level(); }

    virtual int16_t maxRepetitionLevel() const { return scan_spec.column_desc->max_repetition_level(); }

    virtual bool isLeafReader() const { return true; }

    virtual void setParent(SelectiveColumnReader * parent_)
    {
        if (parent_)
        {
            parent = parent_;
            parent_rl = parent->maxRepetitionLevel();
            parent_dl = parent->maxDefinitionLevel();
        }
    }

    void setColumnChunkMeta(std::unique_ptr<parquet::ColumnChunkMetaData> column_chunk_meta_)
    {
        column_chunk_meta = std::move(column_chunk_meta_);
    }

    virtual void setColumnIndex(std::shared_ptr<parquet::ColumnIndex> column_index_) { column_index = std::move(column_index_); }

    virtual void setOffsetIndex(std::shared_ptr<parquet::OffsetIndex> offset_index_) { offset_index = std::move(offset_index_); }

protected:
    void decodePage();
    void initDataPageDecoder(parquet::Encoding::type encoding);
    virtual void skipPageIfNeed();
    bool readPage();
    void readDataPageV1(const parquet::DataPageV1 & page);
    void readDataPageV2(const parquet::DataPageV2 & page);

    // for dictionary reader
    void loadDictPageIfNeeded();
    virtual bool needReadDictPage() { return false; }
    virtual void readDictPage(const parquet::DictionaryPage &) { }
    virtual void initIndexDecoderIfNeeded() { }
    virtual void createDictDecoder() { }
    virtual void downgradeToPlain() { }

    PageReaderCreator page_reader_creator;
    std::unique_ptr<LazyPageReader> page_reader;
    ScanState state;
    ScanSpec scan_spec;
    std::unique_ptr<parquet::ColumnChunkMetaData> column_chunk_meta;
    std::unique_ptr<PlainDecoder> plain_decoder;
    bool plain = true;
    SelectiveColumnReader * parent = nullptr;
    int16_t parent_rl = 0;
    int16_t parent_dl = 0;

    std::shared_ptr<parquet::ColumnIndex> column_index;
    std::shared_ptr<parquet::OffsetIndex> offset_index;
};

class BooleanColumnReader : public SelectiveColumnReader
{
public:
    BooleanColumnReader(PageReaderCreator page_reader_creator_, ScanSpec scan_spec_);
    ~BooleanColumnReader() override = default;
    String getName() override { return "BooleanColumnReader"; }
    const std::unordered_set<ColumnFilterKind> & supportedFilterKinds() const override
    {
        static const std::unordered_set<ColumnFilterKind> kinds = {};
        return kinds;
    }
    DataTypePtr getResultType() override { return std::make_shared<DataTypeUInt8>(); }
    MutableColumnPtr createColumn() override;
    void computeRowSet(OptionalRowSet & row_set, size_t rows_to_read) override;
    void computeRowSetSpace(OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t rows_to_read) override;
    void read(MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read) override;
    void
    readSpace(MutableColumnPtr & column, OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t rows_to_read)
        override;
    size_t skipValuesInCurrentPage(size_t rows_to_skip) override;

private:
    void initBitReader();

    PaddedPODArray<UInt8> buffer;
    std::unique_ptr<arrow::bit_util::BitReader> bit_reader;
};

template <typename SerializedType>
struct IndexTypeTraits
{
};

template <>
struct IndexTypeTraits<Int32>
{
    using IndexType = parquet::Int32ColumnIndex;
};

template <>
struct IndexTypeTraits<Int64>
{
    using IndexType = parquet::Int64ColumnIndex;
};

template <>
struct IndexTypeTraits<Float32>
{
    using IndexType = parquet::FloatColumnIndex;
};

template <>
struct IndexTypeTraits<Float64>
{
    using IndexType = parquet::DoubleColumnIndex;
};


template <typename DataType, typename SerializedType>
class NumberColumnDirectReader : public SelectiveColumnReader
{
public:
    NumberColumnDirectReader(PageReaderCreator page_reader_creator_, ScanSpec scan_spec_, DataTypePtr datatype_);
    ~NumberColumnDirectReader() override = default;
    String getName() override { return "NumberColumnDirectReader"; }
    const std::unordered_set<ColumnFilterKind> & supportedFilterKinds() const override
    {
        static const std::unordered_set<ColumnFilterKind> kinds
            = {ColumnFilterKind::BigIntMultiRange,
               ColumnFilterKind::BigIntValuesUsingBitmask,
               ColumnFilterKind::BigIntValuesUsingHashTable,
               ColumnFilterKind::BigIntRange,
               ColumnFilterKind::NegatedBigIntValuesUsingHashTable,
               ColumnFilterKind::NegatedBigIntValuesUsingBitmask,
               ColumnFilterKind::NegatedBigIntRange,
               ColumnFilterKind::AlwaysTrue,
               ColumnFilterKind::AlwaysFalse,
               ColumnFilterKind::IsNull,
               ColumnFilterKind::IsNotNull};
        return kinds;
    }
    DataTypePtr getResultType() override { return datatype; }
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
    String getName() override { return "NumberDictionaryReader"; }
    const std::unordered_set<ColumnFilterKind> & supportedFilterKinds() const override
    {
        static const std::unordered_set<ColumnFilterKind> kinds
            = {ColumnFilterKind::BigIntMultiRange,
               ColumnFilterKind::BigIntValuesUsingBitmask,
               ColumnFilterKind::BigIntValuesUsingHashTable,
               ColumnFilterKind::BigIntRange,
               ColumnFilterKind::NegatedBigIntValuesUsingHashTable,
               ColumnFilterKind::NegatedBigIntValuesUsingBitmask,
               ColumnFilterKind::NegatedBigIntRange,
               ColumnFilterKind::AlwaysTrue,
               ColumnFilterKind::AlwaysFalse,
               ColumnFilterKind::IsNull,
               ColumnFilterKind::IsNotNull};
        return kinds;
    }
    void computeRowSet(OptionalRowSet & row_set, size_t rows_to_read) override;
    void computeRowSetSpace(OptionalRowSet & set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t rows_to_read) override;
    void read(MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read) override;
    void readSpace(MutableColumnPtr & ptr, OptionalRowSet & set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t size) override;
    DataTypePtr getResultType() override { return datatype; }
    MutableColumnPtr createColumn() override { return datatype->createColumn(); }
    size_t skipValuesInCurrentPage(size_t rows_to_skip) override;

protected:
    bool needReadDictPage() override { return !dict_page_read; }
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
    bool dict_page_read = false;
    PaddedPODArray<typename DataType::FieldType> dict;
    PaddedPODArray<typename DataType::FieldType> batch_buffer;
};

template <typename DataType>
class FixedLengthColumnDirectReader : public SelectiveColumnReader
{
public:
    FixedLengthColumnDirectReader(PageReaderCreator page_reader_creator_, ScanSpec scan_spec_, DataTypePtr datatype_);
    String getName() override { return "FixedLengthColumnDirectReader"; }
    void computeRowSet(std::optional<RowSet> & row_set, size_t rows_to_read) override;
    void read(MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read) override;
    const std::unordered_set<ColumnFilterKind> & supportedFilterKinds() const override
    {
        static const std::unordered_set<ColumnFilterKind> kinds = {};
        return kinds;
    }
    void
    readSpace(MutableColumnPtr & column, OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t rows_to_read)
        override;
    DataTypePtr getResultType() override { return data_type; }
    MutableColumnPtr createColumn() override { return data_type->createColumn(); }
    size_t skipValuesInCurrentPage(size_t rows_to_skip) override;
    ValueConverter getConverter();

private:
    size_t element_size = 0;
    DataTypePtr data_type;
};

template <typename DataType, typename DictValueType>
class FixedLengthColumnDictionaryReader : public SelectiveColumnReader
{
public:
    FixedLengthColumnDictionaryReader(PageReaderCreator page_reader_creator_, ScanSpec scan_spec_, DataTypePtr datatype_);
    String getName() override { return "FixedLengthColumnDictionaryReader"; }
    const std::unordered_set<ColumnFilterKind> & supportedFilterKinds() const override
    {
        static const std::unordered_set<ColumnFilterKind> kinds = {};
        return kinds;
    }
    void computeRowSet(OptionalRowSet & row_set, size_t rows_to_read) override;
    void read(MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read) override;
    void readSpace(MutableColumnPtr & ptr, OptionalRowSet & set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t size) override;
    DataTypePtr getResultType() override { return data_type; }
    MutableColumnPtr createColumn() override { return data_type->createColumn(); }
    size_t skipValuesInCurrentPage(size_t rows_to_skip) override;
    ValueConverter getConverter();

protected:
    bool needReadDictPage() override { return !dict_page_read; }
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
    // mark dict page has read
    bool dict_page_read = false;
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
    String getName() override { return "StringDirectReader"; }
    const std::unordered_set<ColumnFilterKind> & supportedFilterKinds() const override
    {
        static const std::unordered_set<ColumnFilterKind> kinds
            = {ColumnFilterKind::BytesValues,
               ColumnFilterKind::BytesRange,
               ColumnFilterKind::NegatedBytesValues,
               ColumnFilterKind::NegatedBytesRange,
               ColumnFilterKind::AlwaysTrue,
               ColumnFilterKind::AlwaysFalse,
               ColumnFilterKind::IsNull,
               ColumnFilterKind::IsNotNull};
        return kinds;
    }
    void computeRowSet(OptionalRowSet & row_set, size_t rows_to_read) override;
    void
    computeRowSetSpace(OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t /*null_count*/, size_t rows_to_read) override;
    void read(MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read) override;

    void
    readSpace(MutableColumnPtr & column, OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t rows_to_read)
        override;

    size_t skipValuesInCurrentPage(size_t rows_to_skip) override;

    DataTypePtr getResultType() override;
    MutableColumnPtr createColumn() override { return ColumnString::create(); }
};

class StringDictionaryReader : public SelectiveColumnReader
{
public:
    StringDictionaryReader(PageReaderCreator page_reader_creator_, const ScanSpec & scan_spec_)
        : SelectiveColumnReader(std::move(page_reader_creator_), scan_spec_)
    {
    }
    String getName() override { return "StringDictionaryReader"; }
    const std::unordered_set<ColumnFilterKind> & supportedFilterKinds() const override
    {
        static const std::unordered_set<ColumnFilterKind> kinds
            = {ColumnFilterKind::BytesValues,
               ColumnFilterKind::BytesRange,
               ColumnFilterKind::NegatedBytesValues,
               ColumnFilterKind::NegatedBytesRange,
               ColumnFilterKind::AlwaysTrue,
               ColumnFilterKind::AlwaysFalse,
               ColumnFilterKind::IsNull,
               ColumnFilterKind::IsNotNull};
        return kinds;
    }
    DataTypePtr getResultType() override;

    MutableColumnPtr createColumn() override { return ColumnString::create(); }

    void computeRowSet(OptionalRowSet & row_set, size_t rows_to_read) override;

    void computeRowSetSpace(OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t rows_to_read) override;

    void read(MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read) override;

    void
    readSpace(MutableColumnPtr & column, OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t rows_to_read)
        override;

    size_t skipValuesInCurrentPage(size_t rows_to_skip) override;

protected:
    bool needReadDictPage() override { return !dict_page_read; }

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
    bool dict_page_read = false;
};

class OptionalColumnReader : public SelectiveColumnReader
{
public:
    OptionalColumnReader(const ScanSpec & scanSpec, const SelectiveColumnReaderPtr child_, bool has_null_)
        : SelectiveColumnReader(nullptr, scanSpec)
        , child(child_)
        , has_null(has_null_)
    {
        def_level = child->maxDefinitionLevel();
        rep_level = child->maxRepetitionLevel();
    }

    ~OptionalColumnReader() override = default;
    String getName() override { return fmt::format("Optional({})", child->getName()); }
    const std::unordered_set<ColumnFilterKind> & supportedFilterKinds() const override { return child->supportedFilterKinds(); }
    const PaddedPODArray<Int16> & getDefinitionLevels() override;
    const PaddedPODArray<Int16> & getRepetitionLevels() override;
    void readPageIfNeeded() override { child->readPageIfNeeded(); }
    DataTypePtr getResultType() override;
    MutableColumnPtr createColumn() override;
    void computeRowSet(OptionalRowSet & row_set, size_t rows_to_read) override;
    void read(MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read) override;
    size_t skipValuesInCurrentPage(size_t rows_to_skip) override;
    int16_t maxDefinitionLevel() const override { return child->maxDefinitionLevel(); }
    int16_t maxRepetitionLevel() const override { return child->maxRepetitionLevel(); }
    size_t availableRows() const override;
    size_t levelsOffset() const override;
    void advance(size_t rows, bool force) override { child->advance(rows, force); }
    size_t minimumAvailableLevels() override { return child->minimumAvailableLevels(); }
    void setParent(SelectiveColumnReader * parent_) override
    {
        SelectiveColumnReader::setParent(parent_);
        child->setParent(this);
    }
    void setColumnIndex(std::shared_ptr<parquet::ColumnIndex> column_index_) override { child->setColumnIndex(column_index_); }

    void setOffsetIndex(std::shared_ptr<parquet::OffsetIndex> offset_index_) override { child->setOffsetIndex(offset_index_); }

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

    /// There are two scenarios in which an `OptionalColumnReader` is created:
    /// 1. The data within the Parquet file is optional.
    /// 2. There are nullable fields in the nested types of the header, but the data is required. In this case, additional handling is needed because Repeat Level (RL) data does not exist.
    const bool has_null;
};

class ListColumnReader : public SelectiveColumnReader
{
public:
    struct ListState
    {
        NullMap * null_map;
        IColumn::Offsets & offsets;
        MutableColumns columns;
    };
    ListColumnReader(int16_t rep_level_, int16_t def_level_, const SelectiveColumnReaderPtr child_)
        : SelectiveColumnReader(nullptr, ScanSpec{})
        , children({child_})
        , def_level(def_level_)
        , rep_level(rep_level_)
    {
        for (auto & child : children)
            child->setParent(this);
    }

protected:
    // for map reader
    ListColumnReader(int16_t rep_level_, int16_t def_level_, const std::vector<SelectiveColumnReaderPtr> & children_)
        : SelectiveColumnReader(nullptr, ScanSpec{})
        , children({children_})
        , def_level(def_level_)
        , rep_level(rep_level_)
    {
        for (auto & child : children)
            child->setParent(this);
    }

public:
    ~ListColumnReader() override = default;
    String getName() override { return fmt::format("List({})", children.front()->getName()); }
    const std::unordered_set<ColumnFilterKind> & supportedFilterKinds() const override
    {
        static const std::unordered_set<ColumnFilterKind> kinds = {};
        return kinds;
    }
    void read(MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read) override;

    const PaddedPODArray<Int16> & getDefinitionLevels() override;
    const PaddedPODArray<Int16> & getRepetitionLevels() override;
    int16_t maxDefinitionLevel() const override { return def_level; }
    int16_t maxRepetitionLevel() const override { return rep_level; }
    void computeRowSet(std::optional<RowSet> & row_set, size_t rows_to_read) override;
    DataTypePtr getResultType() override;
    MutableColumnPtr createColumn() override;
    void skip(size_t rows) override;
    size_t availableRows() const override;
    size_t levelsOffset() const override;
    void advance(size_t rows, bool force) override { children.front()->advance(rows, force); }
    bool isLeafReader() const override { return false; }
    size_t minimumAvailableLevels() override { return children.front()->minimumAvailableLevels(); }
    virtual ListState getListState(MutableColumnPtr & column);

protected:
    // only have one reader
    std::vector<SelectiveColumnReaderPtr> children;
    int16_t def_level = 0;
    int16_t rep_level = 0;
};

class MapColumnReader : public ListColumnReader
{
public:
    MapColumnReader(int16_t rep_level_, int16_t def_level_, const SelectiveColumnReaderPtr key_, const SelectiveColumnReaderPtr value_)
        : ListColumnReader(rep_level_, def_level_, {key_, value_})
    {
    }
    ~MapColumnReader() override = default;
    String getName() override { return "MapColumnReader"; }
    DataTypePtr getResultType() override;
    MutableColumnPtr createColumn() override;
    void advance(size_t rows, bool force) override;
    size_t minimumAvailableLevels() override;
    ListState getListState(MutableColumnPtr & column) override;
};

class StructColumnReader : public SelectiveColumnReader
{
public:
    StructColumnReader(
        int16_t rep_level_,
        int16_t def_level_,
        const std::unordered_map<String, SelectiveColumnReaderPtr> & children_,
        DataTypePtr structType_)
        : SelectiveColumnReader(nullptr, ScanSpec{})
        , rep_level(rep_level_)
        , def_level(def_level_)
        , children(children_)
        , structType(structType_)
    {
        for (auto & child : children)
            child.second->setParent(this);
    }
    ~StructColumnReader() override = default;
    String getName() override { return "StructColumnReader"; }
    const std::unordered_set<ColumnFilterKind> & supportedFilterKinds() const override
    {
        static const std::unordered_set<ColumnFilterKind> kinds = {};
        return kinds;
    }
    void read(MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read) override;
    void computeRowSet(OptionalRowSet & row_set, size_t rows_to_read) override;
    MutableColumnPtr createColumn() override;
    DataTypePtr getResultType() override;
    void skip(size_t rows) override;
    size_t availableRows() const override;
    size_t levelsOffset() const override;
    void advance(size_t rows, bool force) override;
    size_t minimumAvailableLevels() override;
    bool isLeafReader() const override { return true; }
    int16_t maxDefinitionLevel() const override { return def_level; }
    int16_t maxRepetitionLevel() const override { return rep_level; }

    const PaddedPODArray<Int16> & getDefinitionLevels() override;
    const PaddedPODArray<Int16> & getRepetitionLevels() override;

private:
    int16_t rep_level = 0;
    int16_t def_level = 0;
    std::unordered_map<String, SelectiveColumnReaderPtr> children;
    DataTypePtr structType;
};
}
