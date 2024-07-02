#pragma once

#include <Columns/IColumn.h>
#include <DataTypes/Serializations/ISerialization.h>

#include "ParquetColumnReader.h"
#include "ParquetDataValuesReader.h"

namespace parquet
{

class ColumnDescriptor;

}


namespace DB
{

template <typename TColumn>
class ParquetLeafColReader : public ParquetColumnReader
{
public:
    ParquetLeafColReader(
        const parquet::ColumnDescriptor & col_descriptor_,
        DataTypePtr base_type_,
        std::unique_ptr<parquet::ColumnChunkMetaData> meta_,
        std::unique_ptr<parquet::PageReader> reader_);

    ColumnWithTypeAndName readBatch(UInt64 rows_num, const String & name, const IColumn::Filter * filter) override;
    void skip(size_t num_values) override;

private:
    const parquet::ColumnDescriptor & col_descriptor;
    DataTypePtr base_data_type;
    std::unique_ptr<parquet::ColumnChunkMetaData> col_chunk_meta;
    std::unique_ptr<parquet::PageReader> parquet_page_reader;
    std::unique_ptr<ParquetDataValuesReader> data_values_reader;

    MutableColumnPtr column;
    std::unique_ptr<LazyNullMap> null_map;

    ColumnPtr dictionary;

    /// Total number of values to read in one batch
    size_t batch_value_count = 0;
    size_t cur_value_num = 0;
    size_t num_values_remaining_in_page = 0;
    bool reading_low_cardinality = false;

    Poco::Logger * log;

    void resetColumn(UInt64 rows_num);
    void resetColumn();

    void degradeDictionary();
    ColumnWithTypeAndName releaseColumn(const String & name);

    void nextDataPage();
    void readPage(const parquet::Page & page);
    void readPageV1(const parquet::DataPageV1 & page);
    void readPageV2(const parquet::DataPageV2 & page);
    void readPageDict(const parquet::DictionaryPage & page);

    std::unique_ptr<ParquetDataValuesReader> createDictReader(
        std::unique_ptr<RleValuesReader> def_level_reader, std::unique_ptr<RleValuesReader> rle_data_reader);
};

}
