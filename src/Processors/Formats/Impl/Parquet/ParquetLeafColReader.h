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

    ColumnWithTypeAndName readBatch(UInt64 rows_num, const String & name) override;

private:
    const parquet::ColumnDescriptor & col_descriptor;
    DataTypePtr base_data_type;
    std::unique_ptr<parquet::ColumnChunkMetaData> col_chunk_meta;
    std::unique_ptr<parquet::PageReader> parquet_page_reader;
    std::unique_ptr<ParquetDataValuesReader> data_values_reader;

    MutableColumnPtr column;
    std::unique_ptr<LazyNullMap> null_map;

    ColumnPtr dictionary;

    UInt64 reading_rows_num = 0;
    UInt32 cur_page_values = 0;
    bool reading_low_cardinality = false;

    Poco::Logger * log;

    void resetColumn(UInt64 rows_num);
    void degradeDictionary();
    ColumnWithTypeAndName releaseColumn(const String & name);

    void readPage();
    void readPageV1(const parquet::DataPageV1 & page);
    void readPageV2(const parquet::DataPageV2 & page);
    void initDataReader(parquet::Encoding::type enconding_type,
                        const uint8_t * buffer,
                        std::size_t max_size,
                        std::unique_ptr<RleValuesReader> && def_level_reader);

    std::unique_ptr<ParquetDataValuesReader> createDictReader(
        std::unique_ptr<RleValuesReader> def_level_reader, std::unique_ptr<RleValuesReader> rle_data_reader);
};

}
