#pragma once
#include "config.h"
#if USE_PARQUET

#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Formats/FormatSettings.h>
#include <parquet/metadata.h>

namespace parquet::arrow { class FileReader; }

namespace arrow { class Buffer; class RecordBatchReader;}

namespace DB
{

/* Special format that always returns just one row with Parquet file metadata (see https://parquet.apache.org/docs/file-format/metadata/).
 * The result row have the next structure:
 * num_columns - the number of columns
 * num_rows - the total number of rows
 * num_row_groups - the total number of row groups
 * format_version - parquet format version, always 1.0 or 2.6
 * total_uncompressed_size - total bytes size of the data, calculated as the sum of total_uncompressed_size from all row groups
 * total_compressed_size - total compressed bytes size of the data, calculated as the sum of total_compressed_size from all row groups
 * columns - the list of columns metadata with the next structure:
 *     name - column name
 *     path - column path (differs from name for nested column)
 *     max_definition_level - maximum definition level
 *     max_repetition_level - maximum repetition level
 *     physical_type - column physical type
 *     logical_type - column logical type
 *     compression - compression used for this column
 *     total_compressed_size - total compressed bytes size of the column, calculated as the sum of total_uncompressed_size of the column from all row groups
 *     total_uncompressed_size - total uncompressed bytes size of the column, calculated as the sum of total_compressed_size of the column from all row groups
 *     space_saved - percent of space saved by compression, calculated as (1 - total_compressed_size/total_uncompressed_size).
 *     encodings - the list of encodings used for this column
 * row_groups - the list of row groups metadata with the next structure:
 *     num_columns - the number of columns in the row group
 *     num_rows - the number of rows in the row group
 *     total_uncompressed_size - total bytes size of the row group
 *     total_compressed_size - total compressed bytes size of the row group
 *     columns - the list of column chunks metadata with the next structure:
 *         name - column name
 *         path - column path
 *         total_compressed_size - total compressed bytes size of the column in the row group
 *         total_uncompressed_size - total uncompressed bytes size of the column in the row group
 *         have_statistics - bool flag that indicates if column chunk metadata contains column statistics
 *         statistics - column chunk statistics (all fields are NULL if have_statistics = false) with the next structure:
 *             num_values - the number of non-null values in the column chunk
 *             null_count - the number of NULL values in the column chunk
 *             distinct_count - the number pf distinct values in the column chunk
 *             min - the minimum value of the column chunk
 *             max - the maximum column of the column chunk
 * */

class ParquetMetadataInputFormat : public IInputFormat
{
public:
    ParquetMetadataInputFormat(ReadBuffer & in_, Block header_, const FormatSettings & format_settings_);

    String getName() const override { return "ParquetMetadataInputFormat"; }

    void resetParser() override;

private:
    Chunk read() override;

    void onCancel() noexcept override
    {
        is_stopped = 1;
    }

    void fillColumnsMetadata(const std::shared_ptr<parquet::FileMetaData> & metadata, MutableColumnPtr & column);
    void fillRowGroupsMetadata(const std::shared_ptr<parquet::FileMetaData> & metadata, MutableColumnPtr & column);
    void fillColumnChunksMetadata(const std::unique_ptr<parquet::RowGroupMetaData> & row_group_metadata, IColumn & column);
    void fillColumnStatistics(const std::shared_ptr<parquet::Statistics> & statistics, IColumn & column, int32_t type_length);

    const FormatSettings format_settings;
    bool done = false;
    std::atomic<int> is_stopped{0};
};

class ParquetMetadataSchemaReader : public ISchemaReader
{
public:
    explicit ParquetMetadataSchemaReader(ReadBuffer & in_);

    NamesAndTypesList readSchema() override;
};

}

#endif
