#pragma once
#include <Processors/Formats/Impl/JSONColumnsBlockOutputFormat.h>

namespace DB
{

/* Format JSONColumnsWithMetadata outputs all data as a single block in the next format:
 * {
 *     "meta":
 *     [
 *         {
 *             "name": "name1",
 *             "type": "type1"
 *         },
 *         {
 *             "name": "name2",
 *              "type": "type2"
 *         },
 *         ...
 *     ],
 *
 *     "data":
 *     {
 *         "name1": [value1, value2, value3, ...],
 *         "name2": [value1, value2m value3, ...],
 *         ...
 *     },
 *
 *     "rows": ...,
 *
 *     "statistics":
 *     {
 *         "elapsed": ...,
 *         "rows_read": ...,
 *         "bytes_read": ...
 *     }
 * }
 */
class JSONColumnsWithMetadataBlockOutputFormat : public JSONColumnsBlockOutputFormat
{
public:
    JSONColumnsWithMetadataBlockOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_);

    String getName() const override { return "JSONCompactColumnsBlockOutputFormat"; }

    void setRowsBeforeLimit(size_t rows_before_limit_) override { statistics.rows_before_limit = rows_before_limit_; statistics.applied_limit = true; }
    void setRowsBeforeAggregation(size_t rows_before_aggregation_) override
    {
        statistics.rows_before_aggregation = rows_before_aggregation_;
        statistics.applied_aggregation = true;
    }
    void onProgress(const Progress & progress_) override { statistics.progress.incrementPiecewiseAtomically(progress_); }

protected:
    void consumeTotals(Chunk chunk) override;
    void consumeExtremes(Chunk chunk) override;

    void writePrefix() override;
    void writeSuffix() override;
    void finalizeImpl() override;
    void resetFormatterImpl() override;

    void writeChunkStart() override;
    void writeChunkEnd() override;

    void writeExtremesElement(const char * title, const Columns & columns, size_t row_num);

    DataTypes types;
    size_t rows;
};

}
