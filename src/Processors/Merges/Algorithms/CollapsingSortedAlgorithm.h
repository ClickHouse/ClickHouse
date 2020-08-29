#pragma once
#include <Processors/Merges/Algorithms/IMergingAlgorithmWithSharedChunks.h>
#include <Processors/Merges/Algorithms/MergedData.h>
#include <DataStreams/ColumnGathererStream.h>

namespace Poco
{
    class Logger;
}

namespace DB
{

/** Merges several sorted inputs to one.
  * For each group of consecutive identical values of the primary key (the columns by which the data is sorted),
  *  keeps no more than one row with the value of the column `sign_column = -1` ("negative row")
  *  and no more than a row with the value of the column `sign_column = 1` ("positive row").
  * That is, it collapses the records from the change log.
  *
  * If the number of positive and negative rows is the same, and the last row is positive, then the first negative and last positive rows are written.
  * If the number of positive and negative rows is the same, and the last line is negative, it writes nothing.
  * If the positive by 1 is greater than the negative rows, then only the last positive row is written.
  * If negative by 1 is greater than positive rows, then only the first negative row is written.
  * Otherwise, a logical error.
  */
class CollapsingSortedAlgorithm final : public IMergingAlgorithmWithSharedChunks
{
public:
    CollapsingSortedAlgorithm(
        const Block & header,
        size_t num_inputs,
        SortDescription description_,
        const String & sign_column,
        bool only_positive_sign_, /// For select final. Skip rows with sum(sign) < 0.
        size_t max_block_size,
        WriteBuffer * out_row_sources_buf_,
        bool use_average_block_sizes,
        Poco::Logger * log_);

    Status merge() override;

private:
    MergedData merged_data;

    const size_t sign_column_number;
    const bool only_positive_sign;

    static constexpr size_t max_row_refs = 4; /// first_negative, last_positive, last, current.
    RowRef first_negative_row;
    RowRef last_positive_row;
    RowRef last_row;

    size_t count_positive = 0;    /// The number of positive rows for the current primary key.
    size_t count_negative = 0;    /// The number of negative rows for the current primary key.
    bool last_is_positive = false;  /// true if the last row for the current primary key is positive.

    /// Fields specific for VERTICAL merge algorithm.
    /// Row numbers are relative to the start of current primary key.
    size_t current_pos = 0;                        /// Current row number
    size_t first_negative_pos = 0;                 /// Row number of first_negative
    size_t last_positive_pos = 0;                  /// Row number of last_positive
    PODArray<RowSourcePart> current_row_sources;   /// Sources of rows with the current primary key

    size_t count_incorrect_data = 0;    /// To prevent too many error messages from writing to the log.
    Poco::Logger * log;

    void reportIncorrectData();
    void insertRow(RowRef & row);
    void insertRows();
};

}

