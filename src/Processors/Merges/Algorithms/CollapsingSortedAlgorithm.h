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

class CollapsingSortedAlgorithm : public IMergingAlgorithmWithSharedChunks
{
public:
    CollapsingSortedAlgorithm(
        const Block & header,
        size_t num_inputs,
        SortDescription description_,
        const String & sign_column,
        size_t max_block_size,
        WriteBuffer * out_row_sources_buf_,
        bool use_average_block_sizes,
        Logger * log_);

    Status merge() override;

private:
    MergedData merged_data;

    const size_t sign_column_number;

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
    Logger * log;

    void reportIncorrectData();
    void insertRow(RowRef & row);
    void insertRows();
};

}

