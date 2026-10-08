#pragma once

#include <Core/InterpolateDescription.h>
#include <Core/SortDescription.h>
#include <Interpreters/FillingRow.h>
#include <Processors/ISimpleTransform.h>


namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class QueryStatus;
using QueryStatusPtr = std::shared_ptr<QueryStatus>;

/** Implements modifier WITH FILL of ORDER BY clause.
 *  It fills gaps in data stream by rows with missing values in columns with set WITH FILL and default values in other columns.
 *  Optionally FROM, TO and STEP values can be specified.
 */
class FillingTransform final : public ISimpleTransform
{
public:
    FillingTransform(
        SharedHeader header_,
        const SortDescription & sort_description_,
        const SortDescription & fill_description_,
        InterpolateDescriptionPtr interpolate_description_,
        bool use_with_fill_by_sorting_prefix_,
        QueryStatusPtr process_list_element_);

    String getName() const override { return "FillingTransform"; }

    Status prepare() override;

    static Block transformHeader(Block header, const SortDescription & sort_description);

protected:
    void transform(Chunk & chunk) override;

private:
    using MutableColumnRawPtrs = std::vector<IColumn *>;
    void transformRange(
        const Columns & input_fill_columns,
        const Columns & input_interpolate_columns,
        const Columns & input_sort_prefix_columns,
        const Columns & input_other_columns,
        const MutableColumns & result_columns,
        const MutableColumnRawPtrs & res_fill_columns,
        const MutableColumnRawPtrs & res_interpolate_columns,
        const MutableColumnRawPtrs & res_sort_prefix_columns,
        const MutableColumnRawPtrs & res_other_columns,
        std::pair<size_t, size_t> range,
        bool new_sorting_prefix);

    void saveLastRow(const MutableColumns & result_columns);
    void interpolate(const MutableColumns & result_columns, Block & interpolate_block);

    /// Whether filling-row generation should stop: true if the query was cancelled or a break-mode
    /// timeout was reached. Throws TIMEOUT_EXCEEDED / QUERY_WAS_CANCELLED when max_execution_time is
    /// exceeded (throw mode) or the query was killed.
    bool isCancelledOrTimeLimitExceeded();

    void initColumns(
        const Columns & input_columns,
        Columns & input_fill_columns,
        Columns & input_interpolate_columns,
        Columns & input_sort_prefix_columns,
        Columns & input_other_columns,
        MutableColumns & output_columns,
        MutableColumnRawPtrs & output_fill_columns,
        MutableColumnRawPtrs & output_interpolate_columns,
        MutableColumnRawPtrs & output_sort_prefix_columns,
        MutableColumnRawPtrs & output_other_columns);

    bool generateSuffixIfNeeded(
        const MutableColumns & result_columns,
        MutableColumnRawPtrs res_fill_columns,
        MutableColumnRawPtrs res_interpolate_columns,
        MutableColumnRawPtrs res_sort_prefix_columns,
        MutableColumnRawPtrs res_other_columns);
    bool generateSuffixIfNeeded(const Columns & input_columns, MutableColumns & result_columns);

    void insertFromFillingRow(
        const MutableColumnRawPtrs & filling_columns,
        const MutableColumnRawPtrs & interpolate_columns,
        const MutableColumnRawPtrs & other_columns,
        const Block & interpolate_block);

    const SortDescription sort_description;
    const SortDescription fill_description; /// Contains only columns with WITH FILL.
    SortDescription sort_prefix;
    const InterpolateDescriptionPtr interpolate_description; /// Contains INTERPOLATE columns

    bool running_with_staleness = false; /// True if STALENESS clause was used.
    FillingRow filling_row; /// Current row, which is used to fill gaps.
    FillingRow next_row; /// Row to which we need to generate filling rows.
    bool filling_row_inserted = false;

    using Positions = std::vector<size_t>;
    Positions fill_column_positions;
    Positions interpolate_column_positions;
    Positions other_column_positions;
    Positions sort_prefix_positions;
    std::vector<std::pair<size_t, NameAndTypePair>> input_positions; /// positions in result columns required for actions
    ExpressionActionsPtr interpolate_actions;
    Columns last_row;
    Columns last_range_sort_prefix;
    bool all_chunks_processed = false;    /// flag to determine if we have already processed all chunks
    const bool use_with_fill_by_sorting_prefix;

    /// Used to enforce max_execution_time (and observe KILL QUERY) while generating filling rows,
    /// because a single WITH FILL range can expand into billions of rows within one transform() call
    /// and the executor only checks the time limit between calls.
    QueryStatusPtr process_list_element;

    /// Latched once a `timeout_overflow_mode = 'break'` soft timeout fires, so both the inner and the
    /// outer generation loops stop and the transform returns a partial result.
    bool time_limit_exceeded = false;
};

class FillingNoopTransform final : public ISimpleTransform
{
public:
    FillingNoopTransform(SharedHeader header, const SortDescription & sort_description_)
        : ISimpleTransform(header, std::make_shared<const Block>(FillingTransform::transformHeader(*header, sort_description_)), true)
    {
    }

    void transform(Chunk &) override {}
    String getName() const override { return "FillingNoopTransform"; }
};

}
