#pragma once

#include <Processors/ISimpleTransform.h>
#include <Core/SortDescription.h>
#include <Core/InterpolateDescription.h>
#include <Interpreters/FillingRow.h>


namespace DB
{

/** Implements modifier WITH FILL of ORDER BY clause.
 *  It fills gaps in data stream by rows with missing values in columns with set WITH FILL and default values in other columns.
 *  Optionally FROM, TO and STEP values can be specified.
 */
class FillingTransform : public ISimpleTransform
{
public:
    FillingTransform(const Block & header_, const SortDescription & sort_description_, InterpolateDescriptionPtr interpolate_description_);

    String getName() const override { return "FillingTransform"; }

    Status prepare() override;

    static Block transformHeader(Block header, const SortDescription & sort_description);

protected:
    void transform(Chunk & Chunk) override;

private:
    void saveLastRow(const MutableColumns & result_columns);
    void interpolate(const MutableColumns& result_columns, Block & interpolate_block);

    using MutableColumnRawPtrs = std::vector<IColumn *>;
    void initColumns(
        const Columns & input_columns,
        Columns & input_fill_columns,
        Columns & input_interpolate_columns,
        Columns & input_other_columns,
        MutableColumns & output_columns,
        MutableColumnRawPtrs & output_fill_columns,
        MutableColumnRawPtrs & output_interpolate_columns,
        MutableColumnRawPtrs & output_other_columns);

    const SortDescription sort_description; /// Contains only columns with WITH FILL.
    const InterpolateDescriptionPtr interpolate_description; /// Contains INTERPOLATE columns

    FillingRow filling_row; /// Current row, which is used to fill gaps.
    FillingRow next_row; /// Row to which we need to generate filling rows.

    using Positions = std::vector<size_t>;
    Positions fill_column_positions;
    Positions interpolate_column_positions;
    Positions other_column_positions;
    std::vector<std::pair<size_t, NameAndTypePair>> input_positions; /// positions in result columns required for actions
    ExpressionActionsPtr interpolate_actions;
    bool first = true;
    bool generate_suffix = false;

    Columns last_row;

    /// Determines should we insert filling row before start generating next rows.
    bool should_insert_first = false;
};

class FillingNoopTransform : public ISimpleTransform
{
public:
    FillingNoopTransform(const Block & header, const SortDescription & sort_description_)
        : ISimpleTransform(header, FillingTransform::transformHeader(header, sort_description_), true)
    {
    }

    void transform(Chunk &) override {}
    String getName() const override { return "FillingNoopTransform"; }
};

}
