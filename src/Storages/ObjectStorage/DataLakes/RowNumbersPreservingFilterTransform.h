#pragma once
#include <Processors/ISimpleTransform.h>
#include <Interpreters/ExpressionActions.h>

namespace DB
{

/// Like FilterTransform, but maintains ChunkInfoRowNumbers::applied_filter, so that later
/// consumers of the physical row numbers (the `_row_number` virtual column, streaming position
/// deletes, lazy materialization) are not broken by the filtering.
/// Used for data lake filters that drop rows inside the per-file reading pipeline,
/// e.g. Iceberg equality deletes.
class RowNumbersPreservingFilterTransform final : public ISimpleTransform
{
public:
    RowNumbersPreservingFilterTransform(
        const SharedHeader & header_,
        ExpressionActionsPtr expression_,
        String filter_column_name_,
        bool remove_filter_column_);

    String getName() const override { return "RowNumbersPreservingFilterTransform"; }

    void transform(Chunk & chunk) override;

private:
    ExpressionActionsPtr expression;
    String filter_column_name;
    bool remove_filter_column;
};

}
