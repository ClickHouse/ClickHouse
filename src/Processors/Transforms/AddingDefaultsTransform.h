#pragma once

#include <Processors/ISimpleTransform.h>
#include <Storages/ColumnsDescription.h>

#include <mutex>


namespace DB
{

class IInputFormat;

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class DefaultExpressionsSets;
using DefaultExpressionsSetsPtr = std::shared_ptr<DefaultExpressionsSets>;

/// Adds defaults to columns using BlockDelayedDefaults bitmask attached to Block by child InputStream.
class AddingDefaultsTransform final : public ISimpleTransform
{
public:
    AddingDefaultsTransform(
        SharedHeader header,
        const ColumnsDescription & columns_,
        IInputFormat & input_format_,
        ContextPtr context_,
        DefaultExpressionsSetsPtr default_expressions_sets_ = nullptr);

    String getName() const override { return "AddingDefaultsTransform"; }

protected:
    void onCancel() noexcept override;

    void transform(Chunk & chunk) override;

private:
    const ColumnsDescription columns;
    const ColumnDefaults column_defaults;
    IInputFormat & input_format;
    ContextPtr context;

    /// The sets of the defaults that read a table (`x IN table`), built once for all chunks rather than per chunk.
    /// The readers of one query that create a transform per file or stream pass the sets of the query
    /// (`FormatParserSharedResources::default_expressions_sets`), so that they are built once per query.
    const DefaultExpressionsSetsPtr default_expressions_sets;

    /// The default-expression actions are built per chunk inside `transform`, so `onCancel`
    /// needs a published handle to the instance that is executing right now to forward
    /// `cancelExecution` into its functions.
    std::mutex current_actions_mutex;
    ExpressionActionsPtr current_actions;
};

}
