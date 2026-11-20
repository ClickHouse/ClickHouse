#pragma once

#include <Core/ColumnsWithTypeAndName.h>
#include <Processors/ISimpleTransform.h>
#include <Storages/ColumnsDescription.h>

#include <functional>
#include <mutex>


namespace DB
{

class IInputFormat;

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

/// Adds defaults to columns using BlockDelayedDefaults bitmask attached to Block by child InputStream.
class AddingDefaultsTransform final : public ISimpleTransform
{
public:
    /// Returns `num_rows` rows of the columns among `read_by_defaults` (or their subcolumns) that the input format does not
    /// produce, e.g. Hive partition columns. Called only for a chunk where some default is evaluated, with what it reads.
    using GetExtraInputs = std::function<ColumnsWithTypeAndName(const NameSet & read_by_defaults, size_t num_rows)>;

    AddingDefaultsTransform(
        SharedHeader header,
        const ColumnsDescription & columns_,
        IInputFormat & input_format_,
        ContextPtr context_,
        GetExtraInputs get_extra_inputs_ = {});

    String getName() const override { return "AddingDefaultsTransform"; }

protected:
    void onCancel() noexcept override;

    void transform(Chunk & chunk) override;

private:
    const ColumnsDescription columns;
    const ColumnDefaults column_defaults;
    IInputFormat & input_format;
    ContextPtr context;
    GetExtraInputs get_extra_inputs;

    /// The default-expression actions are built per chunk inside `transform`, so `onCancel`
    /// needs a published handle to the instance that is executing right now to forward
    /// `cancelExecution` into its functions.
    std::mutex current_actions_mutex;
    ExpressionActionsPtr current_actions;
};

}
