#pragma once

#include <Core/ColumnsWithTypeAndName.h>
#include <Processors/ISimpleTransform.h>
#include <Storages/ColumnsDescription.h>

#include <functional>
#include <mutex>
#include <optional>


namespace DB
{

class IInputFormat;

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

/// Adds defaults to columns using BlockDelayedDefaults bitmask attached to Block by child InputStream.
class AddingDefaultsTransform final : public ISimpleTransform
{
public:
    /// Returns `num_rows` rows of the `extra_inputs` among `read_by_defaults`, the columns (not subcolumns) the defaults
    /// read. Called only for a chunk where some default is evaluated.
    using GetExtraInputs = std::function<ColumnsWithTypeAndName(const NameSet & read_by_defaults, size_t num_rows)>;

    /// With `requested_columns_`, only their defaults and the defaults they read are evaluated.
    /// `extra_inputs_` are columns the input format does not produce, e.g. Hive partition columns.
    AddingDefaultsTransform(
        SharedHeader header,
        const ColumnsDescription & columns_,
        IInputFormat & input_format_,
        ContextPtr context_,
        const NamesAndTypesList & extra_inputs_ = {},
        GetExtraInputs get_extra_inputs_ = {},
        const NamesAndTypesList * requested_columns_ = nullptr);

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
    /// The header and the empty `extra_inputs_`.
    Block available_columns;
    std::optional<NameSet> columns_to_fill;

    /// The default-expression actions are built per chunk inside `transform`, so `onCancel`
    /// needs a published handle to the instance that is executing right now to forward
    /// `cancelExecution` into its functions.
    std::mutex current_actions_mutex;
    ExpressionActionsPtr current_actions;
};

}
