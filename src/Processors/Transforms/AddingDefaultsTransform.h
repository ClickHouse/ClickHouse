#pragma once

#include <Processors/ISimpleTransform.h>
#include <Storages/ColumnsDescription.h>

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
    /// If `needed_columns_` is set, the other columns are dropped afterwards, so only the defaults of `needed_columns_`
    /// and of the columns which their defaults depend on are computed.
    AddingDefaultsTransform(
        SharedHeader header,
        const ColumnsDescription & columns_,
        IInputFormat & input_format_,
        ContextPtr context_,
        const std::optional<NamesAndTypesList> & needed_columns_ = {});

    String getName() const override { return "AddingDefaultsTransform"; }

protected:
    void onCancel() noexcept override;

    void transform(Chunk & chunk) override;

private:
    const ColumnsDescription columns;
    const ColumnDefaults column_defaults;
    /// The names in storage of `needed_columns_`.
    std::optional<NameSet> needed_columns;
    IInputFormat & input_format;
    ContextPtr context;

    /// The default-expression actions are built per chunk inside `transform`, so `onCancel`
    /// needs a published handle to the instance that is executing right now to forward
    /// `cancelExecution` into its functions.
    std::mutex current_actions_mutex;
    ExpressionActionsPtr current_actions;
};

}
