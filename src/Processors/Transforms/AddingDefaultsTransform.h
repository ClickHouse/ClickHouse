#pragma once

#include <Processors/ISimpleTransform.h>
#include <Storages/ColumnsDescription.h>


namespace DB
{

class IInputFormat;

/// Adds defaults to columns using BlockDelayedDefaults bitmask attached to Block by child InputStream.
class AddingDefaultsTransform final : public ISimpleTransform
{
public:
    /// injected_columns_ - extra columns not produced by the format but available for
    /// DEFAULT expression evaluation (e.g. columns injected from HTTP request headers via
    /// http_column_*). Each holds a single-row value column. They are temporarily appended
    /// to the working block so DEFAULT expressions can reference them, then stripped so the
    /// output matches the format header. Appended at the end to keep the format's
    /// BlockMissingValues indices for the body columns aligned.
    AddingDefaultsTransform(
        SharedHeader header,
        const ColumnsDescription & columns_,
        IInputFormat & input_format_,
        ContextPtr context_,
        ColumnsWithTypeAndName injected_columns_ = {});

    String getName() const override { return "AddingDefaultsTransform"; }

    /// Updates the injected columns used for DEFAULT expression evaluation.
    /// Called per-entry in the async path so each entry uses its own header values.
    void setInjectedColumns(ColumnsWithTypeAndName cols) { injected_columns = std::move(cols); }

protected:
    void transform(Chunk & chunk) override;

private:
    const ColumnsDescription columns;
    const ColumnDefaults column_defaults;
    IInputFormat & input_format;
    ContextPtr context;
    /// Single-row value columns for header-injected columns (name, type, 1-row column).
    ColumnsWithTypeAndName injected_columns;
};

}
