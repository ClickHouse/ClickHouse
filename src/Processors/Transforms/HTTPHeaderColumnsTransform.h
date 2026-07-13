#pragma once

#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromString.h>
#include <Processors/ISimpleTransform.h>

namespace DB
{

/// Replaces specified columns in each chunk with constant values parsed from strings.
/// Used by the http_column_* feature to inject HTTP header values into INSERT columns.
/// For the sync INSERT path, all rows in a single request carry the same header values.
/// The string values are deserialized through the column type's text serialization,
/// so non-String types (UInt64, Array, etc.) are parsed correctly.
class HTTPHeaderColumnsTransform : public ISimpleTransform
{
public:
    HTTPHeaderColumnsTransform(
        const Block & header_,
        const NameToNameMap & http_header_columns_)
        : ISimpleTransform(header_, header_, false)
    {
        /// Precompute column indices and pre-parse values for the injected columns.
        for (size_t i = 0; i < header_.columns(); ++i)
        {
            auto it = http_header_columns_.find(header_.getByPosition(i).name);
            if (it != http_header_columns_.end())
            {
                const auto & col_type = header_.getByPosition(i).type;
                /// Parse the string value into the target column type once.
                auto parsed_col = col_type->createColumn();
                ReadBufferFromString buf(it->second);
                FormatSettings format_settings;
                col_type->getDefaultSerialization()->deserializeWholeText(*parsed_col, buf, format_settings);
                injected_columns.push_back({i, std::move(parsed_col), col_type});
            }
        }
    }

    String getName() const override { return "HTTPHeaderColumnsTransform"; }

protected:
    void transform(Chunk & chunk) override
    {
        if (injected_columns.empty())
            return;

        size_t num_rows = chunk.getNumRows();
        auto columns = chunk.detachColumns();

        for (const auto & inj : injected_columns)
        {
            auto new_col = inj.type->createColumn();
            new_col->reserve(num_rows);
            for (size_t row = 0; row < num_rows; ++row)
                new_col->insertFrom(*inj.parsed_value, 0);
            columns[inj.index] = std::move(new_col);
        }

        chunk.setColumns(std::move(columns), num_rows);
    }

private:
    struct InjectedColumn
    {
        size_t index;
        ColumnPtr parsed_value;  /// Single pre-parsed value to replicate into each chunk.
        DataTypePtr type;
    };
    std::vector<InjectedColumn> injected_columns;
};

}
