#pragma once

#include <Columns/ColumnConst.h>
#include <Formats/FormatSettings.h>
#include <Formats/parseColumnFromString.h>
#include <Processors/ISimpleTransform.h>

namespace DB
{

/// Injects HTTP header values as INSERT columns for the sync path.
///
/// This is an expanding transform: input is the body-only block (columns the format
/// parsed from the HTTP body), output is the full block (body columns + http_column_*
/// mapped columns). The injected columns are placed at their correct positions in the
/// output so the downstream chain (addMissingDefaults, constraints, sink) receives a
/// complete block and does NOT evaluate DEFAULT expressions for the injected columns.
///
/// Using the format-only block as input lets positional formats (TSV, CSV, Values,
/// RowBinary) work correctly: they see only the body columns and read the exact number
/// of fields they expect.
class HTTPHeaderColumnsTransform : public ISimpleTransform
{
public:
    /// input_header  - block the format produces (body columns only).
    /// output_header - full pipeline block (body + http_column_* columns).
    /// http_header_columns - column_name -> header_value from the URL params.
    /// format_settings - query/session format settings for value deserialization.
    HTTPHeaderColumnsTransform(
        const Block & input_header,
        const Block & output_header,
        const NameToNameMap & http_header_columns,
        const FormatSettings & format_settings)
        : ISimpleTransform(input_header, output_header, false)
    {
        col_sources.reserve(output_header.columns());
        for (size_t i = 0; i < output_header.columns(); ++i)
        {
            const auto & col_name = output_header.getByPosition(i).name;
            if (input_header.has(col_name))
            {
                /// Body column: pass through from input at the corresponding position.
                col_sources.push_back({false, input_header.getPositionByName(col_name), nullptr, nullptr});
            }
            else
            {
                /// Injected column: parse the header value once.
                auto it = http_header_columns.find(col_name);
                const String & str_value = (it != http_header_columns.end()) ? it->second : "";
                const auto & col_type = output_header.getByPosition(i).type;
                auto parsed = parseColumnValueFromString(col_type, str_value, format_settings);
                col_sources.push_back({true, 0, std::move(parsed), col_type});
            }
        }
    }

    String getName() const override { return "HTTPHeaderColumnsTransform"; }

protected:
    void transform(Chunk & chunk) override
    {
        size_t num_rows = chunk.getNumRows();
        auto input_columns = chunk.detachColumns();

        Columns output_columns;
        output_columns.reserve(col_sources.size());
        for (const auto & src : col_sources)
        {
            if (!src.is_injected)
            {
                output_columns.push_back(std::move(input_columns[src.input_idx]));
            }
            else
            {
                output_columns.push_back(ColumnConst::create(src.parsed_value, num_rows));
            }
        }
        chunk.setColumns(std::move(output_columns), num_rows);
    }

private:
    struct ColSource
    {
        bool is_injected;
        size_t input_idx;      /// Position in the input (body) block; valid when !is_injected.
        ColumnPtr parsed_value; /// Pre-parsed single-row column; valid when is_injected.
        DataTypePtr type;       /// Column type; valid when is_injected.
    };
    std::vector<ColSource> col_sources;
};

}
