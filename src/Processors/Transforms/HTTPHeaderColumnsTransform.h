#pragma once

#include <base/defines.h>
#include <Columns/ColumnConst.h>
#include <Common/Exception.h>
#include <Core/HTTPHeaderColumns.h>
#include <Formats/FormatSettings.h>
#include <Formats/parseColumnFromString.h>
#include <Processors/ISimpleTransform.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_QUERY_PARAMETER;
}


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
    /// input_header        - block the format produces (body columns only).
    /// output_header       - full pipeline block (body + http_column_* columns).
    /// http_header_columns - column_name -> header_value from the URL params.
    /// format_settings     - query/session format settings for value deserialization.
    HTTPHeaderColumnsTransform(
        const Block & input_header,
        const Block & output_header,
        const HTTPHeaderColumns & http_header_columns,
        const FormatSettings & format_settings)
        : ISimpleTransform(input_header, output_header, false)
    {
        col_sources.reserve(output_header.columns());
        size_t body_col_idx = 0;
        for (size_t i = 0; i < output_header.columns(); ++i)
        {
            const auto & col_name = output_header.getByPosition(i).name;
            if (http_header_columns.contains(col_name))
            {
                /// Injected column: parse the header value once.
                /// find() is guaranteed non-null here: contains() was just true.
                const String & str_value = *http_header_columns.find(col_name);
                const auto & col_type = output_header.getByPosition(i).type;
                MutableColumnPtr parsed;
                try
                {
                    parsed = parseColumnValueFromString(col_type, str_value, format_settings);
                }
                catch (const DB::Exception & e)
                {
                    throw DB::Exception(DB::ErrorCodes::BAD_QUERY_PARAMETER,
                        "http_column parameter for column '{}' contains value '{}' that cannot be parsed as {}: {}",
                        col_name, str_value, col_type->getName(), e.message());
                }
                col_sources.push_back({true, 0, std::move(parsed), col_type});
            }
            else
            {
                /// Body column: match by name so named formats (JSONEachRow, TSVWithNames,
                /// etc.) land in the right slot regardless of declaration order.
                size_t input_pos = input_header.has(col_name)
                    ? input_header.getPositionByName(col_name)
                    : body_col_idx;
                chassert(input_pos < input_header.columns());
                col_sources.push_back({false, input_pos, nullptr, nullptr});
                ++body_col_idx;
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
                chassert(src.input_idx < input_columns.size());
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
