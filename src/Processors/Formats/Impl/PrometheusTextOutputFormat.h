#pragma once

#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/IRowOutputFormat.h>


namespace DB
{

class WriteBuffer;

/** A stream for outputting data in tsv format.
  */
class PrometheusTextOutputFormat : public IRowOutputFormat
{
public:
    /** with_names - output in the first line a header with column names
      * with_types - output the next line header with the names of the types
      */
    PrometheusTextOutputFormat(
        WriteBuffer & out_,
        const Block & header_,
        const RowOutputFormatParams & params_,
        const FormatSettings & format_settings_);

    String getName() const override { return "PrometheusTextOutputFormat"; }

    /// https://github.com/prometheus/docs/blob/86386ed25bc8a5309492483ec7d18d0914043162/content/docs/instrumenting/exposition_formats.md
    String getContentType() const override { return "text/plain; version=0.0.4; charset=UTF-8"; }

protected:
    void write(const Columns & columns, size_t row_num) override;
    void writeField(const IColumn &, const ISerialization &, size_t) override {}


    struct ColumnPositions
    {
        size_t name;
        size_t value;
        std::optional<size_t> help;
        std::optional<size_t> type;
    };


    ColumnPositions pos;

    const FormatSettings format_settings;
};

}
