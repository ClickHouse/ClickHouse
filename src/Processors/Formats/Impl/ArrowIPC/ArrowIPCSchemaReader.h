#pragma once

#include "config.h"

#if USE_ARROW

#include <Processors/Formats/ISchemaReader.h>
#include <Formats/FormatSettings.h>

namespace DB
{

class ReadBuffer;

/// Native schema reader for the `Arrow` and `ArrowStream` formats (no Apache Arrow library).
class ArrowIPCSchemaReader final : public ISchemaReader
{
public:
    ArrowIPCSchemaReader(ReadBuffer & in_, bool stream_, const FormatSettings & format_settings_);

    NamesAndTypesList readSchema() override;

    std::optional<size_t> readNumberOrRows() override;

    /// The parser reads only the requested columns and never touches the rest of the file,
    /// regardless of `input_format_skip_unknown_fields`.
    bool alwaysSkipsUnknownFields() const override { return true; }

    /// The parser `castColumn`s a decoded numeric column to the requested destination type, and a
    /// cast from an integer to the `UInt32`-backed `IPv4` is valid, so a numeric source value is
    /// accepted into an `IPv4` column.
    bool readsNumericValueIntoIPv4Column() const override { return true; }

    bool castsStringSourceColumns() const override { return true; }

    /// The data carries named columns that the parser maps onto the destination by name.
    bool mapsColumnsByName() const override { return true; }

    bool usesCaseInsensitiveColumnMatching() const override { return format_settings.arrow.case_insensitive_column_matching; }

private:
    const bool stream;
    const FormatSettings format_settings;
    /// Total number of rows of the Arrow file, summed from the record-batch footer blocks during
    /// `readSchema` (the file format only); `nullopt` for `ArrowStream`, which cannot be counted up front.
    std::optional<size_t> num_rows_in_file;
};

}

#endif
