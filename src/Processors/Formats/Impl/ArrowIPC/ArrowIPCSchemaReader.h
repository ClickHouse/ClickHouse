#pragma once

#include "config.h"

#if USE_ARROW

#include <Processors/Formats/ISchemaReader.h>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/Impl/ArrowIPC/SchemaConverter.h>

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

private:
    const bool stream;
    const FormatSettings format_settings;
    /// Total number of rows of the Arrow file, summed from the record-batch footer blocks during
    /// `readSchema` (the file format only); `nullopt` for `ArrowStream`, which cannot be counted up front.
    std::optional<size_t> num_rows_in_file;
};

namespace ArrowIPC
{

/// The columns inferred from an Arrow schema, following `schema_inference_make_columns_nullable`,
/// `input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference` and the GeoParquet metadata.
NamesAndTypesList inferColumns(const ArrowSchema & schema, const FormatSettings & format_settings);

/// Removes `Nullable` at every depth, as `schema_inference_make_columns_nullable = 0` requires, except around
/// `Nothing`: the type of an Arrow `null` field holds only NULLs and stays `Nullable(Nothing)`.
DataTypePtr removeNullableForInference(const DataTypePtr & type, const FormatSettings & format_settings);

}

}

#endif
