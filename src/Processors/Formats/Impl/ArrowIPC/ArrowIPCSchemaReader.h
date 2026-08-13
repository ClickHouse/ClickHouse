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

private:
    const bool stream;
    const FormatSettings format_settings;
    /// Total number of rows of the Arrow file, summed from the record-batch footer blocks during
    /// `readSchema` (the file format only); `nullopt` for `ArrowStream`, which cannot be counted up front.
    std::optional<size_t> num_rows_in_file;
};

}

#endif
