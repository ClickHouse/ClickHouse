#pragma once

#include <Common/isValidUTF8.h>
#include <DataTypes/IDataType.h>
#include <Formats/FormatSettings.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteBufferValidUTF8.h>

#include <string_view>

namespace DB
{

/// Helpers for a column whose type has no first-class Arrow mapping and is therefore written as an opaque
/// variable-width column holding one serialized value per row, per `output_format_arrow_unsupported_types`.
///
/// Two writers produce Arrow: the native IPC one (`SchemaConverter` builds the schema, `RecordBatchEncoder`
/// fills the values) and the one built on the Apache Arrow library (`CHColumnToArrowColumn`, used by Arrow
/// Flight SQL and DeltaLake). In both, the Arrow type and the values are decided in separate places, so
/// those decisions live here to keep them in step: a column typed `utf8` must be filled with text, and a
/// column typed `binary` must not be silently filled with something else.

/// Whether an opaque column carries the value's text form. `text` mode asks for it, except for an aggregate
/// state: `SerializationAggregateFunction::serializeText` writes the raw state bytes, which are not text, so
/// it is serialized as binary in either mode.
inline bool arrowOpaqueValueIsText(FormatSettings::ArrowUnsupportedTypes mode, const DataTypePtr & type)
{
    return mode == FormatSettings::ArrowUnsupportedTypes::TEXT && !WhichDataType(type).isAggregateFunction();
}

/// Whether an opaque column is typed `utf8` rather than `binary`. A text payload uses the Arrow type a
/// `String` column uses and follows the same setting, so that `output_format_arrow_string_as_string = 0`
/// keeps every column of this output free of unvalidated UTF-8 rather than only the real `String` ones.
inline bool
arrowOpaqueTypeIsUtf8(FormatSettings::ArrowUnsupportedTypes mode, const DataTypePtr & type, bool output_string_as_string)
{
    return arrowOpaqueValueIsText(mode, type) && output_string_as_string;
}

/// Returns `value` unchanged when it is already valid UTF-8, otherwise a sanitized copy held in `scratch`,
/// with each invalid sequence replaced by U+FFFD.
///
/// An Arrow `utf8` column is required by the format to hold valid UTF-8, and a text payload can break that:
/// the serialized text of a `JSON` or `Dynamic` value embeds the bytes of its `String` subcolumns verbatim,
/// and those can be arbitrary. The alternative of declaring `binary` instead is what
/// `output_format_arrow_string_as_string = 0` selects, and it stays byte-exact; here the column has been
/// declared as text, so the bytes are made to match the declaration. Only values that are already invalid
/// change, and valid text - which is every value that a reader could have interpreted as text anyway - pays
/// one validation pass and no copy.
inline std::string_view makeValidUTF8View(std::string_view value, String & scratch)
{
    if (UTF8::isValidUTF8(reinterpret_cast<const UInt8 *>(value.data()), value.size()))
        return value;

    WriteBufferFromString scratch_out(scratch);
    {
        WriteBufferValidUTF8 validating_out(scratch_out);
        validating_out.write(value.data(), value.size());
        /// The trailing bytes stay buffered inside the validating buffer until it is flushed, and its
        /// destructor catches and suppresses a failure of that flush, which would silently truncate the
        /// value. Flush explicitly so such a failure propagates as itself.
        validating_out.finalize();
    }
    scratch_out.finalize();
    return scratch;
}

}
