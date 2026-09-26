#pragma once

#include <Common/isValidUTF8.h>
#include <DataTypes/IDataType.h>
#include <Formats/FormatSettings.h>
#include <IO/WriteBufferFromString.h>
#include <IO/writeValidUTF8.h>

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

/// Whether an opaque column carries the value's text form, which also makes it `utf8` rather than `binary`:
/// the Arrow type states which of the two encodings the payload is, so a reader can tell them apart.
///
/// `text` mode asks for the text form, except for an aggregate state:
/// `SerializationAggregateFunction::serializeText` writes the raw state bytes, which are not text, so it is
/// serialized as binary in either mode.
///
/// `output_format_arrow_string_as_string` deliberately does not enter into it. It says how a `String` column
/// is typed, and letting it also move an opaque column would put a text payload into a `binary` column,
/// leaving a reader unable to tell which encoding it holds.
inline bool arrowOpaqueValueIsText(FormatSettings::ArrowUnsupportedTypes mode, const DataTypePtr & type)
{
    return mode == FormatSettings::ArrowUnsupportedTypes::TEXT && !WhichDataType(type).isAggregateFunction();
}

/// Replaces each invalid UTF-8 sequence in `value` with U+FFFD. The result aliases `value` itself when it
/// needs no change and `scratch` otherwise, so both have to outlive it.
///
/// An Arrow `utf8` column is required by the format to hold valid UTF-8, and a text payload can break that:
/// a `Dynamic` holding a `String` serializes those bytes verbatim, and they can be arbitrary. `binary` mode
/// is the byte-exact one; here the column has been declared as text, so the bytes are made to match the
/// declaration.
inline std::string_view makeValidUTF8View(std::string_view value, String & scratch)
{
    if (UTF8::isValidUTF8(reinterpret_cast<const UInt8 *>(value.data()), value.size()))
        return value;

    WriteBufferFromString scratch_out(scratch);
    writeValidUTF8(value.data(), value.data() + value.size(), scratch_out);
    scratch_out.finalize();
    return scratch;
}

}
