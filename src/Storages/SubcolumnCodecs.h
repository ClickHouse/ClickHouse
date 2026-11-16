#pragma once

#include <DataTypes/Serializations/ISerialization.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

struct ColumnDescription;
class ColumnsDescription;
struct CodecValidationSettings;

/// Validates the column codec and the subcolumn codecs of one column against its type and stores their
/// normalized forms. An ALIAS column has no codecs. A column has either a column codec or subcolumn codecs;
/// a declared subcolumn must exist, must not be inside another declared subcolumn, and must not be inside
/// a dynamic type (`JSON`, `Dynamic`, `Variant`).
void validateColumnCodecs(ColumnDescription & column, const CodecValidationSettings & validation_settings);

/// The same for every column that has a column codec or subcolumn codecs. Also checks that no column has the name
/// of a declared subcolumn.
void validateColumnCodecs(ColumnsDescription & columns, const CodecValidationSettings & validation_settings);

/// Codec declared for one stream of `column`: the codec of the declared subcolumn the stream belongs to,
/// otherwise the column codec, otherwise nullptr, which means the part default.
/// `substream_path` is the path of the stream from the column, as the serializations of the column and of its subcolumns
/// enumerate it.
ASTPtr getSubstreamCodecDescription(const ISerialization::SubstreamPath & substream_path, const ColumnDescription & column);

}
