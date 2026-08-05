#pragma once

#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <IO/WriteBuffer.h>

#include <string_view>
#include <unordered_map>
#include <vector>

namespace DB
{
struct FormatSettings;
class ColumnDynamic;
}

namespace DB::Parquet
{

/// Decoding of Parquet VARIANT values (https://github.com/apache/parquet-format VariantEncoding.md)
/// and reconstruction of shredded Variant values (VariantShredding.md).
/// Two output forms are provided:
///  * JSON text (variantValueToJSON / shreddedValueToJSON), used when the String or JSON type
///    was requested (the text is then cast to JSON by castColumn).
///  * Direct construction into ColumnDynamic (variantValueToDecoded / shreddedValueToDecoded /
///    insertIntoDynamicColumn), which skips the JSON text round-trip entirely.

/// Parsed variant metadata: the dictionary of field names shared by all `value` binaries
/// of a Variant column.
struct VariantMetadata
{
    /// Field names; indexed by field_id from encoded objects.
    std::vector<std::string_view> dictionary;
    /// The metadata binary this was parsed from; used to detect when a cached parse stays valid.
    std::string_view raw;
};

void parseVariantMetadata(std::string_view data, VariantMetadata & out);

/// Decode one variant-encoded binary value to JSON text.
void variantValueToJSON(std::string_view value, const VariantMetadata & metadata, WriteBuffer & out, const FormatSettings & settings);

/// Columns backing one Variant value position: the variant-encoded `value` column and
/// the shredded `typed_value` column (each may be absent). Used recursively: object fields
/// and array elements of a shredded Variant are themselves ShreddedValueColumns.
struct ShreddedValueColumns
{
    /// (Nullable) String column with variant-encoded binaries, or nullptr if the schema has no `value` field.
    const IColumn * value = nullptr;
    /// Assembled column for the shredded `typed_value` subtree, or nullptr if unshredded.
    const IColumn * typed_value = nullptr;
    /// Type of `typed_value` (Tuple for shredded objects, Array for shredded arrays, primitive otherwise).
    const IDataType * typed_value_type = nullptr;
};

/// Per-call memo for IDataType::getDefaultSerialization lookups in the shredded walker (the
/// pooled lookup is significant per-row overhead).
using SerializationMemo = std::unordered_map<const IDataType *, SerializationPtr>;

/// Reconstruct one Variant value (possibly shredded) as JSON text.
/// Returns false if the value is missing entirely (both `value` and `typed_value` are null),
/// which is only meaningful for shredded object fields.
bool shreddedValueToJSON(
    const ShreddedValueColumns & columns,
    size_t row,
    const VariantMetadata & metadata,
    WriteBuffer & out,
    const FormatSettings & settings,
    SerializationMemo * memo = nullptr);

/// A decoded Variant value for direct insertion into a ColumnDynamic: a concrete type and the
/// value itself. `type` is nullptr when the value is missing (a Variant null or a missing value),
/// in which case a Dynamic null should be inserted.
/// For arrays, `array_value` additionally carries the per-element decoded values (element types
/// may differ), so they can be inserted element-wise with their types preserved (e.g. Bool,
/// which a bare Field cannot express).
struct DecodedVariantValue
{
    DecodedVariantValue() = default;
    DecodedVariantValue(DataTypePtr type_, Field field_) : type(std::move(type_)), field(std::move(field_)) {}

    DataTypePtr type;
    Field field;
    std::optional<std::pair<DataTypePtr, std::vector<DecodedVariantValue>>> array_value;
};

/// Decode one variant-encoded binary value directly into a type + Field (no JSON text involved).
DecodedVariantValue variantValueToDecoded(std::string_view value, const VariantMetadata & metadata);

/// Reconstruct one Variant value (possibly shredded) directly into a type + Field.
DecodedVariantValue shreddedValueToDecoded(const ShreddedValueColumns & columns, size_t row, const VariantMetadata & metadata);
/// As above, with explicit FormatSettings.
DecodedVariantValue shreddedValueToDecoded(const ShreddedValueColumns & columns, size_t row, const VariantMetadata & metadata, const FormatSettings & settings);

/// Insert a decoded value into a ColumnDynamic, registering a new variant for its type if needed
/// (falling back to the shared variant when the variant count limit is reached).
void insertIntoDynamicColumn(ColumnDynamic & column, const DataTypePtr & type, const Field & value);

/// Insert a decoded value (which may be an array with per-element typed values) into a
/// ColumnDynamic. Arrays with a Dynamic element type are inserted element-wise so per-element
/// types are preserved.
void insertDecodedIntoDynamic(ColumnDynamic & column, const DecodedVariantValue & value);

/// A path of object field names within a Variant value, with field ids resolved against the
/// current metadata dictionary (the ids change when the metadata binary changes, so resolve
/// once per metadata via resolveVariantExtractPath).
struct VariantExtractPath
{
    std::vector<String> names;
    /// Field ids parallel to `names`; nullopt when the name is not in the dictionary (the field
    /// is then absent from all variant-encoded binaries, but may still exist shredded).
    std::vector<std::optional<uint32_t>> ids;
};

/// Resolve path.ids against a freshly parsed metadata dictionary.
void resolveVariantExtractPath(VariantExtractPath & path, const VariantMetadata & metadata);

/// Extract the value at `path` from a (possibly shredded) Variant value, decoding only the
/// addressed subtree. Returns a type-less DecodedVariantValue (a Dynamic null) when the path
/// is absent from this row's value. An empty path decodes the whole value.
DecodedVariantValue shreddedValueExtractPath(
    const ShreddedValueColumns & columns,
    size_t row,
    const VariantMetadata & metadata,
    const VariantExtractPath & path,
    const FormatSettings & settings);

}
