#pragma once

#include <base/types.h>
#include <Core/Field.h>
#include <string_view>

namespace DB
{
class WriteBuffer;
class IDataType;
}

namespace DB::Parquet
{

/// How a decoded variant value maps onto the fixed ClickHouse
/// `Variant(Bool, Int64, Float64, String, Array(String), Map(String, String))` carrier.
enum class VariantFieldKind : UInt8
{
    Null,
    Bool,
    Int64,
    Float64,
    Date,          // Date32
    DateTimeMicros, // DateTime64(6, 'UTC')
    DateTimeNanos,  // DateTime64(9, 'UTC')
    Uuid,          // UUID
    String,        // string, binary, decimal, time (as text)
    Array,         // Array(String)
    Map,           // Map(String, String)
};

struct VariantField
{
    VariantFieldKind kind = VariantFieldKind::Null;
    Field value; // matches the member column for `kind` (empty for Null)
};

/// Codec for the Parquet/Iceberg "variant" logical type binary encoding.
/// A variant value is a pair of binary blobs: `metadata` (a dictionary of object keys) and
/// `value` (the tagged value itself). See
/// https://github.com/apache/parquet-format/blob/master/VariantEncoding.md

/// Decode a variant value into JSON text written to `out`.
void decodeVariantToJSON(std::string_view metadata, std::string_view value, WriteBuffer & out);

/// Convenience wrapper returning the JSON text as a String.
String decodeVariantToJSONString(std::string_view metadata, std::string_view value);

struct VariantBinary
{
    String metadata;
    String value;
};

/// Encode JSON text into the variant binary representation (metadata + value).
VariantBinary encodeJSONToVariant(std::string_view json);

/// Decode a variant value into a ClickHouse Field for the fixed Variant carrier (see above).
/// Nested objects/arrays are decoded one level deep with their elements rendered to String.
VariantField decodeVariantToField(std::string_view metadata, std::string_view value);

/// Encode a ClickHouse value of the given type directly into variant binary (type-preserving,
/// no JSON intermediary). Supports scalars, Array, Tuple and Map.
VariantBinary encodeVariant(const Field & field, const IDataType & type);

}
