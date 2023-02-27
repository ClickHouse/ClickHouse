#pragma once

#include <DataTypes/IDataType.h>
#include <IO/ReadBuffer.h>

namespace DB
{

/// Struct with some additional information about inferred types for JSON formats.
struct JSONInferenceInfo
{
    /// We store numbers that were parsed from strings.
    /// It's used in types transformation to change such numbers back to string if needed.
    std::unordered_set<const IDataType *> numbers_parsed_from_json_strings;
    /// Indicates if currently we are inferring type for Map/Object key.
    bool is_object_key = false;
};

/// Try to determine datatype of the value in buffer/string. If the type cannot be inferred, return nullptr.
/// In general, it tries to parse a type using the following logic:
/// If we see '[', we try to parse an array of values and recursively determine datatype for each element.
/// If we see '(', we try to parse a tuple of values and recursively determine datatype for each element.
/// If we see '{', we try to parse a Map of keys and values and recursively determine datatype for each key/value.
/// If we see a quote '\'', we treat it as a string and read until next quote.
/// If we see NULL it returns Nullable(Nothing)
/// Otherwise we try to read a number.
DataTypePtr tryInferDataTypeForSingleField(ReadBuffer & buf, const FormatSettings & settings);
DataTypePtr tryInferDataTypeForSingleField(std::string_view field, const FormatSettings & settings);

/// The same as tryInferDataTypeForSingleField, but for JSON values.
DataTypePtr tryInferDataTypeForSingleJSONField(ReadBuffer & buf, const FormatSettings & settings, JSONInferenceInfo * json_info);
DataTypePtr tryInferDataTypeForSingleJSONField(std::string_view field, const FormatSettings & settings, JSONInferenceInfo * json_info);

/// Try to parse Date or DateTime value from a string.
DataTypePtr tryInferDateOrDateTimeFromString(std::string_view field, const FormatSettings & settings);

/// Try to parse a number value from a string. By default, it tries to parse Float64,
/// but if setting try_infer_integers is enabled, it also tries to parse Int64.
DataTypePtr tryInferNumberFromString(std::string_view field, const FormatSettings & settings);

/// It takes two types inferred for the same column and tries to transform them to a common type if possible.
/// It's also used when we try to infer some not ordinary types from another types.
/// Example 1:
///     Dates inferred from strings. In this case we should check if dates were inferred from all strings
///     in the same way and if not, transform inferred dates back to strings.
///     For example, when we have Array(Date) (like `['2020-01-01', '2020-02-02']`) and Array(String) (like `['string', 'abc']`
///     we will convert the first type to Array(String).
/// Example 2:
///     When we have integers and floats for the same value, we should convert all integers to floats.
///     For example, when we have Array(Int64) (like `[123, 456]`) and Array(Float64) (like `[42.42, 4.42]`)
///     we will convert the first type to Array(Float64)
/// Example 3:
///     When we have not complete types like Nullable(Nothing), Array(Nullable(Nothing)) or Tuple(UInt64, Nullable(Nothing)),
///     we try to complete them using the other type.
///     For example, if we have Tuple(UInt64, Nullable(Nothing)) and Tuple(Nullable(Nothing), String) we will convert both
///     types to common type Tuple(Nullable(UInt64), Nullable(String))
void transformInferredTypesIfNeeded(DataTypePtr & first, DataTypePtr & second, const FormatSettings & settings);

/// The same as transformInferredTypesIfNeeded but uses some specific transformations for JSON.
/// Example 1:
///     When we have numbers inferred from strings and strings, we convert all such numbers back to string.
///     For example, if we have Array(Int64) (like `['123', '456']`) and Array(String) (like `['str', 'abc']`)
///     we will convert the first type to Array(String). Note that we collect information about numbers inferred
///     from strings in json_info while inference and use it here, so we will know that Array(Int64) contains
///     integer inferred from a string.
/// Example 2:
///     When we have maps with different value types, we convert all types to JSON object type.
///     For example, if we have Map(String, UInt64) (like `{"a" : 123}`) and Map(String, String) (like `{"b" : 'abc'}`)
///     we will convert both types to Object('JSON').
void transformInferredJSONTypesIfNeeded(DataTypePtr & first, DataTypePtr & second, const FormatSettings & settings, JSONInferenceInfo * json_info);

/// Check if type is Tuple(...), try to transform nested types to find a common type for them and if all nested types
/// are the same after transform, we convert this tuple to an Array with common nested type.
/// For example, if we have Tuple(String, Nullable(Nothing)) we will convert it to Array(String).
/// It's used when all rows were read and we have Tuple in the result type that can be actually an Array.
void transformJSONTupleToArrayIfPossible(DataTypePtr & data_type, const FormatSettings & settings, JSONInferenceInfo * json_info);

/// Make type Nullable recursively:
/// - Type -> Nullable(type)
/// - Array(Type) -> Array(Nullable(Type))
/// - Tuple(Type1, ..., TypeN) -> Tuple(Nullable(Type1), ..., Nullable(TypeN))
/// - Map(KeyType, ValueType) -> Map(KeyType, Nullable(ValueType))
/// - LowCardinality(Type) -> LowCardinality(Nullable(Type))
DataTypePtr makeNullableRecursively(DataTypePtr type);

/// Call makeNullableRecursively for all types
/// in the block and return names and types.
NamesAndTypesList getNamesAndRecursivelyNullableTypes(const Block & header);

/// Check if type contains Nothing, like Array(Tuple(Nullable(Nothing), String))
bool checkIfTypeIsComplete(const DataTypePtr & type);

}
