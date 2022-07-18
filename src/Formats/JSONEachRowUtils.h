#pragma once

#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Formats/FormatSettings.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>
#include <utility>

namespace DB
{

std::pair<bool, size_t> fileSegmentationEngineJSONEachRow(ReadBuffer & in, DB::Memory<> & memory, size_t min_chunk_size);
std::pair<bool, size_t> fileSegmentationEngineJSONCompactEachRow(ReadBuffer & in, DB::Memory<> & memory, size_t min_chunk_size, size_t min_rows);


/// Parse JSON from string and convert it's type to ClickHouse type. Make the result type always Nullable.
/// JSON array with different nested types is treated as Tuple.
/// If cannot convert (for example when field contains null), return nullptr.
DataTypePtr getDataTypeFromJSONField(const String & field);

/// Read row in JSONEachRow format and try to determine type for each field.
/// Return list of names and types.
/// If cannot determine the type of some field, return nullptr for it.
NamesAndTypesList readRowAndGetNamesAndDataTypesForJSONEachRow(ReadBuffer & in, bool json_strings);

/// Read row in JSONCompactEachRow format and try to determine type for each field.
/// If cannot determine the type of some field, return nullptr for it.
DataTypes readRowAndGetDataTypesForJSONCompactEachRow(ReadBuffer & in, bool json_strings);

bool nonTrivialPrefixAndSuffixCheckerJSONEachRowImpl(ReadBuffer & buf);

bool readFieldImpl(ReadBuffer & in, IColumn & column, const DataTypePtr & type, const SerializationPtr & serialization, const String & column_name, const FormatSettings & format_settings, bool yield_strings);

}
