#pragma once

#include <Columns/IColumn_fwd.h>
#include <Core/Names.h>
#include <DataTypes/IDataType.h>

#include <map>

namespace DB
{

struct TypedQueryParameter
{
    DataTypePtr type;
    ColumnPtr column;
    String value_hash;
    String scalar_name;
};

using TypedQueryParameters = std::map<String, TypedQueryParameter>;

struct QueryParameterBindings
{
    NameToNameMap text;
    TypedQueryParameters typed;
};

inline constexpr size_t MAX_TYPED_QUERY_PARAMETERS = 1024;
inline constexpr size_t MAX_TYPED_QUERY_PARAMETER_WIRE_BYTES = 64 * 1024 * 1024;
inline constexpr size_t MAX_TYPED_QUERY_PARAMETER_UNCOMPRESSED_BYTES = 64 * 1024 * 1024;
inline constexpr size_t MAX_TYPED_QUERY_PARAMETER_NESTING_DEPTH = 32;

void validateTypedQueryParameterType(const IDataType & type);
void validateTypedQueryParameters(const TypedQueryParameters & parameters, const NameToNameMap * text_parameters = nullptr);
String calculateTypedQueryParameterHash(const IDataType & type, const IColumn & column);
String makeTypedQueryParameterScalarName(const String & name, const String & value_hash);

}
