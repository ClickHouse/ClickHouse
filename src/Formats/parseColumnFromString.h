#pragma once

#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromString.h>

namespace DB
{

/// Parse one value's whole-text representation into a single-row column of the
/// given type. Throws when the text does not fit the type. Shared by the paths
/// that turn HTTP request header strings into typed INSERT column values: the
/// sync transform, async push-time validation, and async flush-time parsing all
/// deserialize the same way, so keeping one helper prevents them from drifting.
inline MutableColumnPtr parseColumnValueFromString(
    const DataTypePtr & type, const String & text, const FormatSettings & format_settings)
{
    auto column = type->createColumn();
    ReadBufferFromString buf(text);
    type->getDefaultSerialization()->deserializeWholeText(*column, buf, format_settings);
    return column;
}

}
