#pragma once
#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/IDataType.h>

namespace DB
{

/// When we deserialize several subcolumns of the same column it's always better to deserialize
/// subcolumns in order of their serialization, so we can avoid seeks back in the data files.
/// This function determines this order.
std::vector<size_t> getSubcolumnsDeserializationOrder(
    const String & column_name,
    const std::vector<ISerialization::SubstreamData> & subcolumns_data,
    const std::vector<String> & substreams_in_serialization_order,
    ISerialization::EnumerateStreamsSettings & enumerate_settings,
    const ISerialization::StreamFileNameSettings & stream_file_name_settings);

}
