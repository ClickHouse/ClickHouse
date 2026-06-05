#pragma once
#include <DataTypes/Serializations/ISerialization.h>

namespace DB
{

/// When we deserialize several subcolumns of the same column it's always better to deserialize
/// subcolumns in order of their serialization, so we can avoid seeks back in the data files.
/// This function determines this order.
/// `column_id_in_storage` is the physical stream name prefix (column ID when
/// the table uses column IDs, otherwise the logical name).
/// `logical_name_in_storage` is the logical column name; when non-empty it
/// is required by `getFileNameForStreamByColumnId` to detect shared Nested
/// offsets streams that depend on the logical Nested parent.
std::vector<size_t> getSubcolumnsDeserializationOrder(
    const String & column_id_in_storage,
    const String & logical_name_in_storage,
    const std::vector<ISerialization::SubstreamData> & subcolumns_data,
    const std::vector<String> & substreams_in_serialization_order,
    ISerialization::EnumerateStreamsSettings & enumerate_settings,
    const ISerialization::StreamFileNameSettings & stream_file_name_settings);

}
