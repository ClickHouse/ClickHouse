#pragma once

#include <Processors/Formats/Impl/PuffinCommon.h>

#include <roaring/roaring64map.hh>

namespace DB
{

class PuffinWriter;

inline constexpr Int32 ICEBERG_ROW_POSITION_FIELD_ID = std::numeric_limits<Int32>::max() - 2;

void validateDeletionVectorPosition(UInt64 position);

String serializeDeletionVectorBlob(roaring::Roaring64Map & positions);

PuffinBlob makeDeletionVectorBlobMetadata(const String & referenced_data_file, UInt64 cardinality);

PuffinBlob writeDeletionVectorBlob(PuffinWriter & writer, const String & referenced_data_file, roaring::Roaring64Map & positions);

}
