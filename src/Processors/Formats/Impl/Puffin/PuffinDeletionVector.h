#pragma once

#include <Processors/Formats/Impl/Puffin/PuffinBlobBuilder.h>
#include <Processors/Formats/Impl/Puffin/PuffinCommon.h>

#include <roaring/roaring64map.hh>

namespace DB
{

PuffinBlobBuilderPtr createPuffinDeletionVectorBuilder(const Block & header);

String serializeDeletionVectorBlob(roaring::Roaring64Map & positions);

PuffinBlob makeDeletionVectorBlobMetadata(const String & referenced_data_file, UInt64 cardinality);

}
