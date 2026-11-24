#pragma once

#include <Processors/Formats/Impl/Puffin/PuffinBlobBuilder.h>

namespace DB
{

PuffinBlobBuilderPtr createPuffinDeletionVectorBuilder(const Block & header);

}
