#pragma once

namespace DB
{

bool fileSegmentationEngineJSONEachRowImpl(ReadBuffer & in, DB::Memory<> & memory, size_t min_chunk_size);

}
