#pragma once

namespace DB
{

std::pair<bool, size_t> fileSegmentationEngineJSONEachRowImpl(ReadBuffer & in, DB::Memory<> & memory, size_t min_chunk_size);

}
