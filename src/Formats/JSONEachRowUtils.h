#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>
#include <utility>

namespace DB
{

std::pair<bool, size_t> fileSegmentationEngineJSONEachRowImpl(ReadBuffer & in, DB::Memory<> & memory, size_t min_chunk_size);

bool nonTrivialPrefixAndSuffixCheckerJSONEachRowImpl(ReadBuffer & buf);

}
