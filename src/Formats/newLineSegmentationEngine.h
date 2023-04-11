#pragma once

#include <IO/ReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>

namespace DB
{
    std::pair<bool, size_t> newLineFileSegmentationEngine(ReadBuffer & in, DB::Memory<> & memory, size_t min_bytes, size_t max_rows);
}
