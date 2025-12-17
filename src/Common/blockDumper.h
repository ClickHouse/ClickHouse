#pragma once

#include <base/types.h>

namespace DB
{

class Block;
class Chunk;
class WriteBuffer;

/// Dump the contents of a Block/Chunk to the provided WriteBuffer in JSONEachRow format.
/// These functions are intended for debugging purposes.
void dumpBlockToJSON(const Block & block, WriteBuffer & buffer);
void dumpChunkToJSON(const Chunk & chunk, const Block & header, WriteBuffer & buffer);

}
