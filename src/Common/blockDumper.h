#pragma once

#include <base/types.h>

namespace DB
{

class Block;
class Chunk;

String dumpBlockToJSON(const Block & block);
String dumpChunkToJSON(const Chunk & chunk, const Block & header);

}
