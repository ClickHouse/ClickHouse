#pragma once

#include <Core/Block.h>


namespace DB
{

/** Converts columns-constants to full columns ("materializes" them).
  */
Block materializeBlock(const Block & block);

}
