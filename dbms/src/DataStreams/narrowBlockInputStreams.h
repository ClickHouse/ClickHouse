#pragma once

#include <DataStreams/IBlockInputStream.h>


namespace DB
{

/** If the number of sources of `inputs` is greater than `width`,
  *  then glues the sources to each other (using ConcatBlockInputStream),
  *  so that the number of sources becomes no more than `width`.
  *
  * Trying to glue the sources with each other uniformly randomly.
  *  (to avoid overweighting if the distribution of the amount of data in different sources is subject to some pattern)
  */
BlockInputStreams narrowBlockInputStreams(BlockInputStreams & inputs, size_t width);

}
