#pragma once

#include <DataStreams/ForkBlockInputStreams.h>


namespace DB
{

/** If passed sources (query execution pipelines) have the same parts,
  *  then glues these parts, replacing them with one source and inserting "forks" (multipliers).
  * This is used for single-pass execution of multiple queries.
  *
  * To execute a glued pipeline, all `inputs` and `forks` must be used in different threads.
  */
void glueBlockInputStreams(BlockInputStreams & inputs, Forks & forks);

}
