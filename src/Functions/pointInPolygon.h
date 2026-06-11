#pragma once

#include <cstddef>

namespace DB
{

/** Sets the maximum size in bytes of the cache of preprocessed constant polygons
  * used by the function pointInPolygon. Entries above the limit are evicted
  * immediately, so lowering the limit shrinks the cache right away.
  */
void setPointInPolygonCacheMaxSizeInBytes(size_t max_size_in_bytes);

}
