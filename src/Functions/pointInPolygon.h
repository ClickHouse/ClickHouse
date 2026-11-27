#pragma once

#include <cstddef>

namespace DB
{

/** Sets the maximum size in bytes of the cache of preprocessed constant polygons
  * used by the function pointInPolygon. Entries above the limit are evicted
  * immediately, so lowering the limit shrinks the cache right away.
  */
void setPointInPolygonCacheMaxSizeInBytes(size_t max_size_in_bytes);

/** Evicts all entries from the cache of preprocessed constant polygons used by the
  * function pointInPolygon. The configured size limit is left unchanged, so the cache
  * keeps accepting entries afterwards. Backs `SYSTEM DROP POINT IN POLYGON CACHE`.
  */
void clearPointInPolygonCache();

}
