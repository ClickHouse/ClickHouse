#pragma once

#include <DB/DataStreams/ForkBlockInputStreams.h>


namespace DB
{

/** Если переданные источники (конвейеры выполнения запроса) имеют одинаковые части,
  *  то склеивает эти части, заменяя на один источник и вставляя "вилки" (размножители).
  * Это используется для однопроходного выполнения нескольких запросов.
  *
  * Для выполнения склеенного конвейера, все inputs и forks должны использоваться в разных потоках.
  */
void glueBlockInputStreams(BlockInputStreams & inputs, Forks & forks);

}
