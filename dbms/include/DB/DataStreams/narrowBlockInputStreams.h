#pragma once

#include <DB/DataStreams/IBlockInputStream.h>


namespace DB
{

/** Если количество источников inputs больше width,
  *  то клеит источники друг с другом (с помощью ConcatBlockInputStream),
  *  чтобы количество источников стало не больше width.
  *
  * Старается клеить источники друг с другом равномерно-случайно.
  *  (чтобы избежать перевеса в случае, если распределение количества данных в разных источниках подчиняется некоторому шаблону)
  */
BlockInputStreams narrowBlockInputStreams(BlockInputStreams & inputs, size_t width);

}
