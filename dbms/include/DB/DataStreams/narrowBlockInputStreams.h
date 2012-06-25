#pragma once

#include <DB/DataStreams/IBlockInputStream.h>


namespace DB
{

/** Если количество источников inputs больше width,
  *  то клеит источники друг с другом (с помощью ConcatBlockInputStream),
  *  чтобы количество источников стало не больше width.
  *
  * Старается клеить источники друг с другом равномерно.
  */
BlockInputStreams narrowBlockInputStreams(BlockInputStreams & inputs, size_t width);

}
