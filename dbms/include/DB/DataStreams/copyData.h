#pragma once

#include <atomic>


namespace DB
{

class IBlockInputStream;
class IBlockOutputStream;

/** Копирует данные из InputStream в OutputStream
  * (например, из БД в консоль и т. п.)
  */
void copyData(IBlockInputStream & from, IBlockOutputStream & to, std::atomic<bool> * is_cancelled = nullptr);

}
