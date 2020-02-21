#pragma once

#include <DataStreams/IBlockStream_fwd.h>

#include <atomic>
#include <functional>


namespace DB
{

class Block;

/** Copies data from the InputStream into the OutputStream
  * (for example, from the database to the console, etc.)
  */
void copyData(IBlockInputStream & from, IBlockOutputStream & to, std::atomic<bool> * is_cancelled = nullptr);

void copyData(IBlockInputStream & from, IBlockOutputStream & to, const std::function<bool()> & is_cancelled);

void copyData(IBlockInputStream & from, IBlockOutputStream & to, const std::function<bool()> & is_cancelled,
              const std::function<void(const Block & block)> & progress);

}
