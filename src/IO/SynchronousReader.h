#pragma once

#include <IO/AsynchronousReader.h>


namespace DB
{

/** Implementation of IAsynchronousReader that in fact synchronous.
  * The only addition is posix_fadvise.
  */
class SynchronousReader final : public IAsynchronousReader
{
public:
    std::future<Result> submit(Request request) override;
};

}

