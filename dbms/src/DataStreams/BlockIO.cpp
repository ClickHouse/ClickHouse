#include <Interpreters/ProcessList.h>
#include <DataStreams/BlockIO.h>

namespace DB
{

BlockIO::~BlockIO() = default;
BlockIO::BlockIO() = default;
BlockIO::BlockIO(const BlockIO &) = default;

}
