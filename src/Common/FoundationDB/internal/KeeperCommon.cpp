#include "KeeperCommon.h"
#include "Common/Exception.h"

namespace DB::ErrorCodes
{
extern const int FDB_EXCEPTION;
}

namespace DB::FoundationDB
{
KeeperException::KeeperException(Coordination::Error error_)
    : Exception(Coordination::errorMessage(error_), ErrorCodes::FDB_EXCEPTION), error(error_)
{
}

const UInt8 KeeperKeys::ListFilterEphemeral = (1 << 7);
}
