#pragma once
#include <Common/Exception.h>

namespace ffi
{
struct EnginePredicate;
}

namespace DB
{
class ActionsDAG;
}

namespace DeltaLake
{
std::unique_ptr<ffi::EnginePredicate> getEnginePredicate(const DB::ActionsDAG & filter, std::exception_ptr & exception);
}
