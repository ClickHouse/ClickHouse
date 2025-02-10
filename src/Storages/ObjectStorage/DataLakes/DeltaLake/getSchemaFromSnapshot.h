#pragma once

#include "config.h"

#if USE_DELTA_KERNEL_RS

#include <Core/NamesAndTypes.h>

namespace ffi
{
struct SharedSnapshot;
}

namespace DeltaLake
{

DB::NamesAndTypesList getSchemaFromSnapshot(ffi::SharedSnapshot * snapshot);

}

#endif
