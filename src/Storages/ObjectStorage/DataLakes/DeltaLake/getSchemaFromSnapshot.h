#pragma once

#include "config.h"

#if USE_DELTA_KERNEL_RS

#include <Core/NamesAndTypes.h>

namespace ffi
{
struct SharedSnapshot;
struct SharedExternEngine;
}

namespace DeltaLake
{

DB::NamesAndTypesList getTableSchemaFromSnapshot(ffi::SharedSnapshot * snapshot);
DB::NamesAndTypesList getReadSchemaFromSnapshot(ffi::SharedSnapshot * snapshot, ffi::SharedExternEngine * engine);

}

#endif
