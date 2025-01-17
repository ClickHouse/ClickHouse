#pragma once
#include <Core/NamesAndTypes.h>

namespace ffi
{
struct SharedSnapshot;
}

namespace DeltaLake
{

DB::NamesAndTypesList getSchemaFromSnapshot(ffi::SharedSnapshot * snapshot);

}
