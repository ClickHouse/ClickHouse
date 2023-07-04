#pragma once
#include <Core/NamesAndTypes.h>

namespace DB
{

NamesAndTypesList getVirtualsForStorage(const NamesAndTypesList & storage_columns_, const NamesAndTypesList & default_virtuals_);

}
