#pragma once

#include "config.h"

#if USE_LANCE

#include <Core/Names.h>
#include <Core/Types.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceTableStateSnapshot.h>

#include <optional>
#include <vector>

namespace DB::Lance
{

struct ScanDescription
{
    TableStateSnapshot snapshot;
    Names projection;
    std::optional<String> predicate;
    size_t max_block_size = 0;
    bool need_only_count = false;
    bool discard_output_columns = false;
};

}

#endif
