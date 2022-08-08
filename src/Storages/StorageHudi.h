#pragma once

#include "config_core.h"

#include <Storages/IStorage.h>

namespace DB
{

class StorageHudi final : public IStorage {
public:
    StorageHudi(
        const StorageID & table_id_,
        ColumnsDescription columns_description_,
        ConstraintsDescription constraints_description_,
        const String & comment);

    String getName() const override { return "Hudi"; }
};

}
