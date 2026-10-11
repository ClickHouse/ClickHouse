#pragma once

#include <Storages/StorageURL.h>

namespace DB
{

class Context;

/** System table "changelog" that dynamically fetches and streams ClickHouse changelog
  * from a remote or local CSV/JSON source using the URL table engine.
  */
class StorageSystemChangelog final : public StorageURL
{
public:
    StorageSystemChangelog(const StorageID & table_id_, const ColumnsDescription & columns_, const ContextPtr & context_);

    std::string getName() const override
    {
        return "SystemChangelog";
    }

    static ColumnsDescription getColumnsDescription();
};

}
