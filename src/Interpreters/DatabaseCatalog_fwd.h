#pragma once

#include <base/types.h>

#include <map>
#include <memory>

namespace DB
{
class DDLGuard;
using DDLGuardPtr = std::unique_ptr<DDLGuard>;

struct TemporaryTableHolder;
using TemporaryTablesMapping = std::map<String, std::shared_ptr<TemporaryTableHolder>>;

class DatabaseCatalog;
}
