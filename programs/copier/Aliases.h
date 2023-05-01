#pragma once

#include <base/types.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <utility>

namespace DB
{
    using ConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;

    using DatabaseAndTableName = std::pair<String, String>;
    using ListOfDatabasesAndTableNames = std::vector<DatabaseAndTableName>;
}
