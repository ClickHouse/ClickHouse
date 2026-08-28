#include <Storages/System/StorageSystemChangelog.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{

/// Configurable variable specifying where to fetch the changelog from.
/// Hosted as a static asset alongside the official ClickHouse documentation.
static const String CHANGELOG_URL = "https://clickhouse.com/docs/resources/changelogs/changelog.csv";
static const String CHANGELOG_FORMAT = "CSVWithNames";
static const String CHANGELOG_COMPRESSION = "auto";

StorageSystemChangelog::StorageSystemChangelog(
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ContextPtr & context_)
    : StorageURL(
        CHANGELOG_URL,
        table_id_,
        CHANGELOG_FORMAT,
        /* format_settings = */ std::nullopt,
        columns_,
        ConstraintsDescription{},
        /* comment = */ "System changelog virtual table",
        context_,
        CHANGELOG_COMPRESSION)
{
}

ColumnsDescription StorageSystemChangelog::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"version", std::make_shared<DataTypeString>(), "ClickHouse release version (e.g. 24.12)"},
        {"date", std::make_shared<DataTypeDate>(), "Release date"},
        {"type", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Change category (e.g. New Feature, Bug Fix)"},
        {"action", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())), "Action verb (e.g. Added, Fixed, Enabled, Disabled, Removed, Optimized)"},
        {"is_experimental", std::make_shared<DataTypeUInt8>(), "Whether the change relates to an experimental feature"},
        {"is_breaking", std::make_shared<DataTypeUInt8>(), "Whether the change is backward-incompatible or breaking"},
        {"is_security_fix", std::make_shared<DataTypeUInt8>(), "Whether the change is a security fix"},
        {"setting_name", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Configuration or query setting name associated with the entry"},
        {"default_enabled", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>()), "Whether the setting or feature is enabled by default (1 = enabled, 0 = disabled, NULL = unspecified)"},
        {"pull_request", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt32>()), "GitHub pull request number"},
        {"author", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Author / contributor handle"},
        {"description", std::make_shared<DataTypeString>(), "Description of the change"}
    };
}

}
