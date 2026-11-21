#include <Storages/PostgreSQL/MaterializedPostgreSQLSettings.h>

#if USE_LIBPQXX

#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTFunction.h>
#include <Core/ServerUUID.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_SETTING;
}

#define LIST_OF_MATERIALIZED_POSTGRESQL_SETTINGS(DECLARE, ALIAS) \
    DECLARE(UInt64, materialized_postgresql_max_block_size, 65536, "Number of row collected before flushing data into table.", 0) \
    DECLARE(String, materialized_postgresql_tables_list, "", "List of tables for MaterializedPostgreSQL database engine", 0) \
    DECLARE(String, materialized_postgresql_unmaterialized_tables_list, "", \
        "Internal, set automatically: the published tables of a whole-schema MaterializedPostgreSQL database that were never " \
        "materialized (for example, their initial snapshot has failed). It is persisted together with `materialized_postgresql_tables_list` " \
        "by `ATTACH TABLE` and `DETACH TABLE PERMANENTLY`, so that `BACKUP` keeps refusing to omit these tables after a restart", 0) \
    DECLARE(String, materialized_postgresql_schema_list, "", "List of schemas for MaterializedPostgreSQL database engine", 0) \
    DECLARE(String, materialized_postgresql_replication_slot, "", "A user-created replication slot", 0) \
    DECLARE(String, materialized_postgresql_snapshot, "", "User provided snapshot in case he manages replication slots himself", 0) \
    DECLARE(String, materialized_postgresql_schema, "", "PostgreSQL schema", 0) \
    DECLARE(Bool, materialized_postgresql_tables_list_with_schema, false, \
        "Consider by default that if there is a dot in tables list 'name.name', " \
        "then the first name is postgres schema and second is postgres table. This setting is needed to allow table names with dots", 0) \
    DECLARE(UInt64, materialized_postgresql_backoff_min_ms, 200, "Poll backoff start point", 0) \
    DECLARE(UInt64, materialized_postgresql_backoff_max_ms, 10000, "Poll backoff max point", 0) \
    DECLARE(UInt64, materialized_postgresql_backoff_factor, 2, "Poll backoff factor", 0) \
    DECLARE(Bool, materialized_postgresql_use_unique_replication_consumer_identifier, false, "Should a unique consumer be registered for table replication", 0) \
    DECLARE(Bool, materialized_postgresql_use_unique_replication_consumer_identifier_mixed_with_server_uuid, true, \
        "Mix the server UUID into the unique replication consumer identifier, so that the replication slot and the publication " \
        "are unique per server, which is needed for objects created with `ON CLUSTER`. Has effect only together with " \
        "`materialized_postgresql_use_unique_replication_consumer_identifier`. An object that was created without this setting " \
        "in its definition (by an older version) behaves as if it is set to 0.", 0) \
    DECLARE(String, materialized_postgresql_unique_replication_consumer_server_uuid, "", \
        "The server UUID that is mixed into the unique replication consumer identifier. It is written into the definition " \
        "when the object is created, so that the names of the replication slot and the publication do not change " \
        "if the server UUID is regenerated. It is not supposed to be set manually.", 0) \
    DECLARE(Bool, materialized_postgresql_use_extended_date_and_time_types, true, \
        "Map PostgreSQL `date` and `timestamp`/`timestamptz` types to ClickHouse `Date32` and `DateTime64` " \
        "(which cover the wider PostgreSQL value range). When disabled, the narrower `Date` and `DateTime` types are used.", 0) \

DECLARE_SETTINGS_TRAITS(MaterializedPostgreSQLSettingsTraits, LIST_OF_MATERIALIZED_POSTGRESQL_SETTINGS, MATERIALIZED_POSTGRESQL_SETTINGS_SUPPORTED_TYPES)
IMPLEMENT_SETTINGS_TRAITS(MaterializedPostgreSQLSettingsTraits, LIST_OF_MATERIALIZED_POSTGRESQL_SETTINGS, MaterializedPostgreSQLSettings, MaterializedPostgreSQLSetting)

MaterializedPostgreSQLSettings::MaterializedPostgreSQLSettings() : impl(std::make_unique<MaterializedPostgreSQLSettingsImpl>())
{
}

MaterializedPostgreSQLSettings::MaterializedPostgreSQLSettings(const MaterializedPostgreSQLSettings & settings)
    : impl(std::make_unique<MaterializedPostgreSQLSettingsImpl>(*settings.impl))
{
}

MaterializedPostgreSQLSettings::MaterializedPostgreSQLSettings(MaterializedPostgreSQLSettings && settings) noexcept = default;

MaterializedPostgreSQLSettings::~MaterializedPostgreSQLSettings() = default;

MATERIALIZED_POSTGRESQL_SETTINGS_SUPPORTED_TYPES(MaterializedPostgreSQLSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)

void MaterializedPostgreSQLSettings::applyChange(const SettingChange & change)
{
    impl->applyChange(change);
}

bool MaterializedPostgreSQLSettings::has(std::string_view name) const
{
    return impl->has(name);
}

void MaterializedPostgreSQLSettings::loadFromQuery(ASTStorage & storage_def)
{
    if (storage_def.settings)
    {
        try
        {
            impl->applyChanges(storage_def.settings->changes);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::UNKNOWN_SETTING)
                e.addMessage("for storage " + storage_def.engine->name);
            throw;
        }
    }
    else
    {
        auto settings_ast = make_intrusive<ASTSetQuery>();
        settings_ast->is_standalone = false;
        storage_def.set(storage_def.settings, settings_ast);
    }
}

void MaterializedPostgreSQLSettings::applyDefaultsForNewOrExistingObject(ASTStorage & storage_def, bool is_attach)
{
    auto set_in_definition = [&](const String & name, Field value)
    {
        if (!storage_def.settings)
        {
            auto settings_ast = make_intrusive<ASTSetQuery>();
            settings_ast->is_standalone = false;
            storage_def.set(storage_def.settings, settings_ast);
        }
        auto & changes = storage_def.settings->changes;
        if (auto it = std::find_if(changes.begin(), changes.end(), [&](const auto & change) { return change.name == name; });
            it != changes.end())
            it->value = std::move(value);
        else
            changes.emplace_back(name, std::move(value));
    };

    /// The setting changes the names of the replication slot and the publication on the PostgreSQL side.
    /// New objects get it by default, and it is written into the definition explicitly to be preserved on restart.
    /// Objects created by older versions do not have it in their definition and continue to use the old names.
    auto & mixed_with_server_uuid = (*this)[MaterializedPostgreSQLSetting::materialized_postgresql_use_unique_replication_consumer_identifier_mixed_with_server_uuid];
    if (!mixed_with_server_uuid.changed)
    {
        if (is_attach)
        {
            mixed_with_server_uuid = false;
        }
        else
        {
            set_in_definition("materialized_postgresql_use_unique_replication_consumer_identifier_mixed_with_server_uuid", Field(true));
            mixed_with_server_uuid = true;
        }
    }

    if (!(*this)[MaterializedPostgreSQLSetting::materialized_postgresql_use_unique_replication_consumer_identifier] || !mixed_with_server_uuid)
        return;

    /// `ServerUUID` is regenerated if the `uuid` file in the server data directory is lost. The replication slot and
    /// the publication of an existing object must not change in that case: a new replication slot would reload
    /// the snapshot into the existing nested tables. So the server UUID is written into the definition as well.
    /// A new object always gets the UUID of the server where it is created, even if the definition already has one:
    /// it may be copied from another object, e.g. by `CREATE TABLE ... ON CLUSTER ... AS src`, which is normalized
    /// on the initiator, and then all replicas would use the same replication slot and publication.
    auto & server_uuid = (*this)[MaterializedPostgreSQLSetting::materialized_postgresql_unique_replication_consumer_server_uuid];
    if (!is_attach || server_uuid.value.empty())
    {
        server_uuid = toString(ServerUUID::get());
        set_in_definition("materialized_postgresql_unique_replication_consumer_server_uuid", Field(server_uuid.value));
    }
    else
    {
        /// Throws if it is not a valid UUID.
        parseFromString<UUID>(server_uuid.value);
    }
}

void MaterializedPostgreSQLSettings::checkConsistencyForFreshDefinition() const
{
    /// A user-managed replication slot has a single fixed name, which is shared by every replica of a database or table
    /// created with `ON CLUSTER`, while the unique replication consumer identifier exists to make the consumer unique.
    if (!(*this)[MaterializedPostgreSQLSetting::materialized_postgresql_replication_slot].value.empty()
        && (*this)[MaterializedPostgreSQLSetting::materialized_postgresql_use_unique_replication_consumer_identifier])
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Setting `materialized_postgresql_replication_slot` cannot be used together with "
            "`materialized_postgresql_use_unique_replication_consumer_identifier`: a user-managed replication slot has a single "
            "fixed name and cannot be unique per consumer. Remove one of these settings");
}

bool MaterializedPostgreSQLSettings::hasBuiltin(std::string_view name)
{
    return MaterializedPostgreSQLSettingsImpl::hasBuiltin(name);
}
}

#endif
