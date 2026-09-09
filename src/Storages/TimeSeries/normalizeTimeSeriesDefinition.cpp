#include <Storages/TimeSeries/normalizeTimeSeriesDefinition.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Core/Settings.h>
#include <Core/SettingsEnums.h>
#include <Core/UUID.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeCustomSimpleAggregateFunction.h>
#include <Access/Common/AccessType.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/StorageID.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/dataTypeToAST.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/IDatabase.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTTTLElement.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>
#include <Storages/TimeSeries/TimeSeriesIDGenerator.h>
#include <base/EnumReflection.h>
#include <algorithm>
#include <optional>
#include <unordered_map>
#include <unordered_set>


namespace DB
{

namespace TimeSeriesSetting
{
    extern const TimeSeriesSettingsBool aggregate_min_time_and_max_time;
    extern const TimeSeriesSettingsASTFunction id_generator;
    extern const TimeSeriesSettingsUInt64 recent_samples_index_granularity;
    extern const TimeSeriesSettingsASTFunction recent_samples_partition_by;
    extern const TimeSeriesSettingsUInt64 recent_samples_ttl_seconds;
    extern const TimeSeriesSettingsUInt64 samples_index_granularity;
    extern const TimeSeriesSettingsBool store_min_time_and_max_time;
    extern const TimeSeriesSettingsUInt64 tags_index_granularity;
    extern const TimeSeriesSettingsMap tags_to_columns;
    extern const TimeSeriesSettingsUInt64 version;
}

namespace Setting
{
    extern const SettingsDefaultTableEngine default_table_engine;
}

namespace ErrorCodes
{
    extern const int BAD_TYPE_OF_FIELD;
    extern const int INCORRECT_QUERY;
    extern const int INVALID_SETTING_VALUE;
    extern const int THERE_IS_NO_COLUMN;
    extern const int UNKNOWN_TABLE;
}


namespace
{
    /// All target kinds of a TimeSeries table.
    /// The RecentSamples target is optional: it's enabled by the `recent_samples_ttl_seconds` setting.
    constexpr std::array<ViewTarget::Kind, 4> getTargetKinds()
    {
        return {ViewTarget::Samples, ViewTarget::RecentSamples, ViewTarget::Tags, ViewTarget::Metrics};
    }

    /// Whether the create query defines inner columns for the specified target.
    bool hasInnerColumns(const ASTCreateQuery & create_query, ViewTarget::Kind kind)
    {
        return create_query.getTargetInnerColumns(kind) != nullptr;
    }

    /// Whether the create query defines an inner engine for the specified target.
    bool hasInnerEngine(const ASTCreateQuery & create_query, ViewTarget::Kind kind)
    {
        return create_query.getTargetInnerEngine(kind) != nullptr;
    }

    /// Whether the create query specifies an external table for the specified target.
    bool hasTargetTableID(const ASTCreateQuery & create_query, ViewTarget::Kind kind)
    {
        return create_query.hasTargetTableID(kind);
    }

    /// Whether the create query has an inner UUID for the specified target.
    bool hasInnerUUID(const ASTCreateQuery & create_query, ViewTarget::Kind kind)
    {
        return create_query.getTargetInnerUUID(kind) != UUIDHelpers::Nil;
    }

    /// Conflict-checking setter for `DataTypePtr`.
    /// Keeps the first non-null value, any subsequent non-null values must equal it.
    void setOrCheckDataType(
        DataTypePtr & target, String & target_source,
        const DataTypePtr & value, const String & value_source,
        std::string_view what, const StorageID & table_id)
    {
        if (!value)
            return;
        if (target)
        {
            if (!target->equals(*value))
                throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD,
                    "{}: Conflicting {} type: {} declares {} but {} declares {}",
                    table_id.getNameForLogs(), what,
                    target_source, target->getName(),
                    value_source, value->getName());
            return;
        }
        target = value;
        target_source = value_source;
    }

    /// Reads the declaration of the outer columns.
    /// If the `time_series` column is found and it is declared with type `Array(Tuple(timestamp_type, scalar_type))`,
    /// the function extracts `timestamp_type` and `scalar_type`.
    void readTypesFromOuterColumns(
        const ASTCreateQuery & query,
        DataTypePtr & timestamp_type, String & timestamp_src,
        DataTypePtr & scalar_type, String & scalar_src,
        const StorageID & table_id)
    {
        if (!query.columns_list || !query.columns_list->columns)
            return;

        for (const auto & column : query.columns_list->columns->children)
        {
            auto column_declaration = boost::static_pointer_cast<ASTColumnDeclaration>(column);
            const auto & name = column_declaration->name;

            if (name == TimeSeriesColumnNames::TimeSeries && column_declaration->getType())
            {
                auto column_type = DataTypeFactory::instance().get(column_declaration->getType());
                const auto * array_type = typeid_cast<const DataTypeArray *>(column_type.get());
                const auto * tuple_type = array_type ? typeid_cast<const DataTypeTuple *>(array_type->getNestedType().get()) : nullptr;
                if (!tuple_type || (tuple_type->getElements().size() != 2))
                    throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD,
                        "{}: Column `{}` must have type Array(Tuple(timestamp, value)), got {}",
                        table_id.getNameForLogs(), TimeSeriesColumnNames::TimeSeries, column_type->getName());

                const auto & elems = tuple_type->getElements();
                String source = "outer column `time_series`";
                setOrCheckDataType(timestamp_type, timestamp_src, elems[0], source, "timestamp", table_id);
                setOrCheckDataType(scalar_type, scalar_src, elems[1], source, "scalar", table_id);
            }

            /// Columns `id`, `timestamp`, `value` belong to the prealpha version and must not be here.
            if (name == TimeSeriesColumnNames::Timestamp
                || name == TimeSeriesColumnNames::Value
                || name == TimeSeriesColumnNames::ID)
            {
                throw Exception(ErrorCodes::INCORRECT_QUERY,
                    "{}: Column `{}` is not allowed in the column list of a TimeSeries table; "
                    "use INNER COLUMNS to specify inner table column types",
                    table_id.getNameForLogs(), name);
            }
        }
    }

    /// Reads SAMPLES INNER COLUMNS declarations and extracts types
    /// `timestamp_type`, `scalar_type`, `id_type`.
    void readTypesFromInnerSamples(
        const ASTCreateQuery & query,
        DataTypePtr & timestamp_type, String & timestamp_src,
        DataTypePtr & scalar_type, String & scalar_src,
        DataTypePtr & id_type, String & id_src,
        const StorageID & table_id)
    {
        const auto * inner_columns = query.getTargetInnerColumns(ViewTarget::Samples);
        if (!inner_columns || !inner_columns->columns)
            return;

        for (const auto & column : inner_columns->columns->children)
        {
            auto column_declaration = boost::static_pointer_cast<ASTColumnDeclaration>(column);
            if (!column_declaration->getType())
                continue;
            auto column_type = DataTypeFactory::instance().get(column_declaration->getType());

            if (column_declaration->name == TimeSeriesColumnNames::Timestamp)
                setOrCheckDataType(timestamp_type, timestamp_src, column_type, "samples inner column `timestamp`", "timestamp", table_id);
            else if (column_declaration->name == TimeSeriesColumnNames::Value)
                setOrCheckDataType(scalar_type, scalar_src, column_type, "samples inner column `value`", "scalar", table_id);
            else if (column_declaration->name == TimeSeriesColumnNames::ID)
                setOrCheckDataType(id_type, id_src, column_type, "samples inner column `id`", "id", table_id);
        }
    }

    /// Reads TAGS INNER COLUMNS declarations and extracts type `id_type`.
    void readTypesFromInnerTags(
        const ASTCreateQuery & query,
        DataTypePtr & id_type, String & id_src,
        const StorageID & table_id)
    {
        const auto * inner_columns = query.getTargetInnerColumns(ViewTarget::Tags);
        if (!inner_columns || !inner_columns->columns)
            return;

        for (const auto & column : inner_columns->columns->children)
        {
            auto column_declaration = boost::static_pointer_cast<ASTColumnDeclaration>(column);
            if (column_declaration->name != TimeSeriesColumnNames::ID)
                continue;

            if (column_declaration->getType())
            {
                auto column_type = DataTypeFactory::instance().get(column_declaration->getType());
                setOrCheckDataType(id_type, id_src, column_type, "tags inner column `id`", "id", table_id);
            }
        }
    }

    /// Reads the declaration of the external samples target table and
    /// extract types `timestamp_type`, `scalar_type`, id_type`.
    void readTypesFromExternalSamples(
        std::string_view table_kind_name,
        const StorageID & external_table_id, const ColumnsDescription & external_columns,
        DataTypePtr & timestamp_type, String & timestamp_src,
        DataTypePtr & scalar_type, String & scalar_src,
        DataTypePtr & id_type, String & id_src,
        const StorageID & table_id)
    {
        for (const auto & column : external_columns)
        {
            if (column.name == TimeSeriesColumnNames::Timestamp)
                setOrCheckDataType(timestamp_type, timestamp_src, column.type,
                    fmt::format("column `{}` of the external `{}` table {}", column.name, table_kind_name, external_table_id.getNameForLogs()),
                    "timestamp", table_id);
            else if (column.name == TimeSeriesColumnNames::Value)
                setOrCheckDataType(scalar_type, scalar_src, column.type,
                    fmt::format("column `{}` of the external `{}` table {}", column.name, table_kind_name, external_table_id.getNameForLogs()),
                    "scalar", table_id);
            else if (column.name == TimeSeriesColumnNames::ID)
                setOrCheckDataType(id_type, id_src, column.type,
                    fmt::format("column `{}` of the external `{}` table {}", column.name, table_kind_name, external_table_id.getNameForLogs()),
                    "id", table_id);
        }
    }

    /// Reads the declaration of the external tags target table and
    /// extract type `id_type`.
    void readTypesFromExternalTags(
        const StorageID & external_table_id, const ColumnsDescription & external_columns,
        DataTypePtr & id_type, String & id_src,
        const StorageID & table_id)
    {
        for (const auto & column : external_columns)
        {
            if (column.name != TimeSeriesColumnNames::ID)
                continue;

            setOrCheckDataType(id_type, id_src, column.type,
                fmt::format("column `{}` of the external `tags` table {}", column.name, external_table_id.getNameForLogs()),
                "id", table_id);
        }
    }

    /// Reads types from columns of the external target tables referenced in a CREATE query.
    void readTypesFromExternalTargets(
        const ASTCreateQuery & query, const ContextPtr & context,
        DataTypePtr & timestamp_type, String & timestamp_src,
        DataTypePtr & scalar_type, String & scalar_src,
        DataTypePtr & id_type, String & id_src,
        const StorageID & table_id)
    {
        auto resolve_external = [&](ViewTarget::Kind kind) -> std::pair<StorageID, ColumnsDescription>
        {
            auto external_table_id = query.getTargetTableID(kind);
            if (!external_table_id)
                return {StorageID::createEmpty(), {}};
            auto resolved_external_table_id = context->tryResolveStorageID(external_table_id);
            context->checkAccess(AccessType::SHOW_COLUMNS, resolved_external_table_id.database_name, resolved_external_table_id.table_name);
            auto external_table = DatabaseCatalog::instance().tryGetTable(resolved_external_table_id, context);
            if (!external_table)
                throw Exception(ErrorCodes::UNKNOWN_TABLE, "TimeSeries: Target table {} doesn't exist", external_table_id.getNameForLogs());
            auto external_metadata = external_table->getInMemoryMetadataPtr(context, false);
            return {external_table_id, external_metadata->columns};
        };

        auto [samples_id, samples_columns] = resolve_external(ViewTarget::Samples);
        if (!samples_id.empty())
            readTypesFromExternalSamples("samples", samples_id, samples_columns,
                                         timestamp_type, timestamp_src, scalar_type, scalar_src, id_type, id_src,
                                         table_id);

        /// An external recent-samples table has the same layout as an external samples table,
        /// and it can be the only declared source of the column types.
        auto [recent_samples_id, recent_samples_columns] = resolve_external(ViewTarget::RecentSamples);
        if (!recent_samples_id.empty())
            readTypesFromExternalSamples("recent samples", recent_samples_id, recent_samples_columns,
                                         timestamp_type, timestamp_src, scalar_type, scalar_src, id_type, id_src,
                                         table_id);

        auto [tags_id, tags_columns] = resolve_external(ViewTarget::Tags);
        if (!tags_id.empty())
            readTypesFromExternalTags(tags_id, tags_columns, id_type, id_src, table_id);
    }

    /// Reads the declared inner engines and extracts the family of the inner engines.
    /// All the inner tables must have the same family, otherwise their contents would diverge between replicas.
    void readInnerEngineFamilyFromInnerEngines(
        const ASTCreateQuery & query,
        std::optional<DefaultTableEngine> & inner_engine_family, String & inner_engine_family_src,
        const StorageID & table_id)
    {
        auto replication_type = [](DefaultTableEngine family) -> std::string_view
        {
            switch (family)
            {
                case DefaultTableEngine::ReplicatedMergeTree:
                    return "replicated";
                case DefaultTableEngine::SharedMergeTree:
                    return "shared";
                default:
                    return "not replicated";
            }
        };

        for (auto kind : getTargetKinds())
        {
            const auto * inner_engine = query.getTargetInnerEngine(kind);
            if (!inner_engine || !inner_engine->engine)
                continue;

            const String & engine_name = inner_engine->engine->name;
            DefaultTableEngine family = DefaultTableEngine::MergeTree;
            if (engine_name.starts_with("Replicated"))
                family = DefaultTableEngine::ReplicatedMergeTree;
            else if (engine_name.starts_with("Shared"))
                family = DefaultTableEngine::SharedMergeTree;

            String source = fmt::format("the inner {} table's engine {}", magic_enum::enum_name(kind), engine_name);
            if (inner_engine_family)
            {
                if (*inner_engine_family != family)
                    throw Exception(ErrorCodes::INCORRECT_QUERY,
                        "{}: {} is {} while {} is {}, but all the inner tables must have the same replication type",
                        table_id.getNameForLogs(),
                        inner_engine_family_src, replication_type(*inner_engine_family),
                        source, replication_type(family));
                continue;
            }
            inner_engine_family = family;
            inner_engine_family_src = source;
        }
    }

    /// Checks that all the inner tables have the same replication type, otherwise their contents would diverge
    /// between replicas. The generated inner engines follow the declared ones, but an engine copied from the old
    /// table by the clause `AS <other_table>` can differ from them.
    void checkInnerEnginesReplicationTypesMatch(const ASTCreateQuery & create_query, const StorageID & table_id)
    {
        std::optional<DefaultTableEngine> inner_engine_family;
        String inner_engine_family_src;
        readInnerEngineFamilyFromInnerEngines(create_query, inner_engine_family, inner_engine_family_src, table_id);
    }

    /// Resolved column types needed during normalization.
    struct ResolvedTimeSeriesTypes
    {
        DataTypePtr timestamp_type;
        DataTypePtr scalar_type;
        DataTypePtr id_type;

        /// The family of the engines of the inner tables: `MergeTree`, `ReplicatedMergeTree` or `SharedMergeTree`.
        /// Unset if no inner engine is declared, see `getDefaultInnerEngineFamily`.
        std::optional<DefaultTableEngine> inner_engine_family;
    };

    /// Resolves the types of the `timestamp`, `value` and `id` columns from the declarations in the query,
    /// then from `fallback_types` (the resolved types of the table from the clause `AS <other_table>`), then from the defaults.
    /// The external target tables are read always (if `check_external_targets` is true),
    /// or only if some type isn't declared in the query (if `check_external_targets_for_missing_types` is true),
    /// or never - on ATTACH they may not exist yet.
    /// `need_inner_engine_family` is set if the family of the inner engines is needed to generate inner engines.
    ResolvedTimeSeriesTypes resolveTimeSeriesTypes(
        const ASTCreateQuery & create_query,
        const ContextPtr & context,
        bool check_external_targets,
        bool check_external_targets_for_missing_types,
        bool need_inner_engine_family,
        const ResolvedTimeSeriesTypes * fallback_types)
    {
        StorageID table_id{create_query.getDatabase(), create_query.getTable()};

        /// A type declared in several places must be the same everywhere.
        ResolvedTimeSeriesTypes types;
        String timestamp_src;
        String scalar_src;
        String id_src;

        readTypesFromOuterColumns(create_query,
            types.timestamp_type, timestamp_src, types.scalar_type, scalar_src, table_id);

        readTypesFromInnerSamples(create_query,
            types.timestamp_type, timestamp_src, types.scalar_type, scalar_src, types.id_type, id_src, table_id);

        readTypesFromInnerTags(create_query,
            types.id_type, id_src, table_id);

        bool all_types_resolved = types.timestamp_type && types.scalar_type && types.id_type;
        if (check_external_targets || (check_external_targets_for_missing_types && !all_types_resolved))
        {
            readTypesFromExternalTargets(create_query, context,
                types.timestamp_type, timestamp_src,
                types.scalar_type, scalar_src,
                types.id_type, id_src,
                table_id);
        }

        if (need_inner_engine_family)
        {
            String inner_engine_family_src;
            readInnerEngineFamilyFromInnerEngines(create_query, types.inner_engine_family, inner_engine_family_src, table_id);
        }

        /// The types and the engines declared in the query win over the ones of the old table.
        if (fallback_types)
        {
            if (!types.timestamp_type)
                types.timestamp_type = fallback_types->timestamp_type;
            if (!types.scalar_type)
                types.scalar_type = fallback_types->scalar_type;
            if (!types.id_type)
                types.id_type = fallback_types->id_type;
            if (!types.inner_engine_family)
                types.inner_engine_family = fallback_types->inner_engine_family;
        }

        /// Apply defaults for unset types.
        if (!types.timestamp_type)
            types.timestamp_type = std::make_shared<DataTypeDateTime64>(3);
        if (!types.scalar_type)
            types.scalar_type = std::make_shared<DataTypeFloat64>();
        if (!types.id_type)
            types.id_type = std::make_shared<DataTypeTuple>(
                DataTypes{std::make_shared<DataTypeUInt64>(), std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeUUID>())});
        /// Validate types.
        {
            WhichDataType ts_which{*types.timestamp_type};
            if (!(ts_which.isDateTime64() || ts_which.isDateTime() || ts_which.isUInt32()))
                throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "{}: Unexpected type {} of the {} column",
                    table_id.getNameForLogs(), types.timestamp_type->getName(), TimeSeriesColumnNames::Timestamp);
        }
        {
            WhichDataType sc_which{*types.scalar_type};
            if (!(sc_which.isFloat64() || sc_which.isFloat32()))
                throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "{}: Unexpected type {} of the {} column",
                    table_id.getNameForLogs(), types.scalar_type->getName(), TimeSeriesColumnNames::Value);
        }
        {
            /// Identifiers can be of any comparable type: the id column is used in the sorting keys of the inner tables
            /// and in JOINs between them.
            const auto & id_type = types.id_type;
            bool id_ok = id_type->isComparable() && !id_type->isNullable() && !id_type->isLowCardinalityNullable()
                && !isNothing(*id_type) && !isVariant(*id_type) && !id_type->hasDynamicSubcolumns();
            if (!id_ok)
                throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD,
                    "{}: Unexpected type {} of the {} column, it must be a comparable non-Nullable type",
                    table_id.getNameForLogs(), id_type->getName(), TimeSeriesColumnNames::ID);
        }

        return types;
    }

    /// Removes the settings copied from the old table which this table must not inherit. `new_settings` is
    /// the SETTINGS clause of the query, which is applied on top of the copied settings afterwards.
    void removeOldSettingsDisabledByNewSettings(SettingsChanges & old_settings, const ASTSetQuery * new_settings)
    {
        /// The version is pinned for the new table separately, so that it gets the latest one.
        old_settings.removeSetting("version");

        /// Obsolete settings are not copied.
        old_settings.removeSetting("use_all_tags_column_to_generate_id");

        /// The settings of the features which the settings of this table disable: a plain CREATE rejects
        /// such a combination (see `checkTimeSeriesSettings`).
        /// The value a setting will have: written in the query, or copied unless the query resets it to the default.
        auto get_new_value = [&](std::string_view name) -> const Field *
        {
            if (new_settings)
            {
                if (const auto * written_value = new_settings->changes.tryGet(name))
                    return written_value;
                if (std::ranges::find(new_settings->default_settings, name) != new_settings->default_settings.end())
                    return nullptr;
            }
            return old_settings.tryGet(name);
        };

        /// The default value of `recent_samples_ttl_seconds` is 345600 (4 days), so an absent setting doesn't disable the recent samples table.
        if (const auto * value = get_new_value("recent_samples_ttl_seconds"); value && (SettingFieldUInt64{*value}.value == 0))
            old_settings.removeSettings({"recent_samples_partition_by", "recent_samples_index_granularity"});

        /// The default value of `store_min_time_and_max_time` is true, so an absent setting doesn't disable the columns.
        if (const auto * value = get_new_value("store_min_time_and_max_time"); value && !SettingFieldBool{*value}.value)
            old_settings.removeSettings({"aggregate_min_time_and_max_time", "filter_by_min_time_and_max_time"});
    }

    /// Removes the settings copied from the old table which were written for its `id` type, if this table has another one.
    void removeOldSettingsDisabledByIdTypeChange(SettingsChanges & old_settings, const DataTypePtr & old_id_type, const DataTypePtr & new_id_type)
    {
        if (!old_id_type->equals(*new_id_type))
            old_settings.removeSetting("id_generator");
    }

    /// Removes the columns copied from the old table which the settings of this table disable.
    /// `old_settings` are the settings of the old table, `new_settings` are the settings of this table.
    void removeInnerColumnsDisabledByNewSettings(
        ASTColumns & inner_table_columns, ViewTarget::Kind inner_table_kind, const TimeSeriesSettings & old_settings, const TimeSeriesSettings & new_settings)
    {
        if ((inner_table_kind != ViewTarget::Tags) || !inner_table_columns.columns)
            return;

        auto & columns = inner_table_columns.columns->children;
        auto remove_column = [&](std::string_view name)
        {
            auto has_name = [&](const ASTPtr & column) { return column->as<ASTColumnDeclaration &>().name == name; };
            columns.erase(std::remove_if(columns.begin(), columns.end(), has_name), columns.end());
        };

        /// The columns "min_time" and "max_time" are not stored.
        if (!new_settings[TimeSeriesSetting::store_min_time_and_max_time])
        {
            remove_column(TimeSeriesColumnNames::MinTime);
            remove_column(TimeSeriesColumnNames::MaxTime);
        }

        /// The ephemeral column "all_tags" was used in version 0 for calculating identifiers: it contained all the tags,
        /// while the "tags" column contained only the tags without dedicated columns.
        if (old_settings[TimeSeriesSetting::version] == 0)
            remove_column(TimeSeriesColumnNames::AllTags);

        /// The tag columns of the old table which aren't tag columns of this table (see `tags_to_columns`).
        auto get_tag_column_names = [](const TimeSeriesSettings & from_settings)
        {
            std::unordered_set<String> tag_column_names;
            const Map & tags_to_columns = from_settings[TimeSeriesSetting::tags_to_columns];
            for (const auto & tag_name_and_column_name : tags_to_columns)
                tag_column_names.insert(tag_name_and_column_name.safeGet<Tuple>().at(1).safeGet<String>());
            return tag_column_names;
        };
        auto new_tag_column_names = get_tag_column_names(new_settings);
        for (const auto & old_tag_column_name : get_tag_column_names(old_settings))
        {
            if (!new_tag_column_names.contains(old_tag_column_name))
                remove_column(old_tag_column_name);
        }
    }

    /// Whether a column of an inner table looks like a column generated by `normalizeInnerColumns` of the current or
    /// of an older version of the server. `settings` are the settings of the table the column was generated for.
    /// Such a column carries no information beyond the settings, so it can be generated again.
    bool isGeneratedInnerColumn(const ASTColumnDeclaration & column, ViewTarget::Kind inner_table_kind, const TimeSeriesSettings & settings)
    {
        if (!column.getType() || column.getComment() || column.getStatisticsDesc() || column.getTTL()
            || column.getCollation() || column.getSettings() || column.null_modifier || column.primary_key_specifier)
            return false;

        const auto & name = column.name;
        auto type = DataTypeFactory::instance().get(column.getType());
        auto type_name = type->getName();

        const bool is_version_0 = (settings[TimeSeriesSetting::version] == 0);
        const bool has_default = column.getDefaultExpression() || (column.default_specifier != ColumnDefaultSpecifier::Empty);

        /// Any type accepted for "timestamp" and "value" counts, see `resolveTimeSeriesTypes`.
        auto is_scalar_type = [](const IDataType & scalar_type)
        {
            WhichDataType which{scalar_type};
            return which.isFloat64() || which.isFloat32();
        };

        auto is_timestamp_type = [](const IDataType & timestamp_type)
        {
            WhichDataType which{timestamp_type};
            return which.isDateTime64() || which.isDateTime() || which.isUInt32();
        };

        auto is_nullable_timestamp = [&](const DataTypePtr & nullable_type)
        {
            return nullable_type->isNullable() && is_timestamp_type(*removeNullable(nullable_type));
        };

        auto codec = column.getCodec();

        switch (inner_table_kind)
        {
            case ViewTarget::Samples:
            case ViewTarget::RecentSamples:
            {
                if (has_default)
                    return false;

                /// Any type counts because the type is also resolved from the old table (see `resolveTimeSeriesTypes`).
                if (name == TimeSeriesColumnNames::ID)
                    return !codec;

                /// The generated "timestamp" and "value" columns have codecs (see `normalizeInnerColumns`).
                /// The codecs are not checked for version 0: its columns are converted to the current form anyway.
                if (name == TimeSeriesColumnNames::Timestamp)
                {
                    if (!is_timestamp_type(*type))
                        return false;
                    return !codec || is_version_0 || (codec->formatWithSecretsOneLine() == "CODEC(DoubleDelta, ZSTD(1))");
                }

                if (name == TimeSeriesColumnNames::Value)
                {
                    if (!is_scalar_type(*type))
                        return false;
                    return !codec || is_version_0 || (codec->formatWithSecretsOneLine() == "CODEC(ZSTD(3))");
                }

                return false;
            }

            case ViewTarget::Tags:
            {
                if (codec)
                    return false;

                if (name == TimeSeriesColumnNames::ID)
                {
                    /// Any type counts because the type is also resolved from the old table (see `resolveTimeSeriesTypes`).
                    /// The DEFAULT is absent (with the `id_generator` setting) or the canonical expression for the type.
                    auto default_expression = column.getDefaultExpression();
                    if (!default_expression)
                        return column.default_specifier == ColumnDefaultSpecifier::Empty;

                    if ((column.default_specifier != ColumnDefaultSpecifier::Default) || column.ephemeral_default)
                        return false;

                    auto canonical_default = TimeSeriesIDGenerator::tryGetDefault(type);
                    if (canonical_default && (default_expression->formatWithSecretsOneLine() == canonical_default->formatWithSecretsOneLine()))
                        return true;

                    /// Version 0 had other canonical expressions, hashing the ephemeral "all_tags" column.
                    return is_version_0;
                }

                if (has_default)
                    return false;

                if (name == TimeSeriesColumnNames::MetricName)
                    return type_name == "LowCardinality(String)";

                if (name == TimeSeriesColumnNames::Tags)
                    return type_name == "Map(LowCardinality(String), String)";

                if ((name == TimeSeriesColumnNames::MinTime) || (name == TimeSeriesColumnNames::MaxTime))
                {
                    /// `Nullable(<timestamp type>)`, or `SimpleAggregateFunction(min|max, Nullable(<timestamp type>))` when aggregated.
                    if (const auto * simple_aggregate = typeid_cast<const DataTypeCustomSimpleAggregateFunction *>(type->getCustomName()))
                    {
                        std::string_view expected_function = (name == TimeSeriesColumnNames::MinTime) ? "min" : "max";
                        const auto & argument_types = simple_aggregate->getArgumentsDataTypes();
                        return (simple_aggregate->getFunctionName() == expected_function) && (argument_types.size() == 1)
                            && is_nullable_timestamp(argument_types[0]);
                    }
                    return is_nullable_timestamp(type);
                }

                /// A tag column from `tags_to_columns`.
                const Map & tags_to_columns = settings[TimeSeriesSetting::tags_to_columns];
                for (const auto & tag_name_and_column_name : tags_to_columns)
                {
                    const auto & column_name = tag_name_and_column_name.safeGet<Tuple>().at(1).safeGet<String>();
                    if (name == column_name)
                        return type_name == "String";
                }

                return false;
            }

            case ViewTarget::Metrics:
            {
                if (has_default || codec)
                    return false;

                if ((name == TimeSeriesColumnNames::MetricFamilyName) || (name == TimeSeriesColumnNames::Help))
                    return type_name == "String";

                if ((name == TimeSeriesColumnNames::Type) || (name == TimeSeriesColumnNames::Unit))
                {
                    if (type_name == "LowCardinality(String)")
                        return true;
                    /// The prealpha version (version 0) generated `String` for "type" and "unit".
                    return is_version_0 && (type_name == "String");
                }

                return false;
            }

            default:
                UNREACHABLE();
        }
    }

    /// Removes the generated columns (see `isGeneratedInnerColumn`) from an inner table's column list,
    /// so that `normalizeInnerColumns` generates them again from the current settings.
    void removeGeneratedInnerColumns(ASTColumns & inner_table_columns, ViewTarget::Kind inner_table_kind, const TimeSeriesSettings & settings)
    {
        if (!inner_table_columns.columns)
            return;

        auto & columns = inner_table_columns.columns->children;
        auto is_generated = [&](const ASTPtr & column)
        {
            return isGeneratedInnerColumn(column->as<ASTColumnDeclaration &>(), inner_table_kind, settings);
        };
        columns.erase(std::remove_if(columns.begin(), columns.end(), is_generated), columns.end());
    }

    /// Removes the parts of an inner table's engine declaration which look like generated by `normalizeInnerEngine`
    /// of the current or of an older version of the server, so that they are generated again; the other parts are kept.
    /// A declaration with an engine which doesn't look generated is kept as a whole.
    /// `settings` are the settings of the table the engine was generated for.
    void removeGeneratedInnerEngine(ASTStorage & inner_engine, ViewTarget::Kind inner_table_kind, const TimeSeriesSettings & settings)
    {
        if (!inner_engine.engine)
            return;

        /// A MergeTree engine of any replication type (`MergeTree`, `ReplicatedMergeTree` or `SharedMergeTree`) without arguments.
        const auto & engine = *inner_engine.engine;
        if (engine.parameters || (engine.arguments && !engine.arguments->children.empty()))
            return;

        std::string_view engine_name = engine.name;
        if (engine_name.starts_with("Replicated"))
            engine_name.remove_prefix(strlen("Replicated"));
        else if (engine_name.starts_with("Shared"))
            engine_name.remove_prefix(strlen("Shared"));

        const bool is_version_0 = (settings[TimeSeriesSetting::version] == 0);

        /// Whether the sorting key consists of the specified columns. A tuple is written as `(a, b)`, or as `tuple(a, b)`
        /// by some older versions, and a single column is written without a tuple.
        auto sorting_key_equals = [&](std::string_view columns)
        {
            if (!inner_engine.order_by)
                return false;
            auto key_str = inner_engine.order_by->formatWithSecretsOneLine();
            return (key_str == columns) || (key_str == fmt::format("({})", columns)) || (key_str == fmt::format("tuple({})", columns));
        };

        auto partitioning_equals = [&](std::string_view expression)
        {
            return inner_engine.partition_by && (inner_engine.partition_by->formatWithSecretsOneLine() == expression);
        };

        auto ttl_equals = [&](std::string_view expression)
        {
            return inner_engine.ttl_table && (inner_engine.ttl_table->formatWithSecretsOneLine() == expression);
        };

        /// Removes the settings with the expected values; the settings clause goes away with its last setting.
        auto remove_settings = [&](const std::unordered_map<std::string_view, Field> & expected_inner_settings)
        {
            if (!inner_engine.settings)
                return;
            std::erase_if(inner_engine.settings->changes, [&](const SettingChange & change)
            {
                auto it = expected_inner_settings.find(change.name);
                return (it != expected_inner_settings.end()) && (change.value == it->second);
            });
            if (inner_engine.settings->changes.empty())
                inner_engine.reset(inner_engine.settings);
        };

        switch (inner_table_kind)
        {
            case ViewTarget::Samples:
            {
                if (engine_name != "MergeTree")
                    return;
                if (sorting_key_equals("id, timestamp"))
                    inner_engine.reset(inner_engine.order_by);
                remove_settings({{"index_granularity", settings[TimeSeriesSetting::samples_index_granularity].value}});
                break;
            }

            case ViewTarget::RecentSamples:
            {
                if (engine_name != "MergeTree")
                    return;
                if (sorting_key_equals("id, timestamp"))
                    inner_engine.reset(inner_engine.order_by);

                /// The partition key is the `recent_samples_partition_by` setting if set, otherwise the default one.
                const auto & partition_by = settings[TimeSeriesSetting::recent_samples_partition_by].value;
                String expected_partition_by = partition_by
                    ? partition_by->formatWithSecretsOneLine()
                    : "toStartOfInterval(toDateTime(timestamp), toIntervalHour(5))";
                if (partitioning_equals(expected_partition_by))
                    inner_engine.reset(inner_engine.partition_by);

                UInt64 ttl_seconds = settings[TimeSeriesSetting::recent_samples_ttl_seconds];
                if (ttl_equals(fmt::format("toDateTime(timestamp) + toIntervalSecond({})", ttl_seconds)))
                    inner_engine.reset(inner_engine.ttl_table);

                remove_settings({
                    {"index_granularity", settings[TimeSeriesSetting::recent_samples_index_granularity].value},
                    {"ttl_only_drop_parts", static_cast<UInt64>(1)}});
                break;
            }

            case ViewTarget::Tags:
            {
                /// The generated engine kind follows the `aggregate_min_time_and_max_time` setting of the old table.
                std::string_view generated_engine_name = settings[TimeSeriesSetting::aggregate_min_time_and_max_time]
                    ? "AggregatingMergeTree"
                    : "ReplacingMergeTree";
                if (engine_name != generated_engine_name)
                    return;

                /// The primary key and the sorting key are connected, so they are considered together.
                bool primary_key_is_generated = !inner_engine.primary_key
                    || (inner_engine.primary_key->formatWithSecretsOneLine() == "metric_name");

                /// The generated sorting key contains `min_time` and `max_time` if they are stored but not aggregated.
                /// Version 0 tables were also generated with the short key regardless of these settings.
                bool min_time_and_max_time_in_sorting_key = settings[TimeSeriesSetting::store_min_time_and_max_time]
                    && !settings[TimeSeriesSetting::aggregate_min_time_and_max_time];
                bool sorting_key_is_generated = sorting_key_equals(
                    min_time_and_max_time_in_sorting_key ? "metric_name, id, min_time, max_time" : "metric_name, id");
                if (!sorting_key_is_generated && is_version_0)
                    sorting_key_is_generated = sorting_key_equals("metric_name, id");

                if (primary_key_is_generated && sorting_key_is_generated)
                {
                    inner_engine.reset(inner_engine.primary_key);
                    inner_engine.reset(inner_engine.order_by);

                    /// `allow_nullable_key` is needed for the nullable `min_time` and `max_time` in the sorting key,
                    /// so it's connected to the sorting key too.
                    remove_settings({{"allow_nullable_key", static_cast<UInt64>(1)}});
                }

                remove_settings({
                    {"index_granularity", settings[TimeSeriesSetting::tags_index_granularity].value},
                    {"allow_dimensions_outside_sorting_key", static_cast<UInt64>(1)}});
                break;
            }

            case ViewTarget::Metrics:
            {
                if (engine_name != "ReplacingMergeTree")
                    return;
                if (sorting_key_equals("metric_family_name"))
                    inner_engine.reset(inner_engine.order_by);
                break;
            }

            default:
                UNREACHABLE();
        }

        inner_engine.reset(inner_engine.engine);
    }

    /// Adds missing required columns to an inner table's column list, building them in canonical order.
    /// Existing columns are taken from `inner_table_columns`; missing columns are created with the given type.
    /// Returns true if the column list was modified.
    bool normalizeInnerColumns(
        ASTColumns & inner_table_columns,
        ViewTarget::Kind inner_table_kind,
        const TimeSeriesSettings & time_series_settings,
        const ResolvedTimeSeriesTypes & resolved_types,
        const StorageID & table_id)
    {
        /// Build a map of the existing inner columns by name.
        std::map<String, ASTPtr> original;
        if (inner_table_columns.columns)
        {
            for (auto & child : inner_table_columns.columns->children)
                original[child->as<ASTColumnDeclaration &>().name] = child;
        }

        auto new_list = make_intrusive<ASTExpressionList>();
        bool changed = false;

        /// If `name` exists in `original`, move it to new_list (erasing from map) and return nullptr.
        /// Otherwise create a new column with `type_ast`, mark `changed`, and return the new declaration.
        auto add_column_if_missing = [&](const String & name, ASTPtr type_ast) -> ASTColumnDeclaration *
        {
            if (auto it = original.find(name); it != original.end())
            {
                new_list->children.push_back(it->second);
                original.erase(it);
                return nullptr;
            }
            auto decl = make_intrusive<ASTColumnDeclaration>();
            decl->name = name;
            decl->setType(std::move(type_ast));
            new_list->children.push_back(decl);
            changed = true;
            return decl.get();
        };

        switch (inner_table_kind)
        {
            case ViewTarget::Samples:
            case ViewTarget::RecentSamples:
            {
                /// Column "id" - no DEFAULT in the samples table: the identifier is computed in the "tags"
                /// inner table because it depends on columns like "metric_name" or "tags" which don't
                /// exist in samples.
                add_column_if_missing(TimeSeriesColumnNames::ID, dataTypeToAST(resolved_types.id_type));

                /// Auto-created "timestamp" and "value" columns get compression codecs: under generic LZ4
                /// near-monotonic millisecond timestamps barely compress and dominate the table size
                /// (>90% of on-disk bytes on a scrape-like corpus). All types accepted by the validation
                /// above are compatible with DoubleDelta (DateTime64/DateTime/UInt32). The "value" column
                /// gets plain ZSTD(3): specialized floating-point codecs such as Gorilla proved unreliable
                /// in practice. Explicitly declared columns keep whatever the user wrote.
                if (auto * timestamp_decl = add_column_if_missing(TimeSeriesColumnNames::Timestamp, dataTypeToAST(resolved_types.timestamp_type)))
                    timestamp_decl->setCodec(makeASTFunction(
                        "CODEC", make_intrusive<ASTIdentifier>("DoubleDelta"), makeASTFunction("ZSTD", make_intrusive<ASTLiteral>(UInt64{1}))));
                if (auto * value_decl = add_column_if_missing(TimeSeriesColumnNames::Value, dataTypeToAST(resolved_types.scalar_type)))
                    value_decl->setCodec(makeASTFunction("CODEC", makeASTFunction("ZSTD", make_intrusive<ASTLiteral>(UInt64{3}))));

                break;
            }

            case ViewTarget::Tags:
            {
                /// Column "id" - with a DEFAULT expression that computes the identifier from "metric_name" and tags.
                /// The DEFAULT is auto-added (derived from the id type) only when the `id_generator` setting is not set.
                add_column_if_missing(TimeSeriesColumnNames::ID, dataTypeToAST(resolved_types.id_type));
                {
                    auto & column = new_list->children.back();
                    if (!column->as<ASTColumnDeclaration &>().getDefaultExpression()
                        && !time_series_settings[TimeSeriesSetting::id_generator].value)
                    {
                        column = column->clone();
                        auto & new_decl = column->as<ASTColumnDeclaration &>();
                        new_decl.default_specifier = ColumnDefaultSpecifier::Default;
                        new_decl.ephemeral_default = false;
                        new_decl.setDefaultExpression(TimeSeriesIDGenerator::getDefault(resolved_types.id_type, table_id));
                        changed = true;
                    }
                }

                add_column_if_missing(TimeSeriesColumnNames::MetricName,
                    makeASTDataType("LowCardinality", makeASTDataType("String")));

                /// Columns corresponding to specific tags specified in the "tags_to_columns" setting.
                const Map & tags_to_columns = time_series_settings[TimeSeriesSetting::tags_to_columns];
                for (const auto & tag_name_and_column_name : tags_to_columns)
                {
                    const auto & tuple = tag_name_and_column_name.safeGet<Tuple>();
                    const auto & column_name = tuple.at(1).safeGet<String>();
                    add_column_if_missing(column_name, makeASTDataType("String"));
                }

                add_column_if_missing(TimeSeriesColumnNames::Tags,
                    makeASTDataType("Map", makeASTDataType("LowCardinality", makeASTDataType("String")), makeASTDataType("String")));

                /// Columns "min_time" and "max_time".
                if (time_series_settings[TimeSeriesSetting::store_min_time_and_max_time])
                {
                    if (time_series_settings[TimeSeriesSetting::aggregate_min_time_and_max_time])
                    {
                        /// When aggregation is enabled the columns need a custom SimpleAggregateFunction type.
                        auto make_agg_type = [&](const String & func_name) -> ASTPtr
                        {
                            DataTypePtr ts_type = makeNullable(resolved_types.timestamp_type);
                            AggregateFunctionProperties properties;
                            auto func = AggregateFunctionFactory::instance().get(func_name, NullsAction::EMPTY, {ts_type}, {}, properties);
                            auto custom_name = std::make_unique<DataTypeCustomSimpleAggregateFunction>(func, DataTypes{ts_type}, Array{});
                            auto type = DataTypeFactory::instance().getCustom(std::make_unique<DataTypeCustomDesc>(std::move(custom_name)));
                            return dataTypeToAST(type);
                        };

                        add_column_if_missing(TimeSeriesColumnNames::MinTime, make_agg_type("min"));
                        add_column_if_missing(TimeSeriesColumnNames::MaxTime, make_agg_type("max"));
                    }
                    else
                    {
                        add_column_if_missing(TimeSeriesColumnNames::MinTime,
                            dataTypeToAST(makeNullable(resolved_types.timestamp_type)));
                        add_column_if_missing(TimeSeriesColumnNames::MaxTime,
                            dataTypeToAST(makeNullable(resolved_types.timestamp_type)));
                    }
                }

                break;
            }

            case ViewTarget::Metrics:
            {
                add_column_if_missing(TimeSeriesColumnNames::MetricFamilyName, makeASTDataType("String"));
                add_column_if_missing(TimeSeriesColumnNames::Type, makeASTDataType("LowCardinality", makeASTDataType("String")));
                add_column_if_missing(TimeSeriesColumnNames::Unit, makeASTDataType("LowCardinality", makeASTDataType("String")));
                add_column_if_missing(TimeSeriesColumnNames::Help, makeASTDataType("String"));
                break;
            }

            default:
                UNREACHABLE();
        }

        /// Copy all remaining original columns at the end (user-defined extra columns).
        for (auto & [name, col] : original)
            new_list->children.push_back(col);

        if (!changed)
            return false;

        inner_table_columns.setOrReplace(inner_table_columns.columns, new_list);
        return true;
    }

    /// Whether the SETTINGS clause of an inner table's engine declaration contains the specified setting.
    bool hasEngineSetting(const ASTStorage & storage, std::string_view name)
    {
        return storage.settings && storage.settings->changes.tryGet(name);
    }

    /// Sets a setting in the SETTINGS clause of an inner table's engine declaration,
    /// overwriting the existing value if present.
    void setEngineSettings(ASTStorage & storage, std::string_view name, const Field & value)
    {
        if (!storage.settings)
        {
            auto settings_ast = make_intrusive<ASTSetQuery>();
            settings_ast->is_standalone = false;
            storage.set(storage.settings, settings_ast);
        }
        storage.settings->changes.setSetting(name, value);
    }

    /// Converts a create query written by a server which didn't support versioning yet:
    /// such tables belong to version 0 (see TimeSeriesVersion.h).
    void convertDefinitionWithoutExplicitVersion(ASTCreateQuery & create_query)
    {
        setTimeSeriesSettingVersion(create_query, 0);
    }

    /// Detects prealpha version by outer columns: prealpha had outer columns `id`, `timestamp`, `value`,
    /// and now we don't have them.
    bool isPrealpha(const ASTCreateQuery & create_query)
    {
        if (!create_query.columns_list || !create_query.columns_list->columns)
            return false;
        for (const auto & column : create_query.columns_list->columns->children)
        {
            const auto & decl = column->as<ASTColumnDeclaration &>();
            if (decl.name == TimeSeriesColumnNames::Timestamp
                || decl.name == TimeSeriesColumnNames::Value
                || decl.name == TimeSeriesColumnNames::ID)
                return true;
        }
        return false;
    }

    /// Converts a prealpha CREATE query: generates `INNER COLUMNS` for inner targets and
    /// replaces outer columns with a single `time_series` column carrying the resolved types.
    /// Function normalizeTimeSeriesDefinition() will rebuild the full list of the outer columns afterwards.
    void convertPrealphaDefinition(ASTCreateQuery & create_query)
    {
        TimeSeriesSettings time_series_settings;
        if (create_query.storage)
            time_series_settings.loadFromQuery(*create_query.storage);

        StorageID table_id{create_query.getDatabase(), create_query.getTable()};

        /// Map of the original outer columns.
        std::map<String, ASTPtr> outer_columns_by_name;
        if (create_query.columns_list && create_query.columns_list->columns)
        {
            for (const auto & child : create_query.columns_list->columns->children)
                outer_columns_by_name[child->as<ASTColumnDeclaration &>().name] = child;
        }

        auto type_from_outer = [&](const String & name) -> DataTypePtr
        {
            if (auto it = outer_columns_by_name.find(name); it != outer_columns_by_name.end())
            {
                const auto & decl = it->second->as<ASTColumnDeclaration &>();
                if (decl.getType())
                    return DataTypeFactory::instance().get(decl.getType());
            }
            return nullptr;
        };

        /// Columns `id`, `timestamp`, `value` were outer columns in the prealpha version.
        DataTypePtr timestamp_type = type_from_outer(TimeSeriesColumnNames::Timestamp);
        DataTypePtr scalar_type = type_from_outer(TimeSeriesColumnNames::Value);
        DataTypePtr id_type = type_from_outer(TimeSeriesColumnNames::ID);
        chassert(timestamp_type || scalar_type || id_type);
        if (!timestamp_type)
            timestamp_type = std::make_shared<DataTypeDateTime64>(3);
        if (!scalar_type)
            scalar_type = std::make_shared<DataTypeFloat64>();
        if (!id_type)
            id_type = std::make_shared<DataTypeUUID>();

        for (auto inner_table_kind : getTargetKinds())
        {
            /// Prealpha tables predate the recent samples table, so there is nothing to convert for it,
            /// and no RECENT SAMPLES target should be added to an old table's definition.
            if (inner_table_kind == ViewTarget::RecentSamples)
                continue;
            if (hasTargetTableID(create_query, inner_table_kind))
                continue;
            if (hasInnerColumns(create_query, inner_table_kind))
                continue;

            auto new_list = make_intrusive<ASTExpressionList>();

            auto add_column = [&](const String & name, ASTPtr type_ast)
            {
                if (auto it = outer_columns_by_name.find(name); it != outer_columns_by_name.end())
                {
                    new_list->children.push_back(it->second->clone());
                    return;
                }
                auto decl = make_intrusive<ASTColumnDeclaration>();
                decl->name = name;
                decl->setType(std::move(type_ast));
                new_list->children.push_back(decl);
            };

            switch (inner_table_kind)
            {
                case ViewTarget::Samples:
                {
                    add_column(TimeSeriesColumnNames::ID, dataTypeToAST(id_type));
                    {
                        auto & new_decl = new_list->children.back()->as<ASTColumnDeclaration &>();
                        new_decl.default_specifier = ColumnDefaultSpecifier::Empty;
                        new_decl.ephemeral_default = false;
                        new_decl.resetDefaultExpression();
                    }
                    add_column(TimeSeriesColumnNames::Timestamp, dataTypeToAST(timestamp_type));
                    add_column(TimeSeriesColumnNames::Value, dataTypeToAST(scalar_type));
                    break;
                }

                case ViewTarget::Tags:
                {
                    /// Column "id".
                    add_column(TimeSeriesColumnNames::ID, dataTypeToAST(id_type));
                    {
                        auto & new_decl = new_list->children.back()->as<ASTColumnDeclaration &>();
                        new_decl.ephemeral_default = false;
                        if (!time_series_settings[TimeSeriesSetting::id_generator].value)
                        {
                            /// Function getDefault has changed since the prealpha version,
                            /// so it can generate different identifiers now.
                            new_decl.default_specifier = ColumnDefaultSpecifier::Default;
                            new_decl.setDefaultExpression(TimeSeriesIDGenerator::getDefault(id_type, table_id));
                        }
                        else
                        {
                            new_decl.default_specifier = ColumnDefaultSpecifier::Empty;
                            new_decl.resetDefaultExpression();
                        }
                    }

                    add_column(TimeSeriesColumnNames::MetricName,
                        makeASTDataType("LowCardinality", makeASTDataType("String")));

                    /// Columns corresponding to specific tags specified in the "tags_to_columns" setting.
                    const Map & tags_to_columns = time_series_settings[TimeSeriesSetting::tags_to_columns];
                    for (const auto & tag_name_and_column_name : tags_to_columns)
                    {
                        const auto & tuple = tag_name_and_column_name.safeGet<Tuple>();
                        const auto & column_name = tuple.at(1).safeGet<String>();
                        add_column(column_name, makeASTDataType("String"));
                    }

                    add_column(TimeSeriesColumnNames::Tags,
                        makeASTDataType("Map", makeASTDataType("LowCardinality", makeASTDataType("String")), makeASTDataType("String")));

                    /// Columns "min_time" and "max_time".
                    if (time_series_settings[TimeSeriesSetting::store_min_time_and_max_time])
                    {
                        if (time_series_settings[TimeSeriesSetting::aggregate_min_time_and_max_time])
                        {
                            /// When aggregation is enabled the columns need a custom SimpleAggregateFunction type.
                            auto make_agg_type = [&](const String & func_name) -> ASTPtr
                            {
                                DataTypePtr ts_type = makeNullable(timestamp_type);
                                AggregateFunctionProperties properties;
                                auto func = AggregateFunctionFactory::instance().get(func_name, NullsAction::EMPTY, {ts_type}, {}, properties);
                                auto custom_name = std::make_unique<DataTypeCustomSimpleAggregateFunction>(func, DataTypes{ts_type}, Array{});
                                auto type = DataTypeFactory::instance().getCustom(std::make_unique<DataTypeCustomDesc>(std::move(custom_name)));
                                return dataTypeToAST(type);
                            };

                            add_column(TimeSeriesColumnNames::MinTime, make_agg_type("min"));
                            add_column(TimeSeriesColumnNames::MaxTime, make_agg_type("max"));
                        }
                        else
                        {
                            add_column(TimeSeriesColumnNames::MinTime,
                                dataTypeToAST(makeNullable(timestamp_type)));
                            add_column(TimeSeriesColumnNames::MaxTime,
                                dataTypeToAST(makeNullable(timestamp_type)));
                        }
                    }

                    break;
                }

                case ViewTarget::Metrics:
                {
                    add_column(TimeSeriesColumnNames::MetricFamilyName, makeASTDataType("String"));
                    add_column(TimeSeriesColumnNames::Type, makeASTDataType("String"));
                    add_column(TimeSeriesColumnNames::Unit, makeASTDataType("String"));
                    add_column(TimeSeriesColumnNames::Help, makeASTDataType("String"));
                    break;
                }

                default:
                    UNREACHABLE();
            }

            auto result = make_intrusive<ASTColumns>();
            result->columns = new_list.get();
            result->children.push_back(std::move(new_list));
            create_query.setTargetInnerColumns(inner_table_kind, result);
        }

        /// Replace the prealpha flat outer columns with a single `time_series` column.
        auto time_series_decl = make_intrusive<ASTColumnDeclaration>();
        time_series_decl->name = TimeSeriesColumnNames::TimeSeries;
        time_series_decl->setType(dataTypeToAST(std::make_shared<DataTypeArray>(
            std::make_shared<DataTypeTuple>(DataTypes{timestamp_type, scalar_type}))));

        auto new_outer_list = make_intrusive<ASTExpressionList>();
        new_outer_list->children.push_back(std::move(time_series_decl));

        auto new_outer_columns = make_intrusive<ASTColumns>();
        new_outer_columns->set(new_outer_columns->columns, new_outer_list);
        create_query.set(create_query.columns_list, new_outer_columns);
    }

    /// Whether the create query was made by a version before the recent samples table existed,
    /// i.e. it doesn't record the `recent_samples_ttl_seconds` setting in its SETTINGS clause.
    bool isVersionWithNoRecentSamplesTTL(const ASTCreateQuery & create_query)
    {
        return create_query.storage && !hasExplicitTimeSeriesSettingRecentSamplesTTL(create_query);
    }

    /// Converts a create query made by a version before the `recent_samples_ttl_seconds` setting existed:
    /// records the setting explicitly in the query's SETTINGS clause, so that its value always matches the table.
    void convertDefinitionWithoutRecentSamplesTTL(ASTCreateQuery & create_query)
    {
        /// Normally the setting is pinned to zero: the table was initially created without the recent
        /// samples table, while the absent setting would read as its non-zero default. However a query
        /// carrying a RECENT SAMPLES target in any form was authored with the recent samples table
        /// enabled and gets the default TTL instead. Such a query can come from an old-format ON CLUSTER
        /// DDL entry (the query text is shipped un-normalized, only the inner UUID is set by the
        /// initiator) or from a hand-written ATTACH query.
        bool authored_with_recent_samples
            = hasInnerColumns(create_query, ViewTarget::RecentSamples) || hasInnerEngine(create_query, ViewTarget::RecentSamples)
            || hasTargetTableID(create_query, ViewTarget::RecentSamples) || hasInnerUUID(create_query, ViewTarget::RecentSamples);
        UInt64 ttl_to_pin = authored_with_recent_samples ? static_cast<UInt64>(TimeSeriesSettings{}[TimeSeriesSetting::recent_samples_ttl_seconds]) : 0;
        setEngineSettings(*create_query.storage, "recent_samples_ttl_seconds", Field(ttl_to_pin));
    }

    /// The family of the inner engines when none is declared: it follows the `default_table_engine` setting.
    /// The value `None` is kept: then the inner engines must be declared explicitly.
    DefaultTableEngine getDefaultInnerEngineFamily(const ContextPtr & context, const StorageID & table_id)
    {
        auto default_table_engine = context->getSettingsRef()[Setting::default_table_engine].value;
        switch (default_table_engine)
        {
            case DefaultTableEngine::MergeTree:
            case DefaultTableEngine::ReplicatedMergeTree:
            case DefaultTableEngine::SharedMergeTree:
            case DefaultTableEngine::None:
                return default_table_engine;
            default:
                throw Exception(ErrorCodes::INCORRECT_QUERY,
                    "{}: The `default_table_engine` setting value '{}' cannot be used to choose the engines of the inner tables "
                    "of a TimeSeries table (supported values are MergeTree, ReplicatedMergeTree and SharedMergeTree); "
                    "specify the inner tables' engines explicitly", table_id.getNameForLogs(), magic_enum::enum_name(default_table_engine));
        }
    }

    /// The prefix of the name of an inner engine for the family of the inner engines, e.g. "Replicated" for `ReplicatedMergeTree`.
    std::string_view getInnerEngineFamilyPrefix(DefaultTableEngine inner_engine_family, ViewTarget::Kind inner_table_kind)
    {
        switch (inner_engine_family)
        {
            case DefaultTableEngine::MergeTree:
                return "";
            case DefaultTableEngine::ReplicatedMergeTree:
                return "Replicated";
            case DefaultTableEngine::SharedMergeTree:
                return "Shared";
            case DefaultTableEngine::None:
                throw Exception(ErrorCodes::INCORRECT_QUERY,
                    "The inner {} table of a TimeSeries table requires an explicit engine "
                    "because the `default_table_engine` setting is 'None'", inner_table_kind);
            default:
                UNREACHABLE();
        }
    }

    /// Generates the engine of an inner table if it's not specified, and applies the TimeSeries settings to it,
    /// whether the engine was generated or specified by the user.
    /// The replication type of a generated engine (`MergeTree`, `ReplicatedMergeTree` or `SharedMergeTree`) is taken
    /// from `resolved_types`, or from the `default_table_engine` setting if no inner engine is declared.
    /// Returns true if the engine declaration was modified.
    bool normalizeInnerEngine(
        ASTStorage & inner_engine,
        ViewTarget::Kind inner_table_kind,
        const TimeSeriesSettings & settings,
        const ResolvedTimeSeriesTypes & resolved_types,
        const StorageID & table_id,
        const ContextPtr & context)
    {
        bool changed = false;

        auto set_engine = [&](std::string_view engine_kind)
        {
            DefaultTableEngine inner_engine_family = resolved_types.inner_engine_family
                ? *resolved_types.inner_engine_family
                : getDefaultInnerEngineFamily(context, table_id);
            auto engine = makeASTFunction(fmt::format("{}{}", getInnerEngineFamilyPrefix(inner_engine_family, inner_table_kind), engine_kind));
            engine->setNoEmptyArgs(false);
            inner_engine.set(inner_engine.engine, engine);
            changed = true;
        };

        auto is_merge_tree = [&] { return inner_engine.engine->name.ends_with("MergeTree"); };

        /// A declared MergeTree engine without keys gets the same keys as a generated one.
        auto needs_sorting_key = [&] { return is_merge_tree() && !inner_engine.order_by && !inner_engine.primary_key; };

        /// A key of one column is written without a tuple, e.g. `ORDER BY metric_family_name`.
        auto set_sorting_key = [&](ASTs key_columns)
        {
            ASTPtr sorting_key;
            if (key_columns.size() == 1)
                sorting_key = key_columns[0];
            else
            {
                auto tuple = makeASTOperator("tuple");
                tuple->arguments->children = std::move(key_columns);
                sorting_key = tuple;
            }
            inner_engine.set(inner_engine.order_by, sorting_key);
            changed = true;
        };

        auto set_primary_key = [&](ASTPtr primary_key)
        {
            inner_engine.set(inner_engine.primary_key, primary_key);
            changed = true;
        };

        auto has_partition_by = [&] { return inner_engine.partition_by != nullptr; };

        auto set_partition_by = [&](ASTPtr partition_by)
        {
            inner_engine.setOrReplace(inner_engine.partition_by, partition_by);
            changed = true;
        };

        /// Sets a TTL deleting the expired rows.
        auto set_ttl = [&](ASTPtr ttl_expression)
        {
            auto ttl_element = make_intrusive<ASTTTLElement>(TTLMode::DELETE, DataDestinationType::DELETE, "", /*if_exists=*/ false);
            ttl_element->setTTL(std::move(ttl_expression));
            auto ttl_list = make_intrusive<ASTExpressionList>();
            ttl_list->children.push_back(std::move(ttl_element));
            inner_engine.setOrReplace(inner_engine.ttl_table, ttl_list);
            changed = true;
        };

        auto has_engine_setting = [&](std::string_view name) { return hasEngineSetting(inner_engine, name); };

        auto set_engine_setting = [&](std::string_view name, UInt64 value)
        {
            setEngineSettings(inner_engine, name, Field(value));
            changed = true;
        };

        /// The `*_index_granularity` settings set `index_granularity` of the inner MergeTree tables, overriding the engine declaration.
        auto set_index_granularity = [&](const SettingFieldUInt64 & index_granularity)
        {
            if (is_merge_tree() && (index_granularity.isChanged() || !has_engine_setting("index_granularity")))
                set_engine_setting("index_granularity", index_granularity.value);
        };

        switch (inner_table_kind)
        {
            case ViewTarget::Samples:
            case ViewTarget::RecentSamples:
            {
                /// The recent samples table gets the same generated engine as the samples table; it becomes
                /// partitioned and TTL'd below.
                if (!inner_engine.engine)
                    set_engine("MergeTree");

                if (needs_sorting_key())
                {
                    set_sorting_key({make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID),
                        make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Timestamp)});
                }

                const auto & index_granularity = settings[(inner_table_kind == ViewTarget::Samples)
                    ? TimeSeriesSetting::samples_index_granularity
                    : TimeSeriesSetting::recent_samples_index_granularity];
                set_index_granularity(index_granularity);

                if (inner_table_kind != ViewTarget::RecentSamples)
                    break;

                /// `recent_samples_ttl_seconds` is a correctness contract for the reader: the TTL always comes from it; non-TTL engines are rejected.
                if (!is_merge_tree())
                    throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
                        "{}: The inner recent samples table requires a MergeTree-family engine to apply the TTL "
                        "defined by the `recent_samples_ttl_seconds` setting", table_id.getNameForLogs());

                /// The table is partitioned by time, so `ttl_only_drop_parts` lets the TTL drop whole expired parts instead of rewriting them.
                if (!has_engine_setting("ttl_only_drop_parts"))
                    set_engine_setting("ttl_only_drop_parts", 1);

                if (const auto & partition_by = settings[TimeSeriesSetting::recent_samples_partition_by].value)
                {
                    /// An explicitly set `recent_samples_partition_by` overrides the partition key from the engine declaration.
                    set_partition_by(partition_by->clone());
                }
                else if (!has_partition_by())
                {
                    /// Otherwise a declared partition key is kept; if there is none, the default one (5-hour buckets) is used.
                    /// `toDateTime` makes the default partition key work for any timestamp type (e.g. a raw `UInt32`),
                    /// same as the TTL expression.
                    set_partition_by(makeASTFunction("toStartOfInterval",
                        makeASTFunction("toDateTime", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Timestamp)),
                        makeASTFunction("toIntervalHour", make_intrusive<ASTLiteral>(static_cast<UInt64>(5)))));
                }

                set_ttl(makeASTOperator("plus",
                    makeASTFunction("toDateTime", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Timestamp)),
                    makeASTFunction("toIntervalSecond",
                        make_intrusive<ASTLiteral>(settings[TimeSeriesSetting::recent_samples_ttl_seconds].value))));
                break;
            }

            case ViewTarget::Tags:
            {
                const bool aggregate_min_time_and_max_time = settings[TimeSeriesSetting::aggregate_min_time_and_max_time];
                if (!inner_engine.engine)
                    set_engine(aggregate_min_time_and_max_time ? "AggregatingMergeTree" : "ReplacingMergeTree");

                if (needs_sorting_key())
                {
                    set_primary_key(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricName));

                    ASTs key_columns;
                    key_columns.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricName));
                    key_columns.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID));
                    if (settings[TimeSeriesSetting::store_min_time_and_max_time] && !aggregate_min_time_and_max_time)
                    {
                        key_columns.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MinTime));
                        key_columns.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MaxTime));

                        /// These columns are nullable, so the sorting key needs `allow_nullable_key`.
                        set_engine_setting("allow_nullable_key", 1);
                    }
                    set_sorting_key(std::move(key_columns));
                }

                set_index_granularity(settings[TimeSeriesSetting::tags_index_granularity]);

                /// The TimeSeries `tags` inner table keeps the tag columns (and the `tags` Map) outside
                /// the sorting key, but they are functionally dependent on `id`, which is part of it: every group of
                /// rows that a background merge collapses together shares the same `id`, hence the same values of
                /// those columns, so this off-key layout is safe here. `AggregatingMergeTree` rejects such a layout
                /// by default (see the `allow_dimensions_outside_sorting_key` setting and
                /// https://github.com/ClickHouse/ClickHouse/issues/751), so enable that setting on the inner tags
                /// engine — both when we generate it and when the user specifies an aggregating engine explicitly.
                if (inner_engine.engine->name.contains("Aggregating")
                    && !has_engine_setting("allow_dimensions_outside_sorting_key"))
                {
                    set_engine_setting("allow_dimensions_outside_sorting_key", 1);
                }
                break;
            }

            case ViewTarget::Metrics:
            {
                if (!inner_engine.engine)
                    set_engine("ReplacingMergeTree");

                if (needs_sorting_key())
                    set_sorting_key({make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricFamilyName)});
                break;
            }

            default:
                UNREACHABLE();
        }

        return changed;
    }

    /// Checks that a target table or an inner-columns list has all the columns required by the
    /// TimeSeries table engine, and that those columns match the resolved types.
    void checkTargetTable(
        const ColumnsDescription & target_table_columns,
        ViewTarget::Kind target_kind,
        const TimeSeriesSettings & time_series_settings,
        const ResolvedTimeSeriesTypes & resolved_types,
        const StorageID & table_id)
    {
        auto check_column = [&](std::string_view column_name)
        {
            if (!target_table_columns.tryGet(String(column_name)))
                throw Exception(
                    ErrorCodes::THERE_IS_NO_COLUMN,
                    "{}: Column {} is required for the {} table used by TimeSeries table engine",
                    table_id.getNameForLogs(),
                    column_name,
                    target_kind);
        };

        auto check_column_type = [&](std::string_view column_name, const DataTypePtr & expected_type)
        {
            check_column(column_name);
            const auto * col = target_table_columns.tryGet(String(column_name));
            if (!col->type->equals(*expected_type))
                throw Exception(
                    ErrorCodes::BAD_TYPE_OF_FIELD,
                    "{}: Column {} in the {} table has type {}, but expected {}",
                    table_id.getNameForLogs(),
                    column_name,
                    target_kind,
                    col->type->getName(),
                    expected_type->getName());
        };

        auto check_column_is_string = [&](std::string_view column_name)
        {
            check_column(column_name);
            const auto * col = target_table_columns.tryGet(String(column_name));
            if (!isString(removeLowCardinalityAndNullable(col->type)))
                throw Exception(
                    ErrorCodes::BAD_TYPE_OF_FIELD,
                    "{}: Column {} in the {} table has type {}, but expected String or LowCardinality(String)",
                    table_id.getNameForLogs(),
                    column_name,
                    target_kind,
                    col->type->getName());
        };

        auto check_column_is_string_map = [&](std::string_view column_name, bool if_exists = false)
        {
            const auto * col = target_table_columns.tryGet(String(column_name));
            if (!col)
            {
                if (!if_exists)
                    check_column(column_name);
                return;
            }
            WhichDataType which{*col->type};
            bool ok = false;
            if (which.isMap())
            {
                const auto & map_type = typeid_cast<const DataTypeMap &>(*col->type);
                ok = isString(removeLowCardinality(map_type.getKeyType()))
                    && isString(removeLowCardinality(map_type.getValueType()));
            }
            if (!ok)
                throw Exception(
                    ErrorCodes::BAD_TYPE_OF_FIELD,
                    "{}: Column {} in the {} table has type {}, but expected Map with String or LowCardinality(String) keys and values",
                    table_id.getNameForLogs(),
                    column_name,
                    target_kind,
                    col->type->getName());
        };

        /// Accepts `Nullable(timestamp_type)` or any aggregate function wrapper.
        auto check_column_min_max_time = [&](std::string_view column_name)
        {
            check_column(column_name);
            const auto * col = target_table_columns.tryGet(String(column_name));
            if (removeNullable(col->type)->equals(*resolved_types.timestamp_type))
                return;
            if (typeid_cast<const DataTypeCustomSimpleAggregateFunction *>(col->type->getCustomName()))
                return;
            if (typeid_cast<const DataTypeAggregateFunction *>(col->type.get()))
                return;
            throw Exception(
                ErrorCodes::BAD_TYPE_OF_FIELD,
                "{}: Column {} in the {} table has type {}, but expected {} (optionally Nullable) or an aggregate-function wrapper",
                table_id.getNameForLogs(),
                column_name,
                target_kind,
                col->type->getName(),
                resolved_types.timestamp_type->getName());
        };

        switch (target_kind)
        {
            case ViewTarget::Samples:
            case ViewTarget::RecentSamples:
            {
                check_column_type(TimeSeriesColumnNames::ID, resolved_types.id_type);
                check_column_type(TimeSeriesColumnNames::Timestamp, resolved_types.timestamp_type);
                check_column_type(TimeSeriesColumnNames::Value, resolved_types.scalar_type);
                break;
            }

            case ViewTarget::Tags:
            {
                check_column_type(TimeSeriesColumnNames::ID, resolved_types.id_type);
                check_column_is_string(TimeSeriesColumnNames::MetricName);

                const Map & tags_to_columns = time_series_settings[TimeSeriesSetting::tags_to_columns];
                for (const auto & tag_name_and_column_name : tags_to_columns)
                {
                    const auto & tuple = tag_name_and_column_name.safeGet<Tuple>();
                    const auto & column_name = tuple.at(1).safeGet<String>();
                    check_column_is_string(column_name);
                }

                check_column_is_string_map(TimeSeriesColumnNames::Tags);
                check_column_is_string_map(TimeSeriesColumnNames::AllTags, /*if_exists=*/ true);

                if (time_series_settings[TimeSeriesSetting::store_min_time_and_max_time])
                {
                    check_column_min_max_time(TimeSeriesColumnNames::MinTime);
                    check_column_min_max_time(TimeSeriesColumnNames::MaxTime);
                }
                break;
            }

            case ViewTarget::Metrics:
            {
                check_column_is_string(TimeSeriesColumnNames::MetricFamilyName);
                check_column_is_string(TimeSeriesColumnNames::Type);
                check_column_is_string(TimeSeriesColumnNames::Unit);
                check_column_is_string(TimeSeriesColumnNames::Help);
                break;
            }

            default:
                UNREACHABLE();
        }
    }

    /// Reads the CREATE query of the table from the clause `AS <other_table>` of `create_query`.
    /// The stored metadata of the old table can be written by an older version, so its normalized form is returned.
    boost::intrusive_ptr<const ASTCreateQuery> getASCreateQuery(const ASTCreateQuery & create_query, const ContextPtr & context)
    {
        chassert(!create_query.as_table.empty());

        /// The definition of the old table is read below.
        auto old_database = context->resolveDatabase(create_query.as_database);
        context->checkAccess(AccessType::SHOW_COLUMNS, old_database, create_query.as_table);

        auto old_create_query = boost::static_pointer_cast<const ASTCreateQuery>(
            DatabaseCatalog::instance().getDatabase(old_database)->getCreateTableQuery(create_query.as_table, context));

        /// The columns of a TimeSeries table are always generated, so the old table's columns can't be copied.
        if (!old_create_query->is_time_series_table)
        {
            StorageID old_table_id{old_create_query->getDatabase(), old_create_query->getTable()};
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                "Cannot CREATE a TimeSeries table AS {} because it is not a TimeSeries table", old_table_id.getNameForLogs());
        }

        auto normalized = boost::static_pointer_cast<ASTCreateQuery>(old_create_query->clone());
        normalizeTimeSeriesDefinition(*normalized, context, LoadingStrictnessLevel::ATTACH, /* is_restore_from_backup = */ false);
        return normalized;
    }

    /// Applies the definition of the table from the clause `AS <other_table>` to `create_query`: its inner columns,
    /// inner engines, and the `SETTINGS` clause. The outer columns are not copied because they are always regenerated.
    void applyASClause(
        ASTCreateQuery & create_query,
        const ResolvedTimeSeriesTypes & new_types,
        const ASTCreateQuery & old_create_query,
        const ResolvedTimeSeriesTypes & old_types)
    {
        /// Copy settings from the old table. Settings are merged by name: a setting written in this query wins.
        if (old_create_query.storage && old_create_query.storage->settings)
        {
            if (!create_query.storage)
                create_query.set(create_query.storage, make_intrusive<ASTStorage>());

            auto merged_settings = boost::static_pointer_cast<ASTSetQuery>(old_create_query.storage->settings->clone());

            /// Some settings of the old table are not copied because the settings written in this query disable them.
            removeOldSettingsDisabledByNewSettings(merged_settings->changes, create_query.storage->settings);

            /// The `id_generator` of the old table is not copied if this table has another `id` type.
            removeOldSettingsDisabledByIdTypeChange(merged_settings->changes, old_types.id_type, new_types.id_type);

            if (create_query.storage->settings)
            {
                /// A `name = DEFAULT` reset is a mention of the setting too, so the value of the old table
                /// is not inherited for it. The reset itself is not kept: an absent setting means the default.
                merged_settings->changes.removeSettings(create_query.storage->settings->default_settings);
                merged_settings->changes.setSettings(create_query.storage->settings->changes);
            }
            create_query.storage->set(create_query.storage->settings, merged_settings);
        }

        /// The inner columns and engines generated for the old table are not copied: they are generated again for
        /// this table, whose settings can differ. The generated columns are recognized with the settings of the old table.
        TimeSeriesSettings old_settings;
        if (old_create_query.storage)
            old_settings.loadFromQuery(*old_create_query.storage);

        /// The merged settings of this table.
        TimeSeriesSettings new_settings;
        if (create_query.storage)
            new_settings.loadFromQuery(*create_query.storage);

        /// Copy inner columns and inner engines from the old table.
        for (auto kind : getTargetKinds())
        {
            /// A disabled recent samples target needs nothing from the old table.
            if ((kind == ViewTarget::RecentSamples) && (new_settings[TimeSeriesSetting::recent_samples_ttl_seconds] == 0))
                continue;

            if (!hasTargetTableID(create_query, kind) && !hasInnerColumns(create_query, kind))
            {
                if (auto * old_inner_columns = old_create_query.getTargetInnerColumns(kind))
                {
                    auto new_inner_columns = boost::static_pointer_cast<ASTColumns>(old_inner_columns->clone());
                    removeInnerColumnsDisabledByNewSettings(*new_inner_columns, kind, old_settings, new_settings);
                    removeGeneratedInnerColumns(*new_inner_columns, kind, old_settings);
                    create_query.setTargetInnerColumns(kind, new_inner_columns);
                }
            }

            if (!hasTargetTableID(create_query, kind) && !hasInnerEngine(create_query, kind))
            {
                if (hasTargetTableID(old_create_query, kind))
                {
                    /// An external target of the old table is not copied, but the query can declare an inner target
                    /// instead by its columns, then the engine is generated.
                    if (!hasInnerColumns(create_query, kind))
                    {
                        StorageID old_table_id{old_create_query.getDatabase(), old_create_query.getTable()};
                        throw Exception(ErrorCodes::INCORRECT_QUERY,
                            "Cannot CREATE a table AS {} because it has external tables", old_table_id.getNameForLogs());
                    }
                }
                else if (const auto * old_inner_engine = old_create_query.getTargetInnerEngine(kind))
                {
                    auto new_inner_engine = boost::static_pointer_cast<ASTStorage>(old_inner_engine->clone());
                    removeGeneratedInnerEngine(*new_inner_engine, kind, old_settings);
                    create_query.setTargetInnerEngine(kind, new_inner_engine);
                }
            }
        }
    }

    /// Generates the canonical column list for the TimeSeries table from the resolved types.
    ColumnsDescription generateTimeSeriesColumns(const DataTypePtr & timestamp_type, const DataTypePtr & scalar_type)
    {
        ColumnsDescription result;

        auto add_column = [&](const String & name, DataTypePtr type)
        {
            result.add(ColumnDescription{name, std::move(type)});
        };

        add_column(TimeSeriesColumnNames::MetricName, std::make_shared<DataTypeString>());

        add_column(TimeSeriesColumnNames::Tags,
                   std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()));

        add_column(TimeSeriesColumnNames::TimeSeries,
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(DataTypes{timestamp_type, scalar_type})));

        add_column(TimeSeriesColumnNames::MetricFamily, std::make_shared<DataTypeString>());
        add_column(TimeSeriesColumnNames::Type, std::make_shared<DataTypeString>());
        add_column(TimeSeriesColumnNames::Unit, std::make_shared<DataTypeString>());
        add_column(TimeSeriesColumnNames::Help, std::make_shared<DataTypeString>());

        return result;
    }

}


void normalizeTimeSeriesDefinition(ASTCreateQuery & create_query, const ContextPtr & context, LoadingStrictnessLevel mode, bool is_restore_from_backup)
{
    chassert(create_query.is_time_series_table);

    /// Whether we're creating a new table.
    /// `is_new_table` is false if we're restoring from a backup.
    bool is_new_table = (mode <= LoadingStrictnessLevel::SECONDARY_CREATE) && !is_restore_from_backup;

    /// Whether the create query may come from an older version, so it can be converted to the current form.
    /// The initial CREATE query is excluded: it must be written in the current form already.
    bool can_convert = (mode != LoadingStrictnessLevel::CREATE) || is_restore_from_backup;

    /// Convert the create_query if it was created before the `version` setting was introduced.
    if (can_convert && !hasExplicitTimeSeriesSettingVersion(create_query))
    {
        convertDefinitionWithoutExplicitVersion(create_query);
        chassert(hasExplicitTimeSeriesSettingVersion(create_query));
    }

    /// The older forms of the definition below were written only by servers which didn't support versioning yet,
    /// so they can be found only in tables of version 0.
    if (can_convert && (getTimeSeriesSettingVersion(create_query) == 0))
    {
        /// Convert the create_query if it was created by the old versions.
        /// (A new query written in the prealpha form must be rejected, see readTypesFromOuterColumns.)
        if (isPrealpha(create_query))
        {
            convertPrealphaDefinition(create_query);
            chassert(!isPrealpha(create_query));
        }

        /// Convert the create_query if it was created before the recent samples table existed.
        if (isVersionWithNoRecentSamplesTTL(create_query))
        {
            convertDefinitionWithoutRecentSamplesTTL(create_query);
            chassert(!isVersionWithNoRecentSamplesTTL(create_query));
        }
    }

    /// Whether the query itself declares a RECENT SAMPLES target. This is checked before applyASClause,
    /// so the flag doesn't count a target copied from the `AS <other_table>` clause. An inner UUID doesn't
    /// count either: it's not written by users, it's stamped by UUID generation - which can legitimately
    /// happen before normalization (e.g. for an ON CLUSTER query using an old DDL entry format).
    bool has_recent_samples_definition
        = hasInnerColumns(create_query, ViewTarget::RecentSamples) || hasInnerEngine(create_query, ViewTarget::RecentSamples)
        || hasTargetTableID(create_query, ViewTarget::RecentSamples);

    /// The definition of the table from the clause `AS <other_table>` if any, and its resolved types.
    boost::intrusive_ptr<const ASTCreateQuery> old_create_query;
    std::optional<ResolvedTimeSeriesTypes> old_types;
    if (!create_query.as_table.empty())
    {
        old_create_query = getASCreateQuery(create_query, context);
        /// The types of the old table are normally in its outer and inner columns, so its external target tables are rarely read.
        old_types = resolveTimeSeriesTypes(
            *old_create_query,
            context,
            /* check_external_targets = */ false,
            /* check_external_targets_for_missing_types = */ is_new_table,
            /* need_inner_engine_family = */ is_new_table,
            /* fallback_types = */ nullptr);
    }

    /// Resolve types timestamp_type, scalar_type, id_type.
    /// External targets are checked only at CREATE time; on ATTACH they may not be loaded yet.
    /// The external target tables of a new table must exist.
    ResolvedTimeSeriesTypes resolved_types = resolveTimeSeriesTypes(
        create_query,
        context,
        /* check_external_targets = */ is_new_table,
        /* check_external_targets_for_missing_types = */ is_new_table,
        /* need_inner_engine_family = */ is_new_table,
        old_types ? &*old_types : nullptr);

    /// Apply the clause `AS <other_table>` if any.
    /// This must happen before pinning the version below: the AS clause merges the SETTINGS clause
    /// of the old table into the query, and the version is pinned in the merged settings.
    if (old_create_query)
        applyASClause(create_query, resolved_types, *old_create_query, *old_types);

    /// For new tables: per-kind, check external tables or normalize the inner table's columns and assign its engine.
    if (is_new_table)
    {
        TimeSeriesSettings settings;
        if (create_query.storage)
            settings.loadFromQuery(*create_query.storage);

        /// This also checks that the version is in the range supported by this server.
        checkTimeSeriesSettings(settings);

        /// Pin `version`, so that the table keeps its version if a future server bumps the latest one.
        /// Converted queries have a version at this point (see above), so a missing version here means a fresh CREATE.
        if (!settings[TimeSeriesSetting::version].isChanged() && create_query.storage)
        {
            setEngineSettings(*create_query.storage, "version",
                Field(settings[TimeSeriesSetting::version].value));
        }

        /// Pin `recent_samples_ttl_seconds`, so that the table keeps its TTL if a future version changes the default.
        if (!settings[TimeSeriesSetting::recent_samples_ttl_seconds].isChanged() && create_query.storage)
        {
            setEngineSettings(*create_query.storage, "recent_samples_ttl_seconds",
                Field(settings[TimeSeriesSetting::recent_samples_ttl_seconds].value));
        }

        const bool recent_samples_enabled = settings[TimeSeriesSetting::recent_samples_ttl_seconds] != 0;

        /// A RECENT SAMPLES declaration can't be used with `recent_samples_ttl_seconds = 0`
        if (!recent_samples_enabled)
        {
            if (has_recent_samples_definition)
                throw Exception(ErrorCodes::INCORRECT_QUERY,
                    "The RECENT SAMPLES target requires the setting `recent_samples_ttl_seconds` to be set to a non-zero value");
            /// A RECENT SAMPLES definition inherited from the `AS <other_table>` clause is just removed
            /// when `recent_samples_ttl_seconds = 0` disables it.
            if (create_query.targets)
                create_query.targets->removeTarget(ViewTarget::RecentSamples);
        }

        for (auto kind : getTargetKinds())
        {
            /// The recent samples target is on by default and disabled by an explicit `recent_samples_ttl_seconds = 0`.
            if ((kind == ViewTarget::RecentSamples) && !recent_samples_enabled)
                continue;

            if (hasTargetTableID(create_query, kind))
            {
                /// An external target table is specified — check it has all the required columns.
                auto target_table_id = create_query.getTargetTableID(kind);
                auto target_table = DatabaseCatalog::instance().getTable(target_table_id, context);
                auto target_metadata = target_table->getInMemoryMetadataPtr(context, false);
                checkTargetTable(target_metadata->columns, kind, settings, resolved_types, target_table_id);
            }
            else
            {
                /// An inner target table should be used. Normalize its column definitions and assign a table engine if not specified.
                StorageID table_id{create_query.getDatabase(), create_query.getTable()};

                auto inner_columns = create_query.getTargetInnerColumns(kind)
                    ? boost::static_pointer_cast<ASTColumns>(create_query.getTargetInnerColumns(kind)->clone())
                    : make_intrusive<ASTColumns>();
                if (normalizeInnerColumns(*inner_columns, kind, settings, resolved_types, table_id))
                    create_query.setTargetInnerColumns(kind, inner_columns);

                /// Validate the user-provided types of the inner columns the same way external targets are validated.
                auto inner_columns_description = InterpreterCreateQuery::getColumnsDescription(
                    *inner_columns->columns, context, mode);
                checkTargetTable(inner_columns_description, kind, settings, resolved_types, table_id);

                auto inner_engine = create_query.getTargetInnerEngine(kind)
                    ? boost::static_pointer_cast<ASTStorage>(create_query.getTargetInnerEngine(kind)->clone())
                    : make_intrusive<ASTStorage>();
                if (normalizeInnerEngine(*inner_engine, kind, settings, resolved_types, table_id, context))
                    create_query.setTargetInnerEngine(kind, inner_engine);
            }
        }

        /// Inner tables with different replication types would diverge between replicas.
        /// The generated inner engines follow the declared ones, but an inner engine copied from the old table can differ.
        checkInnerEnginesReplicationTypesMatch(create_query, StorageID{create_query.getDatabase(), create_query.getTable()});
    }

    /// Regenerate the columns of TimeSeries table from the resolved types.
    /// We can change the columns of TimeSeries table because these columns are designed to work
    /// as IO interface. They store no data, in fact the data is stored in target or inner columns.
    {
        auto new_columns_ast = make_intrusive<ASTColumns>();
        new_columns_ast->set(new_columns_ast->columns,
            InterpreterCreateQuery::formatColumns(generateTimeSeriesColumns(resolved_types.timestamp_type, resolved_types.scalar_type)));
        const auto * old_columns = create_query.columns_list;
        if (!old_columns
            || !old_columns->columns
            || old_columns->formatWithSecretsOneLine() != new_columns_ast->formatWithSecretsOneLine())
        {
            create_query.set(create_query.columns_list, new_columns_ast);
        }
    }
}

}
