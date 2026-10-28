#include <Storages/TimeSeries/normalizeTimeSeriesDefinitionImpl.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Core/Settings.h>
#include <Core/SettingsEnums.h>
#include <Core/UUID.h>
#include <DataTypes/DataTypeCustomSimpleAggregateFunction.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/pullUpTupleElementDefaults.h>
#include <Interpreters/StorageID.h>
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
#include <Storages/ColumnsDescription.h>
#include <Parsers/ASTCodecDeclaration.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTTTLElement.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Parsers/Prometheus/CreateQueryTimeSeriesSettings.h>
#include <Parsers/Prometheus/TimeSeriesVersion.h>
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
    extern const TimeSeriesSettingsDataType id_type;
    extern const TimeSeriesSettingsUInt64 recent_samples_bucket_step_seconds;
    extern const TimeSeriesSettingsUInt64 recent_samples_index_granularity;
    extern const TimeSeriesSettingsUInt64 recent_samples_index_granularity_bytes;
    extern const TimeSeriesSettingsASTFunction recent_samples_partition_by;
    extern const TimeSeriesSettingsUInt64 recent_samples_ttl_seconds;
    extern const TimeSeriesSettingsUInt64 samples_bucket_step_seconds;
    extern const TimeSeriesSettingsUInt64 samples_index_granularity;
    extern const TimeSeriesSettingsUInt64 samples_index_granularity_bytes;
    extern const TimeSeriesSettingsASTFunction samples_partition_by;
    extern const TimeSeriesSettingsBool store_min_time_and_max_time;
    extern const TimeSeriesSettingsBool store_time_ranges;
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
    extern const int LOGICAL_ERROR;
    extern const int THERE_IS_NO_COLUMN;
}


namespace
{
    /// All target kinds of a TimeSeries table.
    /// The RecentSamples and TimeRanges targets are optional: they're enabled by the `recent_samples_ttl_seconds`
    /// and `store_time_ranges` settings (see `isOptionalTarget` in StorageTimeSeries.cpp).
    constexpr std::array<ViewTarget::Kind, 5> getTargetKinds()
    {
        return {ViewTarget::Samples, ViewTarget::RecentSamples, ViewTarget::Tags, ViewTarget::TimeRanges, ViewTarget::MetricFamilies};
    }

    /// If `type` is `Array(Tuple(timestamp_type, value_type))`, optionally wrapped in `SimpleAggregateFunction`, returns
    /// `timestamp_type` and `value_type`, otherwise returns two null pointers. The elements must be named `timestamp` and `value`,
    /// as in the `samples` column of a samples table, or have no names if `allow_unnamed_tuple` is set (the outer `samples` column).
    std::pair<DataTypePtr, DataTypePtr> trySplitSamplesType(const DataTypePtr & type, bool allow_unnamed_tuple)
    {
        /// A type wrapped in `SimpleAggregateFunction` is the nested type with a custom name, so no unwrapping is needed.
        const auto * array_type = typeid_cast<const DataTypeArray *>(type.get());
        const auto * tuple_type = array_type ? typeid_cast<const DataTypeTuple *>(array_type->getNestedType().get()) : nullptr;
        if (!tuple_type || (tuple_type->getElements().size() != 2))
            return {};

        const bool names_ok = tuple_type->hasExplicitNames()
            ? (tuple_type->getElementNames() == Strings{TimeSeriesColumnNames::Timestamp, TimeSeriesColumnNames::Value})
            : allow_unnamed_tuple;
        if (!names_ok)
            return {};

        return {tuple_type->getElements()[0], tuple_type->getElements()[1]};
    }

    /// Whether `type` is `SimpleAggregateFunction(timeSeriesGroupArray, ...)`: the aggregating engine of a samples table
    /// merges the rows of the same series and bucket with this function.
    bool isTimeSeriesGroupArraySimpleAggregate(const DataTypePtr & type)
    {
        const auto * simple_aggregate_function = typeid_cast<const DataTypeCustomSimpleAggregateFunction *>(type->getCustomName());
        return simple_aggregate_function && (simple_aggregate_function->getFunctionName() == "timeSeriesGroupArray");
    }

    /// Normalize `DEFAULT` expressions written inside `Tuple` data types by pulling them up to the column
    /// level for every column declaration of a `TimeSeries` table - both the outer columns and the columns
    /// of the inner tables (`SAMPLES INNER COLUMNS (...)` and so on). This must happen before any of the
    /// declared types is reified with `DataTypeFactory`, because a type AST that still carries a `DEFAULT`
    /// is rejected by `DataTypeTuple::create`.
    /// See https://github.com/ClickHouse/ClickHouse/issues/2797.
    void pullUpTupleElementDefaultsInColumnList(ASTColumns * columns_list)
    {
        if (!columns_list || !columns_list->columns)
            return;

        for (const auto & column : columns_list->columns->children)
            pullUpTupleElementDefaults(column->as<ASTColumnDeclaration &>());
    }

    void pullUpTupleElementDefaultsInTimeSeriesDefinition(ASTCreateQuery & create_query)
    {
        pullUpTupleElementDefaultsInColumnList(create_query.columns_list);

        for (auto kind : getTargetKinds())
            pullUpTupleElementDefaultsInColumnList(create_query.getTargetInnerColumns(kind));
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
    /// If the column with samples is found and it is declared with type `Array(Tuple(timestamp_type, value_type))`,
    /// the function extracts `timestamp_type` and `value_type`. Both names of the column, `samples` and `time_series`,
    /// are accepted whatever the version of the table is: the column is regenerated under the name of the version afterwards.
    void readTypesFromOuterColumns(
        const ASTCreateQuery & query,
        DataTypePtr & timestamp_type, String & timestamp_src,
        DataTypePtr & value_type, String & value_src,
        const StorageID & table_id)
    {
        if (!query.columns_list || !query.columns_list->columns)
            return;

        for (const auto & column : query.columns_list->columns->children)
        {
            auto column_declaration = boost::static_pointer_cast<ASTColumnDeclaration>(column);
            const auto & name = column_declaration->name;

            if ((name == TimeSeriesColumnNames::Samples || name == TimeSeriesColumnNames::TimeSeries) && column_declaration->getType())
            {
                auto column_type = DataTypeFactory::instance().get(column_declaration->getType());
                auto [samples_timestamp_type, samples_value_type] = trySplitSamplesType(column_type, /* allow_unnamed_tuple = */ true);
                if (!samples_timestamp_type || !samples_value_type)
                    throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD,
                        "{}: Column `{}` must have type Array(Tuple(timestamp, value)), got {}",
                        table_id.getNameForLogs(), name, column_type->getName());

                String source = fmt::format("outer column `{}`", name);
                setOrCheckDataType(timestamp_type, timestamp_src, samples_timestamp_type, source, "timestamp", table_id);
                setOrCheckDataType(value_type, value_src, samples_value_type, source, "value", table_id);
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

    /// Reads SAMPLES INNER COLUMNS declarations and extracts types `timestamp_type`, `value_type`, `id_type`.
    /// A table with buckets of samples declares the timestamp and value types in the `samples` column,
    /// the tables of earlier versions declare them in the `timestamp` and `value` columns.
    void readTypesFromInnerSamples(
        const ASTCreateQuery & query,
        DataTypePtr & timestamp_type, String & timestamp_src,
        DataTypePtr & value_type, String & value_src,
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
                setOrCheckDataType(value_type, value_src, column_type, "samples inner column `value`", "value", table_id);
            else if (column_declaration->name == TimeSeriesColumnNames::ID)
                setOrCheckDataType(id_type, id_src, column_type, "samples inner column `id`", "id", table_id);
            else if (column_declaration->name == TimeSeriesColumnNames::Samples)
            {
                auto [samples_timestamp_type, samples_value_type] = trySplitSamplesType(column_type, /* allow_unnamed_tuple = */ false);
                if (!samples_timestamp_type || !samples_value_type || !isTimeSeriesGroupArraySimpleAggregate(column_type))
                    throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD,
                        "{}: Column `{}` must have type SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(timestamp <timestamp type>, value <value type>))), got {}",
                        table_id.getNameForLogs(), TimeSeriesColumnNames::Samples, column_type->getName());
                setOrCheckDataType(timestamp_type, timestamp_src, samples_timestamp_type, "samples inner column `samples`", "timestamp", table_id);
                setOrCheckDataType(value_type, value_src, samples_value_type, "samples inner column `samples`", "value", table_id);
            }
        }
    }

    /// Reads the INNER COLUMNS declarations of the "tags" or "time ranges" target and extracts type `id_type`.
    void readIdTypeFromInnerColumns(
        const ASTCreateQuery & query,
        ViewTarget::Kind kind, std::string_view table_kind_name,
        DataTypePtr & id_type, String & id_src,
        const StorageID & table_id)
    {
        const auto * inner_columns = query.getTargetInnerColumns(kind);
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
                setOrCheckDataType(id_type, id_src, column_type, fmt::format("{} inner column `id`", table_kind_name), "id", table_id);
            }
        }
    }

    /// Reads the declaration of the external samples target table and
    /// extract types `timestamp_type`, `value_type`, id_type`.
    void readTypesFromExternalSamples(
        std::string_view table_kind_name,
        const StorageID & external_table_id, const ColumnsDescription & external_columns,
        DataTypePtr & timestamp_type, String & timestamp_src,
        DataTypePtr & value_type, String & value_src,
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
                setOrCheckDataType(value_type, value_src, column.type,
                    fmt::format("column `{}` of the external `{}` table {}", column.name, table_kind_name, external_table_id.getNameForLogs()),
                    "value", table_id);
            else if (column.name == TimeSeriesColumnNames::ID)
                setOrCheckDataType(id_type, id_src, column.type,
                    fmt::format("column `{}` of the external `{}` table {}", column.name, table_kind_name, external_table_id.getNameForLogs()),
                    "id", table_id);
        }
    }

    /// Reads the declaration of the external "tags" or "time ranges" target table and extracts type `id_type`.
    void readIdTypeFromExternalTable(
        std::string_view table_kind_name,
        const StorageID & external_table_id, const ColumnsDescription & external_columns,
        DataTypePtr & id_type, String & id_src,
        const StorageID & table_id)
    {
        for (const auto & column : external_columns)
        {
            if (column.name != TimeSeriesColumnNames::ID)
                continue;

            setOrCheckDataType(id_type, id_src, column.type,
                fmt::format("column `{}` of the external `{}` table {}", column.name, table_kind_name, external_table_id.getNameForLogs()),
                "id", table_id);
        }
    }

    /// Reads types from the columns of the external target tables referenced in a CREATE query.
    /// `external_target_columns` contains the columns of the external target tables which were read,
    /// the targets missing there are skipped.
    void readTypesFromExternalTargets(
        const ASTCreateQuery & query, const std::map<ViewTarget::Kind, ColumnsDescription> & external_target_columns,
        DataTypePtr & timestamp_type, String & timestamp_src,
        DataTypePtr & value_type, String & value_src,
        DataTypePtr & id_type, String & id_src,
        const StorageID & table_id)
    {
        auto find_external = [&](ViewTarget::Kind kind) -> const ColumnsDescription *
        {
            auto it = external_target_columns.find(kind);
            if ((it == external_target_columns.end()) || !query.hasTargetTableID(kind))
                return nullptr;
            return &it->second;
        };

        if (const auto * samples_columns = find_external(ViewTarget::Samples))
            readTypesFromExternalSamples("samples", query.getTargetTableID(ViewTarget::Samples), *samples_columns,
                                         timestamp_type, timestamp_src, value_type, value_src, id_type, id_src,
                                         table_id);

        /// An external recent-samples table has the same layout as an external samples table,
        /// and it can be the only declared source of the column types.
        if (const auto * recent_samples_columns = find_external(ViewTarget::RecentSamples))
            readTypesFromExternalSamples("recent samples", query.getTargetTableID(ViewTarget::RecentSamples), *recent_samples_columns,
                                         timestamp_type, timestamp_src, value_type, value_src, id_type, id_src,
                                         table_id);

        if (const auto * tags_columns = find_external(ViewTarget::Tags))
            readIdTypeFromExternalTable("tags", query.getTargetTableID(ViewTarget::Tags), *tags_columns, id_type, id_src, table_id);

        if (const auto * time_ranges_columns = find_external(ViewTarget::TimeRanges))
            readIdTypeFromExternalTable("time ranges", query.getTargetTableID(ViewTarget::TimeRanges), *time_ranges_columns, id_type, id_src, table_id);
    }

    /// Reads the `id_type` setting from the SETTINGS clause of a CREATE query and extracts type `id_type`.
    void readIdTypeFromSettings(
        const ASTCreateQuery & query,
        DataTypePtr & id_type, String & id_src,
        const StorageID & table_id)
    {
        if (!query.storage)
            return;
        TimeSeriesSettings settings;
        settings.loadFromQuery(*query.storage);
        setOrCheckDataType(id_type, id_src, settings[TimeSeriesSetting::id_type].value, "setting `id_type`", "id", table_id);
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
        DataTypePtr value_type;
        DataTypePtr id_type;

        /// The family of the engines of the inner tables: `MergeTree`, `ReplicatedMergeTree` or `SharedMergeTree`.
        /// Unset if no inner engine is declared, see `getDefaultInnerEngineFamily`.
        std::optional<DefaultTableEngine> inner_engine_family;
    };

    /// Resolves the types of the `timestamp`, `value` and `id` columns from the declarations in the query
    /// (the columns, the `id_type` setting, and the external target tables whose columns are in `external_target_columns`),
    /// then from `fallback_types` (the resolved types of the table from the clause `AS <other_table>`), then from the defaults.
    /// `need_inner_engine_family` is set if the family of the inner engines is needed to generate inner engines.
    ResolvedTimeSeriesTypes resolveTimeSeriesTypes(
        const ASTCreateQuery & create_query,
        const std::map<ViewTarget::Kind, ColumnsDescription> & external_target_columns,
        bool need_inner_engine_family,
        const ResolvedTimeSeriesTypes * fallback_types)
    {
        StorageID table_id{create_query.getDatabase(), create_query.getTable()};

        /// A type declared in several places must be the same everywhere.
        ResolvedTimeSeriesTypes types;
        String timestamp_src;
        String value_src;
        String id_src;

        readTypesFromOuterColumns(create_query,
            types.timestamp_type, timestamp_src, types.value_type, value_src, table_id);

        readTypesFromInnerSamples(create_query,
            types.timestamp_type, timestamp_src, types.value_type, value_src, types.id_type, id_src, table_id);

        readIdTypeFromInnerColumns(create_query, ViewTarget::Tags, "tags",
            types.id_type, id_src, table_id);

        readIdTypeFromInnerColumns(create_query, ViewTarget::TimeRanges, "time ranges",
            types.id_type, id_src, table_id);

        readIdTypeFromSettings(create_query,
            types.id_type, id_src, table_id);

        readTypesFromExternalTargets(create_query, external_target_columns,
            types.timestamp_type, timestamp_src,
            types.value_type, value_src,
            types.id_type, id_src,
            table_id);

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
            if (!types.value_type)
                types.value_type = fallback_types->value_type;
            if (!types.id_type)
                types.id_type = fallback_types->id_type;
            if (!types.inner_engine_family)
                types.inner_engine_family = fallback_types->inner_engine_family;
        }

        /// Apply defaults for unset types.
        if (!types.timestamp_type)
            types.timestamp_type = std::make_shared<DataTypeDateTime64>(3);
        if (!types.value_type)
            types.value_type = std::make_shared<DataTypeFloat64>();
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
            WhichDataType value_which{*types.value_type};
            if (!(value_which.isFloat64() || value_which.isFloat32()))
                throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "{}: Unexpected type {} of the {} column",
                    table_id.getNameForLogs(), types.value_type->getName(), TimeSeriesColumnNames::Value);
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

    /// The family of the inner engines when none is declared: it follows the `default_table_engine` setting.
    /// The value `None` is kept: then the inner engines must be declared explicitly.
    DefaultTableEngine getDefaultInnerEngineFamily(const Settings & query_settings, const StorageID & table_id)
    {
        auto default_table_engine = query_settings[Setting::default_table_engine].value;
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

    /// The engine kind of an inner table generated for `settings`, without the family prefix (see `getInnerEngineFamilyPrefix`).
    /// The tables merging rows need an aggregating engine: the samples tables of a table with buckets of samples (the rows of the same
    /// series and bucket), the time ranges table (the range of a series), and the tags table of the earlier versions aggregating
    /// `min_time` and `max_time`. The other tables use `MergeTree` or `ReplacingMergeTree`.
    std::string_view getDefaultInnerEngineKind(const TimeSeriesSettings & settings, ViewTarget::Kind inner_table_kind)
    {
        switch (inner_table_kind)
        {
            case ViewTarget::Samples:
            case ViewTarget::RecentSamples:
                return settings.hasBucketedSamples() ? "AggregatingMergeTree" : "MergeTree";

            case ViewTarget::Tags:
            {
                bool aggregate = (settings[TimeSeriesSetting::version] < TimeSeriesVersion::MIN_WITH_TIME_RANGES_TARGET)
                    && settings[TimeSeriesSetting::aggregate_min_time_and_max_time];
                return aggregate ? "AggregatingMergeTree" : "ReplacingMergeTree";
            }

            case ViewTarget::TimeRanges:
                return "AggregatingMergeTree";

            case ViewTarget::MetricFamilies:
                return "ReplacingMergeTree";

            default:
                UNREACHABLE();
        }
    }

    /// The codecs of the generated columns of the samples tables: `Delta`, `T64`, `ZSTD(3)` for the timestamps (the columns `bucket`,
    /// `min_time` and `max_time` of a table with buckets of samples get it too), `ALP`, `ZSTD(3)` for the values.
    /// Near-monotonic timestamps barely compress under generic codecs.
    ASTPtr getDefaultTimestampCodec()
    {
        return makeASTFunction(
            "CODEC",
            make_intrusive<ASTIdentifier>("Delta"),
            make_intrusive<ASTIdentifier>("T64"),
            makeASTFunction("ZSTD", make_intrusive<ASTLiteral>(UInt64{3})));
    }

    ASTPtr getDefaultValueCodec()
    {
        return makeASTFunction("CODEC", make_intrusive<ASTIdentifier>("ALP"), makeASTFunction("ZSTD", make_intrusive<ASTLiteral>(UInt64{3})));
    }

    /// The value of `index_granularity` of an inner samples table generated for `settings`: the `*_index_granularity` setting if set,
    /// otherwise the default of the setting for a table with buckets of samples (its rows are much bigger than the rows
    /// of the tables of earlier versions, so the default is smaller), or the default of the earlier versions.
    UInt64 getIndexGranularityFromSettings(const TimeSeriesSettings & settings, ViewTarget::Kind inner_table_kind)
    {
        chassert((inner_table_kind == ViewTarget::Samples) || (inner_table_kind == ViewTarget::RecentSamples));
        const bool is_samples = (inner_table_kind == ViewTarget::Samples);

        const auto & setting = settings[is_samples ? TimeSeriesSetting::samples_index_granularity : TimeSeriesSetting::recent_samples_index_granularity];
        if (setting.isChanged() || settings.hasBucketedSamples())
            return setting.value;
        return is_samples ? 32768 : 8192;
    }

    ASTPtr getDefaultTagsIndex()
    {
        auto index = make_intrusive<ASTIndexDeclaration>(
            make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Tags),
            makeASTFunction("text", makeASTOperator("equals",
                make_intrusive<ASTIdentifier>("tokenizer"), make_intrusive<ASTLiteral>("keyValuePairs"))),
            "tags_idx");
        /// A text index covers the whole part; its posting lists identify individual rows.
        index->granularity = ASTIndexDeclaration::DEFAULT_TEXT_INDEX_GRANULARITY;
        return index;
    }

    /// The generated partition key of a samples table with buckets of samples: one partition per month. `toDateTime` makes the key
    /// work for any type of the timestamps, and the timezone is named because `toYYYYMM` counts from local midnight, so a change
    /// of the server timezone would otherwise move the meaning of the partition values already written.
    ASTPtr getDefaultSamplesPartitionKey()
    {
        return makeASTFunction("toYYYYMM",
            makeASTFunction("toDateTime", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Bucket), make_intrusive<ASTLiteral>(String{"UTC"})));
    }

    /// The generated partition key of an inner recent samples table, as the given version spells it: 5-hour intervals
    /// of the start of the bucket (with buckets of samples) or of the timestamp (earlier versions).
    /// `toDateTime` makes the key work for any type of the timestamps (e.g. a raw `UInt32`).
    /// `toStartOfInterval` counts hours from local midnight, so from `MIN_WITH_UTC_PARTITION_KEY` the
    /// timezone is named instead of being taken from the server.
    ASTPtr getDefaultRecentSamplesPartitionKey(UInt64 version)
    {
        ASTPtr time;
        if (version >= TimeSeriesVersion::MIN_WITH_BUCKETED_SAMPLES)
            time = makeASTFunction("toDateTime", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Bucket), make_intrusive<ASTLiteral>(String{"UTC"}));
        else if (version >= TimeSeriesVersion::MIN_WITH_UTC_PARTITION_KEY)
            time = makeASTFunction("toDateTime", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Timestamp), make_intrusive<ASTLiteral>(String{"UTC"}));
        else
            time = makeASTFunction("toDateTime", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Timestamp));
        return makeASTFunction("toStartOfInterval", std::move(time), makeASTFunction("toIntervalHour", make_intrusive<ASTLiteral>(static_cast<UInt64>(5))));
    }

    /// The generated TTL expression of an inner recent samples table: a row expires when all its samples are older than
    /// `recent_samples_ttl_seconds`. The samples of a row of a table with buckets of samples have timestamps in
    /// [bucket, bucket + bucket_step), so the TTL counts from the start of the bucket plus the step.
    ASTPtr getRecentSamplesTTLFromSettings(const TimeSeriesSettings & settings)
    {
        UInt64 ttl_seconds = settings[TimeSeriesSetting::recent_samples_ttl_seconds];
        ASTPtr time;
        if (settings.hasBucketedSamples())
        {
            ttl_seconds += settings[TimeSeriesSetting::recent_samples_bucket_step_seconds];
            time = makeASTFunction("toDateTime", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Bucket));
        }
        else
        {
            time = makeASTFunction("toDateTime", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Timestamp));
        }
        return makeASTOperator("plus", std::move(time), makeASTFunction("toIntervalSecond", make_intrusive<ASTLiteral>(ttl_seconds)));
    }

    /// The codec declared by the entry `CODEC <name> (...)` of the column list of an inner table, where `name` is a column
    /// or a subcolumn of a column written as a dotted name (e.g. `samples.timestamp`), or nullptr if there is no such entry.
    ASTPtr getCodecDeclaration(const ASTColumns & inner_table_columns, std::string_view name)
    {
        if (!inner_table_columns.codec_declarations)
            return nullptr;

        for (const auto & child : inner_table_columns.codec_declarations->children)
        {
            const auto & declaration = child->as<const ASTCodecDeclaration &>();
            if (declaration.name == name)
                return declaration.getCodec();
        }
        return nullptr;
    }

    /// Returns the name of the column with the name of a metric family in the "metric families" table used by the versions
    /// of TimeSeries tables other than `version` (see `TimeSeriesColumnNames::getInnerMetricFamily`).
    const char * getInnerMetricFamilyOfOtherVersions(UInt64 version)
    {
        return (version >= TimeSeriesVersion::MIN_WITH_METRIC_FAMILY_INNER_COLUMN)
            ? TimeSeriesColumnNames::MetricFamilyName : TimeSeriesColumnNames::MetricFamily;
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

        /// The version of this table: written in the query, otherwise the latest one (the version of the old table is not copied).
        UInt64 new_version = TimeSeriesVersion::LATEST;
        if (const auto * value = get_new_value("version"))
            new_version = SettingFieldUInt64{*value}.value;

        /// The `id_type` setting exists from version 2 (see TimeSeriesVersion.h), so it isn't copied into a table pinned to an earlier version.
        if (new_version < TimeSeriesVersion::MIN_WITH_ID_TYPE_SETTING)
            old_settings.removeSetting("id_type");

        /// The `id` type and the expression generating identifiers are declared in the inner columns from version
        /// MIN_WITHOUT_ID_SETTINGS (see TimeSeriesVersion.h), so the settings declaring them aren't copied into a table of
        /// that version: the `id` type is still inherited through the resolved types, the expression is generated for it.
        if (new_version >= TimeSeriesVersion::MIN_WITHOUT_ID_SETTINGS)
            old_settings.removeSettings({"id_type", "id_generator"});

        /// The same for the settings of the deduplication caches which exist from version 7.
        if (new_version < TimeSeriesVersion::MIN_WITH_DEDUPLICATION_CACHES)
            old_settings.removeSettings({"metric_families_deduplication_cache_size_bytes", "metric_families_deduplication_cache_expiration_seconds",
                                        "tags_deduplication_cache_size_bytes", "tags_deduplication_cache_expiration_seconds"});

        /// The time range of a time series is stored in the "time ranges" table, tables of earlier versions store it in the "tags" table
        /// (see TimeSeriesVersion.h), so the settings of the other form aren't copied.
        if (new_version >= TimeSeriesVersion::MIN_WITH_TIME_RANGES_TARGET)
            old_settings.removeSettings({"store_min_time_and_max_time", "aggregate_min_time_and_max_time", "filter_by_min_time_and_max_time"});
        else
            old_settings.removeSetting("store_time_ranges");

        /// The settings of the buckets of samples exist from version MIN_WITH_BUCKETED_SAMPLES (see TimeSeriesVersion.h),
        /// so they aren't copied into a table pinned to an earlier version.
        if (new_version < TimeSeriesVersion::MIN_WITH_BUCKETED_SAMPLES)
            old_settings.removeSettings({"samples_bucket_step_seconds", "samples_partition_by", "samples_index_granularity_bytes",
                                         "recent_samples_bucket_step_seconds", "recent_samples_index_granularity_bytes"});

        /// The default value of `recent_samples_ttl_seconds` is 345600 (4 days), so an absent setting doesn't disable the recent samples table.
        if (const auto * value = get_new_value("recent_samples_ttl_seconds"); value && (SettingFieldUInt64{*value}.value == 0))
            old_settings.removeSettings({"recent_samples_bucket_step_seconds", "recent_samples_partition_by",
                                         "recent_samples_index_granularity", "recent_samples_index_granularity_bytes"});

        /// The columns `min_time` and `max_time` of the tags table exist in tables of earlier versions only, and the default
        /// value of `store_min_time_and_max_time` is true, so an absent setting doesn't disable them.
        /// The deduplication cache of the tags table is used only without these columns.
        if (new_version < TimeSeriesVersion::MIN_WITH_TIME_RANGES_TARGET)
        {
            if (const auto * value = get_new_value("store_min_time_and_max_time"); value && !SettingFieldBool{*value}.value)
                old_settings.removeSettings({"aggregate_min_time_and_max_time", "filter_by_min_time_and_max_time"});
            else
                old_settings.removeSettings({"tags_deduplication_cache_size_bytes", "tags_deduplication_cache_expiration_seconds"});
        }
    }

    /// Removes the settings copied from the old table which were written for its `id` type, if this table has another one.
    void removeOldSettingsDisabledByIdTypeChange(SettingsChanges & old_settings, const DataTypePtr & old_id_type, const DataTypePtr & new_id_type)
    {
        if (!old_id_type->equals(*new_id_type))
            old_settings.removeSettings({"id_type", "id_generator"});
    }

    /// Removes a column from the column list of an inner table together with the codecs declared for its subcolumns
    /// (the entries `CODEC <column>.<subcolumn> (...)` of the list). Nothing happens if there is no such column.
    void removeInnerColumn(ASTColumns & inner_table_columns, std::string_view column_name)
    {
        if (inner_table_columns.columns)
        {
            auto & columns = inner_table_columns.columns->children;
            auto has_name = [&](const ASTPtr & column) { return column->as<const ASTColumnDeclaration &>().name == column_name; };
            columns.erase(std::remove_if(columns.begin(), columns.end(), has_name), columns.end());
        }

        if (inner_table_columns.codec_declarations)
        {
            const String prefix = String{column_name} + ".";
            auto & declarations = inner_table_columns.codec_declarations->children;
            auto is_for_column = [&](const ASTPtr & child)
            {
                const auto & declared_name = child->as<const ASTCodecDeclaration &>().name;
                return (declared_name == column_name) || declared_name.starts_with(prefix);
            };
            declarations.erase(std::remove_if(declarations.begin(), declarations.end(), is_for_column), declarations.end());

            /// The clause goes away with its last declaration.
            if (declarations.empty())
                inner_table_columns.reset(inner_table_columns.codec_declarations);
        }
    }

    /// Removes the columns copied from the old table which the settings of this table disable.
    /// `old_settings` are the settings of the old table, `new_settings` are the settings of this table.
    void removeInnerColumnsDisabledByNewSettings(
        ASTColumns & inner_table_columns, ViewTarget::Kind inner_table_kind, const TimeSeriesSettings & old_settings, const TimeSeriesSettings & new_settings)
    {
        if (!inner_table_columns.columns)
            return;

        auto remove_column = [&](std::string_view name) { removeInnerColumn(inner_table_columns, name); };

        if (inner_table_kind == ViewTarget::MetricFamilies)
        {
            /// The column with the name of a metric family is named by the version (see TimeSeriesVersion.h),
            /// so a column under the name used by the other versions isn't kept.
            remove_column(getInnerMetricFamilyOfOtherVersions(new_settings[TimeSeriesSetting::version]));
            return;
        }

        if ((inner_table_kind == ViewTarget::Samples) || (inner_table_kind == ViewTarget::RecentSamples))
        {
            /// A table with buckets of samples has the columns `samples`, `bucket`, `min_time`, `max_time`, a table of an earlier
            /// version has the columns `timestamp`, `value` (see TimeSeriesVersion.h): the columns of the other layout aren't kept.
            if (new_settings.hasBucketedSamples() && !old_settings.hasBucketedSamples())
            {
                remove_column(TimeSeriesColumnNames::Timestamp);
                remove_column(TimeSeriesColumnNames::Value);
            }
            else if (!new_settings.hasBucketedSamples() && old_settings.hasBucketedSamples())
            {
                remove_column(TimeSeriesColumnNames::Samples);
                remove_column(TimeSeriesColumnNames::Bucket);
                remove_column(TimeSeriesColumnNames::MinTime);
                remove_column(TimeSeriesColumnNames::MaxTime);
            }
            return;
        }

        if (inner_table_kind != ViewTarget::Tags)
            return;

        /// This table doesn't store the columns "min_time" and "max_time" in the tags table
        /// (they are stored in the time ranges table, if any, which is generated for this table).
        if (!new_settings.hasMinTimeAndMaxTimeInTagsTable())
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
    /// `inner_table_columns` is the column list the column belongs to: the codecs of the subcolumns are declared there.
    bool isGeneratedInnerColumn(
        const ASTColumnDeclaration & column, const ASTColumns & inner_table_columns, ViewTarget::Kind inner_table_kind, const TimeSeriesSettings & settings)
    {
        if (!column.getType() || column.getComment() || column.getStatisticsDesc() || column.getTTL()
            || column.getCollation() || column.getSettings() || column.null_modifier || column.primary_key_specifier)
            return false;

        const auto & name = column.name;
        auto type = DataTypeFactory::instance().get(column.getType());
        auto type_name = type->getName();

        const bool is_version_0 = (settings[TimeSeriesSetting::version] == 0);
        const bool has_default = column.getDefaultExpression() || (column.default_specifier != ColumnDefaultSpecifier::Empty);

        /// Whether a declared expression is the same as a generated one when formatted.
        auto same_ast = [](const IAST & lhs, const IAST & rhs) { return lhs.formatWithSecretsOneLine() == rhs.formatWithSecretsOneLine(); };

        /// Any type accepted for "timestamp" and "value" counts, see `resolveTimeSeriesTypes`.
        auto is_value_type = [](const IDataType & value_type)
        {
            WhichDataType which{value_type};
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

        /// Whether the type is `SimpleAggregateFunction(min|max, <argument>)` with an argument accepted by `is_argument_ok`.
        auto is_min_or_max_simple_aggregate = [&](const auto & is_argument_ok)
        {
            const auto * simple_aggregate = typeid_cast<const DataTypeCustomSimpleAggregateFunction *>(type->getCustomName());
            if (!simple_aggregate)
                return false;
            std::string_view expected_function = (name == TimeSeriesColumnNames::MinTime) ? "min" : "max";
            const auto & argument_types = simple_aggregate->getArgumentsDataTypes();
            return (simple_aggregate->getFunctionName() == expected_function) && (argument_types.size() == 1)
                && is_argument_ok(argument_types[0]);
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

                if (settings.hasBucketedSamples())
                {
                    /// The generated "samples" column has no codec of its own, the codecs are declared for its elements
                    /// `timestamp` and `value` (see `normalizeInnerColumns`): the column is generated if it has no codecs at all
                    /// or exactly the generated ones.
                    if (name == TimeSeriesColumnNames::Samples)
                    {
                        auto [samples_timestamp_type, samples_value_type] = trySplitSamplesType(type, /* allow_unnamed_tuple = */ false);
                        if (!samples_timestamp_type || !samples_value_type || !is_timestamp_type(*samples_timestamp_type) || !is_value_type(*samples_value_type))
                            return false;

                        /// A codec of the column itself is its inline clause or the entry `CODEC samples (...)`, which mean the same.
                        if (codec || getCodecDeclaration(inner_table_columns, column.name))
                            return false;

                        ASTPtr timestamp_codec = getCodecDeclaration(inner_table_columns, column.name + "." + TimeSeriesColumnNames::Timestamp);
                        ASTPtr value_codec = getCodecDeclaration(inner_table_columns, column.name + "." + TimeSeriesColumnNames::Value);
                        if (!timestamp_codec && !value_codec)
                            return true;

                        return timestamp_codec && value_codec
                            && same_ast(*timestamp_codec, *getDefaultTimestampCodec()) && same_ast(*value_codec, *getDefaultValueCodec());
                    }

                    /// The generated `bucket`, `min_time` and `max_time` columns have the codec of the timestamps (see `normalizeInnerColumns`).
                    if (codec && !same_ast(*codec, *getDefaultTimestampCodec()))
                        return false;

                    if (name == TimeSeriesColumnNames::Bucket)
                        return is_timestamp_type(*type);

                    /// `SimpleAggregateFunction(min|max, <timestamp type>)`.
                    if ((name == TimeSeriesColumnNames::MinTime) || (name == TimeSeriesColumnNames::MaxTime))
                        return is_min_or_max_simple_aggregate([&](const DataTypePtr & argument_type) { return is_timestamp_type(*argument_type); });

                    return false;
                }

                /// The generated "timestamp" and "value" columns have codecs (see `normalizeInnerColumns`): the current ones
                /// or the ones generated by the earlier servers (`DoubleDelta, ZSTD(1)` and `ZSTD(3)`).
                /// The codecs are not checked for version 0: its columns are converted to the current form anyway.
                if (name == TimeSeriesColumnNames::Timestamp)
                {
                    if (!is_timestamp_type(*type))
                        return false;
                    if (!codec || is_version_0)
                        return true;
                    return same_ast(*codec, *getDefaultTimestampCodec()) || (codec->formatWithSecretsOneLine() == "CODEC(DoubleDelta, ZSTD(1))");
                }

                if (name == TimeSeriesColumnNames::Value)
                {
                    if (!is_value_type(*type))
                        return false;
                    if (!codec || is_version_0)
                        return true;
                    return same_ast(*codec, *getDefaultValueCodec()) || (codec->formatWithSecretsOneLine() == "CODEC(ZSTD(3))");
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
                    if (canonical_default && same_ast(*default_expression, *canonical_default))
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
                    return is_nullable_timestamp(type) || is_min_or_max_simple_aggregate(is_nullable_timestamp);
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

            case ViewTarget::TimeRanges:
            {
                if (has_default || codec)
                    return false;

                /// Any type counts because the type is also resolved from the old table (see `resolveTimeSeriesTypes`).
                if (name == TimeSeriesColumnNames::ID)
                    return true;

                /// `SimpleAggregateFunction(min|max, <timestamp type>)`.
                if ((name == TimeSeriesColumnNames::MinTime) || (name == TimeSeriesColumnNames::MaxTime))
                    return is_min_or_max_simple_aggregate([&](const DataTypePtr & argument_type) { return is_timestamp_type(*argument_type); });

                return false;
            }

            case ViewTarget::MetricFamilies:
            {
                if (has_default || codec)
                    return false;

                if ((name == TimeSeriesColumnNames::getInnerMetricFamily(settings[TimeSeriesSetting::version])) || (name == TimeSeriesColumnNames::Help))
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

    /// Removes the default `tags` index copied by `CREATE AS`, so it is generated for the new engine.
    void removeGeneratedInnerIndices(ASTColumns & inner_columns, ViewTarget::Kind kind)
    {
        if (kind != ViewTarget::Tags || !inner_columns.indices)
            return;

        const auto default_index = getDefaultTagsIndex()->formatWithSecretsOneLine();
        auto & indices = inner_columns.indices->children;
        auto is_generated = [&](const ASTPtr & index) { return index->formatWithSecretsOneLine() == default_index; };
        indices.erase(std::remove_if(indices.begin(), indices.end(), is_generated), indices.end());
    }

    /// Removes the generated columns (see `isGeneratedInnerColumn`) from an inner table's column list,
    /// so that `normalizeInnerColumns` generates them again from the current settings.
    void removeGeneratedInnerColumns(ASTColumns & inner_table_columns, ViewTarget::Kind inner_table_kind, const TimeSeriesSettings & settings)
    {
        if (!inner_table_columns.columns)
            return;

        /// The names are collected first because the removal changes the list being iterated.
        Strings generated_columns;
        for (const auto & column : inner_table_columns.columns->children)
        {
            const auto & column_declaration = column->as<const ASTColumnDeclaration &>();
            if (isGeneratedInnerColumn(column_declaration, inner_table_columns, inner_table_kind, settings))
                generated_columns.push_back(column_declaration.name);
        }

        for (const auto & name : generated_columns)
            removeInnerColumn(inner_table_columns, name);
    }

    /// Removes what this table can't keep from an inner engine copied from the old table: the engine if this table generates
    /// another kind (then `normalizeInnerEngine` generates it), and the keys referring to columns this table doesn't have.
    /// `old_settings` are the settings of the old table, `new_settings` are the settings of this table.
    void removeInnerEngineDisabledByNewSettings(
        ASTStorage & inner_engine, ViewTarget::Kind inner_table_kind, const TimeSeriesSettings & old_settings, const TimeSeriesSettings & new_settings)
    {
        /// An engine of another kind than this table generates (e.g. `MergeTree` for a table which must merge rows) isn't kept:
        /// it's generated again with the same family, without the arguments of the old engine.
        if (getDefaultInnerEngineKind(old_settings, inner_table_kind) != getDefaultInnerEngineKind(new_settings, inner_table_kind))
            inner_engine.reset(inner_engine.engine);

        /// The names of the columns the keys of the old table can refer to which this table doesn't have.
        std::vector<std::string_view> missing_columns;

        if (inner_table_kind == ViewTarget::MetricFamilies)
        {
            /// The column with the name of a metric family is named by the version (see TimeSeriesVersion.h),
            /// so the keys referring to it under the name used by the other versions can't be kept.
            missing_columns.emplace_back(getInnerMetricFamilyOfOtherVersions(new_settings[TimeSeriesSetting::version]));
        }
        else if ((inner_table_kind == ViewTarget::Samples) || (inner_table_kind == ViewTarget::RecentSamples))
        {
            /// A table with buckets of samples has the columns `samples`, `bucket`, `min_time`, `max_time`, a table of an earlier
            /// version has the columns `timestamp`, `value` (see TimeSeriesVersion.h): the keys over the columns of the other
            /// layout can't be kept.
            if (new_settings.hasBucketedSamples() && !old_settings.hasBucketedSamples())
                missing_columns = {TimeSeriesColumnNames::Timestamp, TimeSeriesColumnNames::Value};
            else if (!new_settings.hasBucketedSamples() && old_settings.hasBucketedSamples())
                missing_columns = {TimeSeriesColumnNames::Samples, TimeSeriesColumnNames::Bucket, TimeSeriesColumnNames::MinTime, TimeSeriesColumnNames::MaxTime};
        }
        else if (inner_table_kind == ViewTarget::Tags)
        {
            /// The columns `min_time` and `max_time` exist in the tags table of the earlier versions only (see TimeSeriesVersion.h):
            /// the keys over them can't be kept by a table which doesn't store them there.
            if (old_settings.hasMinTimeAndMaxTimeInTagsTable() && !new_settings.hasMinTimeAndMaxTimeInTagsTable())
                missing_columns = {TimeSeriesColumnNames::MinTime, TimeSeriesColumnNames::MaxTime};
        }

        if (missing_columns.empty())
            return;

        auto references_missing_column = [&](const IAST * key)
        {
            IdentifierNameSet identifiers;
            if (key)
                key->collectIdentifierNames(identifiers);
            return std::any_of(missing_columns.begin(), missing_columns.end(),
                               [&](std::string_view name) { return identifiers.contains(String{name}); });
        };

        /// The primary key and the sorting key go together: a surviving primary key alone would become the sorting key,
        /// shorter than the generated one.
        if (references_missing_column(inner_engine.order_by) || references_missing_column(inner_engine.primary_key))
        {
            inner_engine.reset(inner_engine.order_by);
            inner_engine.reset(inner_engine.primary_key);
        }
        if (references_missing_column(inner_engine.partition_by))
            inner_engine.reset(inner_engine.partition_by);
        if (references_missing_column(inner_engine.ttl_table))
            inner_engine.reset(inner_engine.ttl_table);
    }

    /// Strips generated inner engine clauses (partition by, TTL, settings) so they are regenerated.
    /// Custom engine kinds or engines with arguments are preserved.
    /// `settings` are the settings of the table the engine was generated for.
    void removeGeneratedInnerEngine(ASTStorage & inner_engine, ViewTarget::Kind inner_table_kind, const TimeSeriesSettings & settings)
    {
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

        /// A generated engine (the kind generated for the old settings, without arguments) is dropped to be generated again.
        /// Its clauses are checked below either way: a declared engine can carry generated keys and settings.
        if (inner_engine.engine)
        {
            const auto & engine = *inner_engine.engine;
            const bool has_arguments = engine.parameters || (engine.arguments && !engine.arguments->children.empty());

            std::string_view engine_name = engine.name;
            if (engine_name.starts_with("Replicated"))
                engine_name.remove_prefix(strlen("Replicated"));
            else if (engine_name.starts_with("Shared"))
                engine_name.remove_prefix(strlen("Shared"));

            if (!has_arguments && (engine_name == getDefaultInnerEngineKind(settings, inner_table_kind)))
                inner_engine.reset(inner_engine.engine);
        }

        switch (inner_table_kind)
        {
            case ViewTarget::Samples:
            case ViewTarget::RecentSamples:
            {
                /// The generated sorting key and settings follow the version of the old table (see TimeSeriesVersion.h):
                /// a table with buckets of samples merges the rows of the same bucket, the tables of earlier versions don't merge rows.
                const bool bucketed = settings.hasBucketedSamples();
                if (!inner_engine.primary_key && sorting_key_equals(bucketed ? "id, bucket" : "id, timestamp"))
                    inner_engine.reset(inner_engine.order_by);

                std::unordered_map<std::string_view, Field> expected_inner_settings{{"index_granularity", getIndexGranularityFromSettings(settings, inner_table_kind)}};
                if (bucketed)
                {
                    const auto & index_granularity_bytes = settings[(inner_table_kind == ViewTarget::Samples)
                        ? TimeSeriesSetting::samples_index_granularity_bytes : TimeSeriesSetting::recent_samples_index_granularity_bytes];
                    expected_inner_settings.emplace("index_granularity_bytes", index_granularity_bytes.value);
                }

                if (inner_table_kind == ViewTarget::Samples)
                {
                    /// The partition key of a table with buckets of samples is the `samples_partition_by` setting if set, otherwise the default one.
                    /// The tables of earlier versions were generated without a partition key.
                    if (bucketed)
                    {
                        const auto & partition_by = settings[TimeSeriesSetting::samples_partition_by].value;
                        String expected_partition_by = partition_by
                            ? partition_by->formatWithSecretsOneLine()
                            : getDefaultSamplesPartitionKey()->formatWithSecretsOneLine();
                        if (partitioning_equals(expected_partition_by))
                            inner_engine.reset(inner_engine.partition_by);
                    }
                }
                else
                {
                    /// The partition key is the `recent_samples_partition_by` setting if set, otherwise the default one.
                    const auto & partition_by = settings[TimeSeriesSetting::recent_samples_partition_by].value;
                    /// The default names a timezone only from the version that introduced it, so an older table's
                    /// own spelling is still recognised as generated and does not survive into a copy of it.
                    String expected_partition_by = partition_by
                        ? partition_by->formatWithSecretsOneLine()
                        : getDefaultRecentSamplesPartitionKey(settings[TimeSeriesSetting::version])->formatWithSecretsOneLine();
                    if (partitioning_equals(expected_partition_by))
                        inner_engine.reset(inner_engine.partition_by);

                    if (ttl_equals(getRecentSamplesTTLFromSettings(settings)->formatWithSecretsOneLine()))
                        inner_engine.reset(inner_engine.ttl_table);

                    expected_inner_settings.emplace("ttl_only_drop_parts", static_cast<UInt64>(1));
                }

                remove_settings(expected_inner_settings);
                break;
            }

            case ViewTarget::Tags:
            {
                /// The primary key and the sorting key are connected, so they are considered together.
                bool primary_key_is_generated = !inner_engine.primary_key
                    || (inner_engine.primary_key->formatWithSecretsOneLine() == "metric_name");

                /// The generated sorting key contains `min_time` and `max_time` if they are stored in the tags table but not aggregated.
                /// Version 0 tables were also generated with the short key regardless of these settings.
                bool sorting_key_is_generated = sorting_key_equals(
                    settings.hasMinTimeAndMaxTimeInTagsSortingKey() ? "metric_name, id, min_time, max_time" : "metric_name, id");
                if (!sorting_key_is_generated && is_version_0)
                    sorting_key_is_generated = sorting_key_equals("metric_name, id");

                if (primary_key_is_generated && sorting_key_is_generated)
                {
                    /// `allow_nullable_key` is generated only for the nullable `min_time` and `max_time` in the sorting key.
                    /// Otherwise the user set it for their own nullable key column, so it is kept.
                    if (sorting_key_equals("metric_name, id, min_time, max_time"))
                        remove_settings({{"allow_nullable_key", static_cast<UInt64>(1)}});

                    inner_engine.reset(inner_engine.primary_key);
                    inner_engine.reset(inner_engine.order_by);
                }

                remove_settings({
                    {"index_granularity", settings[TimeSeriesSetting::tags_index_granularity].value},
                    {"allow_dimensions_outside_sorting_key", static_cast<UInt64>(1)}});
                break;
            }

            case ViewTarget::TimeRanges:
            {
                if (!inner_engine.primary_key && sorting_key_equals("id"))
                    inner_engine.reset(inner_engine.order_by);
                break;
            }

            case ViewTarget::MetricFamilies:
            {
                if (!inner_engine.primary_key && sorting_key_equals(TimeSeriesColumnNames::getInnerMetricFamily(settings[TimeSeriesSetting::version])))
                    inner_engine.reset(inner_engine.order_by);
                break;
            }

            default:
                UNREACHABLE();
        }
    }

    /// The type of the samples in a row of a samples table with buckets of samples: `Array(Tuple(timestamp <timestamp_type>, value <value_type>))`.
    DataTypePtr makeSamplesType(const DataTypePtr & timestamp_type, const DataTypePtr & value_type)
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(
            DataTypes{timestamp_type, value_type}, Strings{TimeSeriesColumnNames::Timestamp, TimeSeriesColumnNames::Value}));
    }

    /// Makes the AST of the type `SimpleAggregateFunction(<function_name>, <argument_type>)`.
    ASTPtr makeSimpleAggregate(const String & function_name, const DataTypePtr & argument_type)
    {
        AggregateFunctionProperties properties;
        auto function = AggregateFunctionFactory::instance().get(function_name, NullsAction::EMPTY, {argument_type}, {}, properties);
        auto custom_name = std::make_unique<DataTypeCustomSimpleAggregateFunction>(function, DataTypes{argument_type}, Array{});
        auto type = DataTypeFactory::instance().getCustom(std::make_unique<DataTypeCustomDesc>(std::move(custom_name)));
        return dataTypeToAST(type);
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

        /// Declares a codec for a subcolumn of a generated column: the entry `CODEC <column>.<subcolumn> (...)` of the column list.
        auto add_codec_declaration = [&](const String & subcolumn_name, ASTPtr codec)
        {
            if (!inner_table_columns.codec_declarations)
                inner_table_columns.set(inner_table_columns.codec_declarations, make_intrusive<ASTExpressionList>());
            inner_table_columns.codec_declarations->children.push_back(make_intrusive<ASTCodecDeclaration>(subcolumn_name, std::move(codec)));
            changed = true;
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

                if (time_series_settings.hasBucketedSamples())
                {
                    /// The generated `samples` column gets the codecs of the `timestamp` and `value` columns of the earlier versions, declared
                    /// for the elements of its tuples; a declared column keeps the user's codecs.
                    auto samples_type = makeSamplesType(resolved_types.timestamp_type, resolved_types.value_type);
                    if (add_column_if_missing(TimeSeriesColumnNames::Samples, makeSimpleAggregate("timeSeriesGroupArray", samples_type)))
                    {
                        add_codec_declaration(String{TimeSeriesColumnNames::Samples} + "." + TimeSeriesColumnNames::Timestamp, getDefaultTimestampCodec());
                        add_codec_declaration(String{TimeSeriesColumnNames::Samples} + "." + TimeSeriesColumnNames::Value, getDefaultValueCodec());
                    }

                    /// `bucket` is computed on insertion from the `*_bucket_step_seconds` setting of the table. The three columns get the codec
                    /// of the timestamps: they are sorted or nearly sorted timestamps, and they dominate the rows with few samples.
                    if (auto * bucket_decl = add_column_if_missing(TimeSeriesColumnNames::Bucket, dataTypeToAST(resolved_types.timestamp_type)))
                        bucket_decl->setCodec(getDefaultTimestampCodec());

                    /// The time range of the samples in the row: it's used to skip rows when reading a time range.
                    if (auto * min_time_decl = add_column_if_missing(TimeSeriesColumnNames::MinTime, makeSimpleAggregate("min", resolved_types.timestamp_type)))
                        min_time_decl->setCodec(getDefaultTimestampCodec());
                    if (auto * max_time_decl = add_column_if_missing(TimeSeriesColumnNames::MaxTime, makeSimpleAggregate("max", resolved_types.timestamp_type)))
                        max_time_decl->setCodec(getDefaultTimestampCodec());
                    break;
                }

                /// Generated `timestamp` and `value` columns get the generated codecs, explicitly declared columns keep the user's codecs.
                if (auto * timestamp_decl = add_column_if_missing(TimeSeriesColumnNames::Timestamp, dataTypeToAST(resolved_types.timestamp_type)))
                    timestamp_decl->setCodec(getDefaultTimestampCodec());
                if (auto * value_decl = add_column_if_missing(TimeSeriesColumnNames::Value, dataTypeToAST(resolved_types.value_type)))
                    value_decl->setCodec(getDefaultValueCodec());

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

                /// Columns "min_time" and "max_time" (tables of earlier versions kept them in the tags table).
                if (time_series_settings.hasMinTimeAndMaxTimeInTagsTable())
                {
                    DataTypePtr min_max_time_type = makeNullable(resolved_types.timestamp_type);
                    if (time_series_settings[TimeSeriesSetting::aggregate_min_time_and_max_time])
                    {
                        /// When aggregation is enabled the columns need a custom SimpleAggregateFunction type.
                        add_column_if_missing(TimeSeriesColumnNames::MinTime, makeSimpleAggregate("min", min_max_time_type));
                        add_column_if_missing(TimeSeriesColumnNames::MaxTime, makeSimpleAggregate("max", min_max_time_type));
                    }
                    else
                    {
                        add_column_if_missing(TimeSeriesColumnNames::MinTime, dataTypeToAST(min_max_time_type));
                        add_column_if_missing(TimeSeriesColumnNames::MaxTime, dataTypeToAST(min_max_time_type));
                    }
                }

                break;
            }

            case ViewTarget::TimeRanges:
            {
                /// Column "id" - no DEFAULT in the time ranges table, the identifier is computed in the "tags" inner table.
                add_column_if_missing(TimeSeriesColumnNames::ID, dataTypeToAST(resolved_types.id_type));

                /// Columns "min_time" and "max_time" are always aggregated: the time range of a time series grows with every insert.
                add_column_if_missing(TimeSeriesColumnNames::MinTime, makeSimpleAggregate("min", resolved_types.timestamp_type));
                add_column_if_missing(TimeSeriesColumnNames::MaxTime, makeSimpleAggregate("max", resolved_types.timestamp_type));
                break;
            }

            case ViewTarget::MetricFamilies:
            {
                /// The column with the name of a metric family is named by the version (see TimeSeriesVersion.h),
                /// a declaration under the name used by the other versions is a mistake: nothing would write to that column.
                UInt64 version = time_series_settings[TimeSeriesSetting::version];
                const char * metric_family_column_name = TimeSeriesColumnNames::getInnerMetricFamily(version);
                const char * other_name = getInnerMetricFamilyOfOtherVersions(version);
                if (original.contains(other_name))
                    throw Exception(ErrorCodes::INCORRECT_QUERY,
                        "{}: Column {} of the inner metric families table must be named {} in TimeSeries tables of version {}",
                        table_id.getNameForLogs(), other_name, metric_family_column_name, version);

                add_column_if_missing(metric_family_column_name, makeASTDataType("String"));
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
        setTimeSeriesVersion(create_query, 0);
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
        DataTypePtr value_type = type_from_outer(TimeSeriesColumnNames::Value);
        DataTypePtr id_type = type_from_outer(TimeSeriesColumnNames::ID);
        chassert(timestamp_type || value_type || id_type);
        if (!timestamp_type)
            timestamp_type = std::make_shared<DataTypeDateTime64>(3);
        if (!value_type)
            value_type = std::make_shared<DataTypeFloat64>();
        if (!id_type)
            id_type = std::make_shared<DataTypeUUID>();

        for (auto inner_table_kind : getTargetKinds())
        {
            /// Prealpha tables predate the optional targets (the recent samples and the time ranges tables), so there is
            /// nothing to convert for them, and no such target should be added to an old table's definition.
            if ((inner_table_kind == ViewTarget::RecentSamples) || (inner_table_kind == ViewTarget::TimeRanges))
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
                    add_column(TimeSeriesColumnNames::Value, dataTypeToAST(value_type));
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

                    /// Columns "min_time" and "max_time" (prealpha tables are of version 0, so they keep them in the tags table).
                    if (time_series_settings[TimeSeriesSetting::store_min_time_and_max_time])
                    {
                        DataTypePtr min_max_time_type = makeNullable(timestamp_type);
                        if (time_series_settings[TimeSeriesSetting::aggregate_min_time_and_max_time])
                        {
                            /// When aggregation is enabled the columns need a custom SimpleAggregateFunction type.
                            add_column(TimeSeriesColumnNames::MinTime, makeSimpleAggregate("min", min_max_time_type));
                            add_column(TimeSeriesColumnNames::MaxTime, makeSimpleAggregate("max", min_max_time_type));
                        }
                        else
                        {
                            add_column(TimeSeriesColumnNames::MinTime, dataTypeToAST(min_max_time_type));
                            add_column(TimeSeriesColumnNames::MaxTime, dataTypeToAST(min_max_time_type));
                        }
                    }

                    break;
                }

                case ViewTarget::MetricFamilies:
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
            std::make_shared<DataTypeTuple>(DataTypes{timestamp_type, value_type}))));

        auto new_outer_list = make_intrusive<ASTExpressionList>();
        new_outer_list->children.push_back(std::move(time_series_decl));

        auto new_outer_columns = make_intrusive<ASTColumns>();
        new_outer_columns->set(new_outer_columns->columns, new_outer_list);
        create_query.set(create_query.columns_list, new_outer_columns);
    }

    /// Whether the create query was made by a version before the recent samples table existed,
    /// i.e. it doesn't record the `recent_samples_ttl_seconds` setting in its SETTINGS clause.
    bool isVersionWithoutExplicitRecentSamplesTTL(const ASTCreateQuery & create_query)
    {
        return create_query.storage && !hasExplicitTimeSeriesRecentSamplesTTL(create_query);
    }

    /// Converts a create query made by a version before the `recent_samples_ttl_seconds` setting existed:
    /// records the setting explicitly in the query's SETTINGS clause, so that its value always matches the table.
    void convertDefinitionWithoutRecentSamplesTTL(ASTCreateQuery & create_query)
    {
        /// A table without any mention of the recent samples target was created before the target existed, so the TTL
        /// is pinned to zero. A query mentioning the target gets the default TTL: an old-format ON CLUSTER DDL entry
        /// ships the raw query where the initiator only set the inner UUID, or the query is a hand-written ATTACH.
        bool initiator_supports_recent_samples
            = hasInnerColumns(create_query, ViewTarget::RecentSamples) || hasInnerEngine(create_query, ViewTarget::RecentSamples)
            || hasTargetTableID(create_query, ViewTarget::RecentSamples) || hasInnerUUID(create_query, ViewTarget::RecentSamples);
        UInt64 ttl_to_pin = initiator_supports_recent_samples ? static_cast<UInt64>(TimeSeriesSettings{}[TimeSeriesSetting::recent_samples_ttl_seconds]) : 0;
        setEngineSettings(*create_query.storage, "recent_samples_ttl_seconds", Field(ttl_to_pin));
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
        const Settings & query_settings)
    {
        bool changed = false;

        auto set_engine = [&](std::string_view engine_kind)
        {
            DefaultTableEngine inner_engine_family = resolved_types.inner_engine_family
                ? *resolved_types.inner_engine_family
                : getDefaultInnerEngineFamily(query_settings, table_id);
            auto engine = makeASTFunction(fmt::format("{}{}", getInnerEngineFamilyPrefix(inner_engine_family, inner_table_kind), engine_kind));
            /// Formats as `MergeTree` instead of `MergeTree()`, the same way the parser reads an engine without arguments.
            engine->setNoEmptyArgs(true);
            inner_engine.set(inner_engine.engine, engine);
            changed = true;
        };

        auto is_merge_tree = [&] { return inner_engine.engine->name.ends_with("MergeTree"); };

        /// Throws if the engine is not a MergeTree-family engine; `what` names the feature which needs one.
        auto require_merge_tree = [&](std::string_view what)
        {
            if (!is_merge_tree())
                throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
                    "{}: {} requires a MergeTree-family engine of the inner {} table", table_id.getNameForLogs(), what, inner_table_kind);
        };

        /// A declared MergeTree engine without keys gets the same keys as a generated one.
        /// A dropped sorting key is only regenerated while no explicit primary key survives, so the three
        /// branches above refuse to drop one when a primary key is present. Otherwise a copied table would
        /// reach MergeTree with neither, and be rejected with "ORDER BY cannot be empty". The tags branch
        /// drops the pair together, so it is not affected.
        auto needs_sorting_key = [&] { return is_merge_tree() && !inner_engine.order_by && !inner_engine.primary_key; };

        /// A key of one column is written without a tuple, e.g. `ORDER BY metric_family`.
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

        /// The `*_index_granularity` and `*_index_granularity_bytes` settings set the same-named settings of the inner MergeTree tables,
        /// overriding the engine declaration. `value` is the value of the setting, or the default of the version if the setting isn't set.
        auto set_merge_tree_setting = [&](std::string_view name, const SettingFieldUInt64 & setting, UInt64 value)
        {
            if (is_merge_tree() && (setting.isChanged() || !has_engine_setting(name)))
                set_engine_setting(name, value);
        };

        /// A missing engine is generated; a declared engine is kept and gets the generated keys and settings below.
        if (!inner_engine.engine)
            set_engine(getDefaultInnerEngineKind(settings, inner_table_kind));

        switch (inner_table_kind)
        {
            case ViewTarget::Samples:
            case ViewTarget::RecentSamples:
            {
                /// The recent samples table gets the same generated engine as the samples table; it becomes
                /// partitioned and TTL'd below.
                const bool bucketed = settings.hasBucketedSamples();

                /// The rows of the same series and bucket are merged by `timeSeriesGroupArray`, which needs an aggregating engine:
                /// `AggregatingMergeTree`, `ReplicatedAggregatingMergeTree` or `SharedAggregatingMergeTree`.
                if (bucketed && !inner_engine.engine->name.ends_with("AggregatingMergeTree"))
                    throw Exception(ErrorCodes::INCORRECT_QUERY,
                        "{}: The inner {} table of a TimeSeries table of version {} requires an engine of the AggregatingMergeTree family "
                        "to merge the rows of the same time series and bucket, got {}",
                        table_id.getNameForLogs(), inner_table_kind, settings[TimeSeriesSetting::version].value, inner_engine.engine->name);

                if (needs_sorting_key())
                {
                    set_sorting_key({make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID),
                        make_intrusive<ASTIdentifier>(bucketed ? TimeSeriesColumnNames::Bucket : TimeSeriesColumnNames::Timestamp)});
                }

                const bool is_samples = (inner_table_kind == ViewTarget::Samples);
                const auto & index_granularity = settings[is_samples
                    ? TimeSeriesSetting::samples_index_granularity : TimeSeriesSetting::recent_samples_index_granularity];
                set_merge_tree_setting("index_granularity", index_granularity, getIndexGranularityFromSettings(settings, inner_table_kind));

                if (bucketed)
                {
                    const auto & index_granularity_bytes = settings[is_samples
                        ? TimeSeriesSetting::samples_index_granularity_bytes : TimeSeriesSetting::recent_samples_index_granularity_bytes];
                    set_merge_tree_setting("index_granularity_bytes", index_granularity_bytes, index_granularity_bytes.value);
                }

                if (is_samples)
                {
                    /// The engine is a MergeTree one here: the setting requires a version with buckets of samples (see `checkTimeSeriesSettings`).
                    if (const auto & partition_by = settings[TimeSeriesSetting::samples_partition_by].value)
                    {
                        /// An explicitly set `samples_partition_by` overrides the partition key from the engine declaration.
                        set_partition_by(partition_by->clone());
                    }
                    else if (bucketed && !has_partition_by())
                    {
                        /// Otherwise a declared partition key is kept; if there is none, the default one (one partition per month) is used.
                        set_partition_by(getDefaultSamplesPartitionKey());
                    }
                    break;
                }

                /// `recent_samples_ttl_seconds` is a correctness contract for the reader: the TTL always comes from it; non-TTL engines are rejected.
                require_merge_tree("The TTL defined by the `recent_samples_ttl_seconds` setting");

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
                    /// Otherwise a declared partition key is kept; if there is none, the default one (5-hour intervals) is used
                    /// (see getDefaultRecentSamplesPartitionKey).
                    set_partition_by(getDefaultRecentSamplesPartitionKey(settings[TimeSeriesSetting::version]));
                }

                set_ttl(getRecentSamplesTTLFromSettings(settings));
                break;
            }

            case ViewTarget::Tags:
            {
                if (needs_sorting_key())
                {
                    set_primary_key(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricName));

                    ASTs key_columns;
                    key_columns.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricName));
                    key_columns.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID));
                    if (settings.hasMinTimeAndMaxTimeInTagsSortingKey())
                    {
                        key_columns.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MinTime));
                        key_columns.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MaxTime));

                        /// These columns are nullable, so the sorting key needs `allow_nullable_key`.
                        set_engine_setting("allow_nullable_key", 1);
                    }
                    set_sorting_key(std::move(key_columns));
                }

                set_merge_tree_setting("index_granularity", settings[TimeSeriesSetting::tags_index_granularity], settings[TimeSeriesSetting::tags_index_granularity].value);

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

            case ViewTarget::TimeRanges:
            {
                if (needs_sorting_key())
                    set_sorting_key({make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID)});
                break;
            }

            case ViewTarget::MetricFamilies:
            {
                if (needs_sorting_key())
                    set_sorting_key({make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::getInnerMetricFamily(settings[TimeSeriesSetting::version]))});
                break;
            }

            default:
                UNREACHABLE();
        }

        return changed;
    }

    /// Adds the default text index for exact label lookups in the `tags` map.
    /// Explicit index declarations are kept, and engines outside the `MergeTree` family have no indexes.
    bool normalizeInnerIndices(ASTColumns & inner_columns, const ASTStorage & inner_engine, ViewTarget::Kind kind)
    {
        if (kind != ViewTarget::Tags || !inner_engine.engine->name.ends_with("MergeTree")
            || (inner_columns.indices && !inner_columns.indices->children.empty()))
            return false;

        auto indices = make_intrusive<ASTExpressionList>();
        indices->children.push_back(getDefaultTagsIndex());
        inner_columns.setOrReplace(inner_columns.indices, indices);
        return true;
    }

    /// Checks that a target table or an inner-columns list has all the columns required by the
    /// TimeSeries table engine, and that those columns match the resolved types.
    /// `is_external` is set for an external target table, then `table_id` is the name of that table.
    void checkTargetTable(
        const ColumnsDescription & target_table_columns,
        ViewTarget::Kind target_kind,
        bool is_external,
        const TimeSeriesSettings & time_series_settings,
        const ResolvedTimeSeriesTypes & resolved_types,
        const StorageID & table_id)
    {
        /// A table of version MIN_WITHOUT_EXTERNAL_TARGETS or later can have inner target tables only (see TimeSeriesVersion.h),
        /// an existing table keeps the external target tables of its version.
        if (is_external && (time_series_settings[TimeSeriesSetting::version] >= TimeSeriesVersion::MIN_WITHOUT_EXTERNAL_TARGETS))
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                "The {} target refers to the external table {}, but a table of version {} can have inner target tables only "
                "(external target tables are supported for version {} and earlier); "
                "use the INNER COLUMNS and INNER ENGINE clauses to customize the inner table instead",
                target_kind, table_id.getNameForLogs(), time_series_settings[TimeSeriesSetting::version].value,
                TimeSeriesVersion::MIN_WITHOUT_EXTERNAL_TARGETS - 1);

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

        /// Accepts the timestamp type, optionally Nullable, optionally wrapped in a SimpleAggregateFunction with the function
        /// which aggregates the column (`min` for `min_time`, `max` for `max_time`). The inserts write plain timestamps,
        /// an aggregating engine merges them with that function; other wrappers can't be filled or would merge them wrongly.
        auto check_column_min_max_time = [&](std::string_view column_name, std::string_view aggregate_function_name)
        {
            check_column(column_name);
            const auto * col = target_table_columns.tryGet(String(column_name));
            DataTypePtr type = col->type;
            if (const auto * simple_aggregate_function = typeid_cast<const DataTypeCustomSimpleAggregateFunction *>(type->getCustomName()))
            {
                /// The storage type of a `SimpleAggregateFunction` compares equal to the plain type, so the wrapper is checked by name.
                bool is_expected_wrapper = (simple_aggregate_function->getFunctionName() == aggregate_function_name)
                    && (simple_aggregate_function->getArgumentsDataTypes().size() == 1);
                type = is_expected_wrapper ? simple_aggregate_function->getArgumentsDataTypes()[0] : nullptr;
            }
            if (type && removeNullable(type)->equals(*resolved_types.timestamp_type))
                return;
            throw Exception(
                ErrorCodes::BAD_TYPE_OF_FIELD,
                "{}: Column {} in the {} table has type {}, but expected {} (optionally Nullable, optionally wrapped in SimpleAggregateFunction({}, ...))",
                table_id.getNameForLogs(),
                column_name,
                target_kind,
                col->type->getName(),
                resolved_types.timestamp_type->getName(),
                aggregate_function_name);
        };

        /// Accepts `SimpleAggregateFunction(min, <timestamp type>)` for `min_time` and `SimpleAggregateFunction(max, <timestamp type>)`
        /// for `max_time` only: the aggregating engine merges the rows of the same series and bucket, and the bounds of a merged row
        /// must cover all its samples (a plain or Nullable column would keep the bounds of one insert, other wrappers would merge them wrongly).
        auto check_column_bucket_bound = [&](std::string_view column_name, std::string_view aggregate_function_name)
        {
            check_column(column_name);
            const auto * col = target_table_columns.tryGet(String(column_name));
            if (const auto * simple_aggregate_function = typeid_cast<const DataTypeCustomSimpleAggregateFunction *>(col->type->getCustomName());
                simple_aggregate_function && (simple_aggregate_function->getFunctionName() == aggregate_function_name)
                && (simple_aggregate_function->getArgumentsDataTypes().size() == 1)
                && simple_aggregate_function->getArgumentsDataTypes()[0]->equals(*resolved_types.timestamp_type))
            {
                return;
            }
            throw Exception(
                ErrorCodes::BAD_TYPE_OF_FIELD,
                "{}: Column {} in the {} table has type {}, but expected SimpleAggregateFunction({}, {})",
                table_id.getNameForLogs(),
                column_name,
                target_kind,
                col->type->getName(),
                aggregate_function_name,
                resolved_types.timestamp_type->getName());
        };

        /// Accepts `SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(timestamp <timestamp type>, value <value type>)))` only:
        /// the inserts write sorted arrays, the aggregating engine merges the rows of the same series and bucket with that function
        /// (a plain array wouldn't be merged, other wrappers would merge it wrongly), and the codecs of the elements are declared by these names.
        auto check_column_samples = [&](std::string_view column_name)
        {
            check_column(column_name);
            const auto * col = target_table_columns.tryGet(String(column_name));
            auto [samples_timestamp_type, samples_value_type] = trySplitSamplesType(col->type, /* allow_unnamed_tuple = */ false);
            if (isTimeSeriesGroupArraySimpleAggregate(col->type) && samples_timestamp_type && samples_value_type
                && samples_timestamp_type->equals(*resolved_types.timestamp_type) && samples_value_type->equals(*resolved_types.value_type))
                return;
            throw Exception(
                ErrorCodes::BAD_TYPE_OF_FIELD,
                "{}: Column {} in the {} table has type {}, but expected SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(timestamp {}, value {})))",
                table_id.getNameForLogs(),
                column_name,
                target_kind,
                col->type->getName(),
                resolved_types.timestamp_type->getName(),
                resolved_types.value_type->getName());
        };

        /// Checks that a column of the layout of the other versions is not declared.
        auto check_column_not_exists = [&](std::string_view column_name)
        {
            if (target_table_columns.tryGet(String(column_name)))
                throw Exception(ErrorCodes::INCORRECT_QUERY,
                    "{}: Column {} must not exist in the {} table of a TimeSeries table of version {}: "
                    "the table stores buckets of samples in the columns `samples`, `bucket`, `min_time`, `max_time`",
                    table_id.getNameForLogs(), column_name, target_kind, time_series_settings[TimeSeriesSetting::version].value);
        };

        auto check_metric_family_column = [&](std::string_view column_name)
        {
            UInt64 version = time_series_settings[TimeSeriesSetting::version];
            const char * other_name = getInnerMetricFamilyOfOtherVersions(version);
            if (!target_table_columns.has(String(column_name)) && target_table_columns.has(other_name))
            {
                String other_versions = (version >= TimeSeriesVersion::MIN_WITH_METRIC_FAMILY_INNER_COLUMN)
                    ? fmt::format("before {}", TimeSeriesVersion::MIN_WITH_METRIC_FAMILY_INNER_COLUMN)
                    : fmt::format("{} and later", TimeSeriesVersion::MIN_WITH_METRIC_FAMILY_INNER_COLUMN);
                throw Exception(ErrorCodes::THERE_IS_NO_COLUMN,
                    "{}: Column {} is required for the metric families table used by TimeSeries table engine (version {}), "
                    "but the table has column {} instead, which is the name used by TimeSeries tables of versions {}. "
                    "Rename the column: ALTER TABLE {} RENAME COLUMN {} TO {}",
                    table_id.getNameForLogs(), column_name, version, other_name, other_versions,
                    table_id.getNameForLogs(), other_name, column_name);
            }
            check_column_is_string(column_name);
        };

        switch (target_kind)
        {
            case ViewTarget::Samples:
            case ViewTarget::RecentSamples:
            {
                check_column_type(TimeSeriesColumnNames::ID, resolved_types.id_type);

                if (!time_series_settings.hasBucketedSamples())
                {
                    check_column_type(TimeSeriesColumnNames::Timestamp, resolved_types.timestamp_type);
                    check_column_type(TimeSeriesColumnNames::Value, resolved_types.value_type);
                    break;
                }

                /// The columns `timestamp` and `value` belong to the layout of the earlier versions.
                check_column_not_exists(TimeSeriesColumnNames::Timestamp);
                check_column_not_exists(TimeSeriesColumnNames::Value);

                check_column_samples(TimeSeriesColumnNames::Samples);
                check_column_type(TimeSeriesColumnNames::Bucket, resolved_types.timestamp_type);
                check_column_bucket_bound(TimeSeriesColumnNames::MinTime, "min");
                check_column_bucket_bound(TimeSeriesColumnNames::MaxTime, "max");
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

                if (time_series_settings.hasMinTimeAndMaxTimeInTagsTable())
                {
                    check_column_min_max_time(TimeSeriesColumnNames::MinTime, "min");
                    check_column_min_max_time(TimeSeriesColumnNames::MaxTime, "max");
                }
                break;
            }

            case ViewTarget::TimeRanges:
            {
                check_column_type(TimeSeriesColumnNames::ID, resolved_types.id_type);
                check_column_min_max_time(TimeSeriesColumnNames::MinTime, "min");
                check_column_min_max_time(TimeSeriesColumnNames::MaxTime, "max");
                break;
            }

            case ViewTarget::MetricFamilies:
            {
                check_metric_family_column(TimeSeriesColumnNames::getInnerMetricFamily(time_series_settings[TimeSeriesSetting::version]));
                check_column_is_string(TimeSeriesColumnNames::Type);
                check_column_is_string(TimeSeriesColumnNames::Unit);
                check_column_is_string(TimeSeriesColumnNames::Help);
                break;
            }

            default:
                UNREACHABLE();
        }
    }

    /// Normalizes the definition of the table from the clause `AS <other_table>`:
    /// the stored metadata of the old table can be written by an older version.
    boost::intrusive_ptr<const ASTCreateQuery> normalizeASCreateQuery(const ASTCreateQuery & as_create_query)
    {
        /// The columns of a TimeSeries table are always generated, so the old table's columns can't be copied.
        if (!as_create_query.is_time_series_table)
        {
            StorageID old_table_id{as_create_query.getDatabase(), as_create_query.getTable()};
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                "Cannot CREATE a TimeSeries table AS {} because it is not a TimeSeries table", old_table_id.getNameForLogs());
        }

        auto normalized = boost::static_pointer_cast<ASTCreateQuery>(as_create_query.clone());
        NormalizeTimeSeriesDefinitionParams params;
        params.mode = LoadingStrictnessLevel::ATTACH;
        normalizeTimeSeriesDefinitionImpl(*normalized, params);
        return normalized;
    }

    /// Makes the description of the columns of an inner table from their declarations. Only the names and the types
    /// are needed to check the columns here, the whole declarations are checked when the inner table is created.
    ColumnsDescription toColumnsDescription(const ASTColumns & inner_table_columns, ViewTarget::Kind inner_table_kind, const StorageID & table_id)
    {
        ColumnsDescription result;
        if (!inner_table_columns.columns)
            return result;

        for (const auto & child : inner_table_columns.columns->children)
        {
            const auto & column_declaration = child->as<ASTColumnDeclaration &>();
            if (!column_declaration.getType())
                throw Exception(ErrorCodes::INCORRECT_QUERY,
                    "{}: Column {} of the inner {} table must have an explicit type",
                    table_id.getNameForLogs(), column_declaration.name, inner_table_kind);
            result.add(ColumnDescription{column_declaration.name, DataTypeFactory::instance().get(column_declaration.getType())});
        }
        return result;
    }

    /// Records the type of the `id` column in the `id_type` setting, and the expression generating identifiers
    /// in the `id_generator` setting, if the definition doesn't keep them otherwise.
    /// An external tags table keeps both outside the definition, so they're recorded to make the definition self-contained
    /// (e.g. the clause `AS <this_table>` reads the `id` type from the definition without reading the external tables)
    /// and to keep them the same if the external table changes.
    /// An inner tags table declares the `id` type in its columns, and the DEFAULT expression of its `id` column generates
    /// identifiers unless the `id_generator` setting is set: then the `id` type the expression was written for is recorded too.
    void recordIdTypeAndIdGenerator(
        ASTStorage & storage,
        TimeSeriesSettings & settings,
        const ResolvedTimeSeriesTypes & resolved_types,
        const StorageID & external_tags_table_id,
        const ColumnsDescription * external_tags_columns,
        const StorageID & table_id)
    {
        bool tags_are_external = (external_tags_columns != nullptr);
        if (!tags_are_external && !settings[TimeSeriesSetting::id_generator].value)
            return;

        if (!settings[TimeSeriesSetting::id_type].value)
        {
            settings[TimeSeriesSetting::id_type] = resolved_types.id_type;
            setEngineSettings(storage, "id_type", Field(resolved_types.id_type->getName()));
        }

        if (tags_are_external && !settings[TimeSeriesSetting::id_generator].value)
        {
            /// The expression is resolved the same way as TimeSeriesSink does it: the DEFAULT expression
            /// of the `id` column of the external tags table, then the expression chosen for the `id` type.
            ASTPtr id_generator;
            if (const auto * id_column = external_tags_columns->tryGet(TimeSeriesColumnNames::ID))
                id_generator = id_column->default_desc.expression;
            if (!id_generator)
                id_generator = TimeSeriesIDGenerator::getDefault(resolved_types.id_type, table_id);

            auto * id_generator_function = id_generator->as<ASTFunction>();
            if (!id_generator_function)
                throw Exception(ErrorCodes::INCORRECT_QUERY,
                    "{}: The DEFAULT expression {} of column {} of the external tags table {} cannot be used to generate identifiers "
                    "because it is not a function call; specify the expression in the `id_generator` setting instead",
                    table_id.getNameForLogs(), id_generator->formatWithSecretsOneLine(), TimeSeriesColumnNames::ID,
                    external_tags_table_id.getNameForLogs());

            settings[TimeSeriesSetting::id_generator] = boost::intrusive_ptr<ASTFunction>(id_generator_function);
            setEngineSettings(storage, "id_generator", Field(id_generator_function->formatWithSecretsOneLine()));
        }
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

            /// The `id_type` and `id_generator` of the old table are not copied if this table has another `id` type.
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
            /// A disabled optional target needs nothing from the old table.
            if ((kind == ViewTarget::RecentSamples) && !new_settings.isRecentSamplesTargetEnabled())
                continue;
            if ((kind == ViewTarget::TimeRanges) && !new_settings.isTimeRangesTargetEnabled())
                continue;

            if (!hasTargetTableID(create_query, kind) && !hasInnerColumns(create_query, kind))
            {
                if (auto * old_inner_columns = old_create_query.getTargetInnerColumns(kind))
                {
                    auto new_inner_columns = boost::static_pointer_cast<ASTColumns>(old_inner_columns->clone());
                    removeInnerColumnsDisabledByNewSettings(*new_inner_columns, kind, old_settings, new_settings);
                    removeGeneratedInnerColumns(*new_inner_columns, kind, old_settings);
                    removeGeneratedInnerIndices(*new_inner_columns, kind);
                    create_query.setTargetInnerColumns(kind, new_inner_columns);
                }
            }

            /// An external target of the old table (a table of version 9 or earlier could have one) has no inner engine
            /// and isn't copied: the new table gets an inner target instead, generated unless the query declares it.
            if (!hasTargetTableID(create_query, kind) && !hasInnerEngine(create_query, kind))
            {
                if (const auto * old_inner_engine = old_create_query.getTargetInnerEngine(kind))
                {
                    auto new_inner_engine = boost::static_pointer_cast<ASTStorage>(old_inner_engine->clone());
                    /// The generated clauses are recognized by the spelling of the old table first: removing the clauses of the old
                    /// layout before that could leave a generated clause unrecognized, e.g. `allow_nullable_key` without the key it was
                    /// generated for.
                    removeGeneratedInnerEngine(*new_inner_engine, kind, old_settings);
                    removeInnerEngineDisabledByNewSettings(*new_inner_engine, kind, old_settings, new_settings);
                    create_query.setTargetInnerEngine(kind, new_inner_engine);
                }
            }
        }
    }

    /// Generates the canonical outer columns from the resolved types.
    /// The name of the column with samples depends on the version of the table (see TimeSeriesVersion.h).
    ColumnsDescription generateOuterColumns(const DataTypePtr & timestamp_type, const DataTypePtr & value_type, UInt64 version)
    {
        ColumnsDescription result;

        auto add_column = [&](const String & name, DataTypePtr type)
        {
            result.add(ColumnDescription{name, std::move(type)});
        };

        add_column(TimeSeriesColumnNames::MetricName, std::make_shared<DataTypeString>());

        add_column(TimeSeriesColumnNames::Tags,
                   std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()));

        add_column(TimeSeriesColumnNames::getOuterSamples(version),
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(DataTypes{timestamp_type, value_type})));

        add_column(TimeSeriesColumnNames::MetricFamily, std::make_shared<DataTypeString>());
        add_column(TimeSeriesColumnNames::Type, std::make_shared<DataTypeString>());
        add_column(TimeSeriesColumnNames::Unit, std::make_shared<DataTypeString>());
        add_column(TimeSeriesColumnNames::Help, std::make_shared<DataTypeString>());

        return result;
    }

}


void normalizeTimeSeriesDefinitionImpl(ASTCreateQuery & create_query, const NormalizeTimeSeriesDefinitionParams & params)
{
    chassert(create_query.is_time_series_table);

    /// Whether we're creating a new table.
    bool is_new_table = params.isNewTable();

    /// Whether the create query may come from an older version, so it can be converted to the current form.
    /// The initial CREATE query is excluded: it must be written in the current form already.
    bool can_convert = (params.mode != LoadingStrictnessLevel::CREATE) || params.is_restore_from_backup;

    /// Convert the create_query if it was created before the `version` setting was introduced.
    if (can_convert && !hasExplicitTimeSeriesVersion(create_query))
    {
        convertDefinitionWithoutExplicitVersion(create_query);
        chassert(hasExplicitTimeSeriesVersion(create_query));
    }

    /// The older forms of the definition below were written only by servers which didn't support versioning yet,
    /// so they can be found only in tables of version 0.
    if (can_convert && (getTimeSeriesVersion(create_query) == 0))
    {
        /// Convert the create_query if it was created by the old versions.
        /// (A new query written in the prealpha form must be rejected, see readTypesFromOuterColumns.)
        if (isPrealpha(create_query))
        {
            convertPrealphaDefinition(create_query);
            chassert(!isPrealpha(create_query));
        }

        /// Convert the create_query if it was created before the recent samples table existed.
        if (isVersionWithoutExplicitRecentSamplesTTL(create_query))
        {
            convertDefinitionWithoutRecentSamplesTTL(create_query);
            chassert(!isVersionWithoutExplicitRecentSamplesTTL(create_query));
        }
    }

    /// Whether the query itself declares the optional targets. This is checked before applyASClause,
    /// so the flags don't count a target copied from the `AS <other_table>` clause.
    bool has_recent_samples_definition = hasTimeSeriesTargetDefinition(create_query, ViewTarget::RecentSamples);
    bool has_time_ranges_definition = hasTimeSeriesTargetDefinition(create_query, ViewTarget::TimeRanges);

    /// Pull up `DEFAULT` expressions written inside `Tuple` data types of the outer and inner columns.
    /// It has to be done before the declared types are reified below.
    pullUpTupleElementDefaultsInTimeSeriesDefinition(create_query);

    /// The definition of the table from the clause `AS <other_table>` if any, and its resolved types.
    /// The clause is used only for a new table: the stored definition of an existing table has no such clause.
    boost::intrusive_ptr<const ASTCreateQuery> old_create_query;
    std::optional<ResolvedTimeSeriesTypes> old_types;
    if (is_new_table && !create_query.as_table.empty())
    {
        if (!params.as_create_query)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "The definition of table {} from the clause AS is required to normalize the definition of a new TimeSeries table",
                StorageID{create_query.as_database, create_query.as_table}.getNameForLogs());

        old_create_query = normalizeASCreateQuery(*params.as_create_query);

        /// The types of the old table are in its outer columns, inner columns and the `id_type` setting,
        /// so its external target tables are not read. (A table with an external tags table created before the `id_type`
        /// setting existed doesn't declare its `id` type anywhere in its definition, then the default type is used.)
        old_types = resolveTimeSeriesTypes(
            *old_create_query,
            /* external_target_columns = */ {},
            /* need_inner_engine_family = */ true,
            /* fallback_types = */ nullptr);
    }

    /// Resolve types timestamp_type, value_type, id_type.
    /// The columns of the external target tables are passed for a new table only: on ATTACH they may not be loaded yet.
    ResolvedTimeSeriesTypes resolved_types = resolveTimeSeriesTypes(
        create_query,
        params.external_target_columns,
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
        if (!params.query_settings)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The query settings are required to normalize the definition of a new TimeSeries table");

        StorageID table_id{create_query.getDatabase(), create_query.getTable()};

        /// The columns of every external target table must be passed for a new table.
        auto get_external_target_columns = [&](ViewTarget::Kind kind) -> const ColumnsDescription &
        {
            auto it = params.external_target_columns.find(kind);
            if (it == params.external_target_columns.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "{}: The columns of the external {} table {} are required to normalize the definition of a new TimeSeries table",
                    table_id.getNameForLogs(), kind, create_query.getTargetTableID(kind).getNameForLogs());
            return it->second;
        };

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

        const bool recent_samples_enabled = settings.isRecentSamplesTargetEnabled();

        /// Pin the steps of the buckets, so that the table keeps its buckets if a future version changes the defaults:
        /// the rows are read with the assumption that the buckets are aligned to the steps.
        if (settings.hasBucketedSamples() && create_query.storage)
        {
            if (!settings[TimeSeriesSetting::samples_bucket_step_seconds].isChanged())
            {
                setEngineSettings(*create_query.storage, "samples_bucket_step_seconds",
                    Field(settings[TimeSeriesSetting::samples_bucket_step_seconds].value));
            }
            if (recent_samples_enabled && !settings[TimeSeriesSetting::recent_samples_bucket_step_seconds].isChanged())
            {
                setEngineSettings(*create_query.storage, "recent_samples_bucket_step_seconds",
                    Field(settings[TimeSeriesSetting::recent_samples_bucket_step_seconds].value));
            }
        }

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

        /// The time ranges target is on by default and disabled by an explicit `store_time_ranges = 0` (tables of earlier versions have no such target).
        const bool time_ranges_enabled = settings.isTimeRangesTargetEnabled();

        if (!time_ranges_enabled)
        {
            if (has_time_ranges_definition)
            {
                if (settings[TimeSeriesSetting::version] < TimeSeriesVersion::MIN_WITH_TIME_RANGES_TARGET)
                    throw Exception(ErrorCodes::INCORRECT_QUERY,
                        "The TIME RANGES target requires `version` to be at least {}, but the table has version {}",
                        TimeSeriesVersion::MIN_WITH_TIME_RANGES_TARGET, settings[TimeSeriesSetting::version].value);
                throw Exception(ErrorCodes::INCORRECT_QUERY,
                    "The TIME RANGES target requires the setting `store_time_ranges` to be enabled");
            }
            /// A TIME RANGES definition inherited from the `AS <other_table>` clause is just removed when it's disabled.
            if (create_query.targets)
                create_query.targets->removeTarget(ViewTarget::TimeRanges);
        }

        for (auto kind : getTargetKinds())
        {
            /// The recent samples target is on by default and disabled by an explicit `recent_samples_ttl_seconds = 0`.
            if ((kind == ViewTarget::RecentSamples) && !recent_samples_enabled)
                continue;
            if ((kind == ViewTarget::TimeRanges) && !time_ranges_enabled)
                continue;

            if (hasTargetTableID(create_query, kind))
            {
                /// An external target table is specified - check it has all the required columns.
                checkTargetTable(get_external_target_columns(kind), kind, /* is_external = */ true, settings, resolved_types, create_query.getTargetTableID(kind));
            }
            else
            {
                /// An inner target table should be used. Normalize its column definitions and assign a table engine if not specified.
                auto inner_columns = create_query.getTargetInnerColumns(kind)
                    ? boost::static_pointer_cast<ASTColumns>(create_query.getTargetInnerColumns(kind)->clone())
                    : make_intrusive<ASTColumns>();
                if (normalizeInnerColumns(*inner_columns, kind, settings, resolved_types, table_id))
                    create_query.setTargetInnerColumns(kind, inner_columns);

                /// Validate the user-provided types of the inner columns the same way external targets are validated.
                checkTargetTable(toColumnsDescription(*inner_columns, kind, table_id), kind, /* is_external = */ false, settings, resolved_types, table_id);

                auto inner_engine = create_query.getTargetInnerEngine(kind)
                    ? boost::static_pointer_cast<ASTStorage>(create_query.getTargetInnerEngine(kind)->clone())
                    : make_intrusive<ASTStorage>();
                if (normalizeInnerEngine(*inner_engine, kind, settings, resolved_types, table_id, *params.query_settings))
                    create_query.setTargetInnerEngine(kind, inner_engine);

                if (settings[TimeSeriesSetting::version] >= TimeSeriesVersion::MIN_WITH_TAGS_TEXT_INDEX
                    && normalizeInnerIndices(*inner_columns, *inner_engine, kind))
                    create_query.setTargetInnerColumns(kind, inner_columns);
            }
        }

        /// Inner tables with different replication types would diverge between replicas.
        /// The generated inner engines follow the declared ones, but an inner engine copied from the old table can differ.
        checkInnerEnginesReplicationTypesMatch(create_query, table_id);

        /// Record `id_type` and `id_generator` in the SETTINGS clause if they aren't kept in the definition otherwise.
        /// A table pinned to an earlier version is written the way that version did it, without the settings,
        /// and a table of version MIN_WITHOUT_ID_SETTINGS or later keeps them in the inner columns only (see TimeSeriesVersion.h).
        if (create_query.storage && (settings[TimeSeriesSetting::version] >= TimeSeriesVersion::MIN_WITH_ID_TYPE_SETTING)
            && (settings[TimeSeriesSetting::version] < TimeSeriesVersion::MIN_WITHOUT_ID_SETTINGS))
        {
            bool tags_are_external = hasTargetTableID(create_query, ViewTarget::Tags);
            recordIdTypeAndIdGenerator(
                *create_query.storage, settings, resolved_types,
                tags_are_external ? create_query.getTargetTableID(ViewTarget::Tags) : StorageID::createEmpty(),
                tags_are_external ? &get_external_target_columns(ViewTarget::Tags) : nullptr,
                table_id);
        }
    }

    /// Regenerate the columns of TimeSeries table from the resolved types.
    /// We can change the columns of TimeSeries table because these columns are designed to work
    /// as IO interface. They store no data, in fact the data is stored in target or inner columns.
    /// The version is pinned at this point (see above), so the columns are generated the way that version does it.
    {
        auto new_columns_ast = make_intrusive<ASTColumns>();
        new_columns_ast->set(new_columns_ast->columns,
            InterpreterCreateQuery::formatColumns(generateOuterColumns(
                resolved_types.timestamp_type, resolved_types.value_type, getTimeSeriesVersion(create_query))));
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
