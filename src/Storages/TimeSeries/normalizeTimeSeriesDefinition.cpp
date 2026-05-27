#include <Storages/TimeSeries/normalizeTimeSeriesDefinition.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeCustomSimpleAggregateFunction.h>
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
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>
#include <Storages/TimeSeries/TimeSeriesIDGenerator.h>
#include <unordered_set>


namespace DB
{

namespace TimeSeriesSetting
{
    extern const TimeSeriesSettingsBool aggregate_min_time_and_max_time;
    extern const TimeSeriesSettingsASTFunction id_generator;
    extern const TimeSeriesSettingsBool store_min_time_and_max_time;
    extern const TimeSeriesSettingsMap tags_to_columns;
    extern const TimeSeriesSettingsBool use_all_tags_column_to_generate_id;
}

namespace ErrorCodes
{
    extern const int BAD_TYPE_OF_FIELD;
    extern const int INCORRECT_QUERY;
    extern const int THERE_IS_NO_COLUMN;
    extern const int UNKNOWN_TABLE;
}


namespace
{
    constexpr std::array<ViewTarget::Kind, 3> getTargetKinds()
    {
        return {ViewTarget::Samples, ViewTarget::Tags, ViewTarget::Metrics};
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
                    fmt::format("column `{}` of the external `samples` table {}", column.name, external_table_id.getNameForLogs()),
                    "timestamp", table_id);
            else if (column.name == TimeSeriesColumnNames::Value)
                setOrCheckDataType(scalar_type, scalar_src, column.type,
                    fmt::format("column `{}` of the external `samples` table {}", column.name, external_table_id.getNameForLogs()),
                    "scalar", table_id);
            else if (column.name == TimeSeriesColumnNames::ID)
                setOrCheckDataType(id_type, id_src, column.type,
                    fmt::format("column `{}` of the external `samples` table {}", column.name, external_table_id.getNameForLogs()),
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
            auto external_table = DatabaseCatalog::instance().tryGetTable(context->tryResolveStorageID(external_table_id), context);
            if (!external_table)
                throw Exception(ErrorCodes::UNKNOWN_TABLE, "TimeSeries: Target table {} doesn't exist", external_table_id.getNameForLogs());
            return {external_table_id, external_table->getInMemoryMetadataPtr(context, false)->columns};
        };

        auto [samples_id, samples_columns] = resolve_external(ViewTarget::Samples);
        if (!samples_id.empty())
            readTypesFromExternalSamples(samples_id, samples_columns,
                                         timestamp_type, timestamp_src, scalar_type, scalar_src, id_type, id_src,
                                         table_id);

        auto [tags_id, tags_columns] = resolve_external(ViewTarget::Tags);
        if (!tags_id.empty())
            readTypesFromExternalTags(tags_id, tags_columns, id_type, id_src, table_id);
    }

    /// Resolved column types needed during normalization.
    struct ResolvedTimeSeriesTypes
    {
        DataTypePtr timestamp_type;
        DataTypePtr scalar_type;
        DataTypePtr id_type;
    };

    /// Resolves types `timestamp_type`, `scalar_type`, `id_type`; sets by defaults the types
    /// which are not set explicitly.
    /// `check_external_targets` is set when external target tables are expected to exist (CREATE time);
    /// on ATTACH they are allowed not to be loaded yet.
    ResolvedTimeSeriesTypes resolveTimeSeriesTypes(
        const ASTCreateQuery & create_query,
        const ContextPtr & context,
        bool check_external_targets)
    {
        StorageID table_id{create_query.getDatabase(), create_query.getTable()};

        DataTypePtr timestamp_type;
        DataTypePtr scalar_type;
        DataTypePtr id_type;
        String timestamp_src;
        String scalar_src;
        String id_src;

        readTypesFromOuterColumns(create_query,
            timestamp_type, timestamp_src, scalar_type, scalar_src, table_id);

        readTypesFromInnerSamples(create_query,
            timestamp_type, timestamp_src, scalar_type, scalar_src, id_type, id_src, table_id);

        readTypesFromInnerTags(create_query,
            id_type, id_src, table_id);

        if (check_external_targets)
        {
            readTypesFromExternalTargets(create_query, context,
                timestamp_type, timestamp_src,
                scalar_type, scalar_src,
                id_type, id_src,
                table_id);
        }

        /// Apply defaults for unset types.
        if (!timestamp_type)
            timestamp_type = std::make_shared<DataTypeDateTime64>(3);
        if (!scalar_type)
            scalar_type = std::make_shared<DataTypeFloat64>();
        if (!id_type)
            id_type = std::make_shared<DataTypeUUID>();

        /// Validate types.
        {
            WhichDataType ts_which{*timestamp_type};
            if (!(ts_which.isDateTime64() || ts_which.isDateTime() || ts_which.isUInt32()))
                throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "{}: Unexpected type {} of the {} column",
                    table_id.getNameForLogs(), timestamp_type->getName(), TimeSeriesColumnNames::Timestamp);
        }
        {
            WhichDataType sc_which{*scalar_type};
            if (!(sc_which.isFloat64() || sc_which.isFloat32()))
                throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "{}: Unexpected type {} of the {} column",
                    table_id.getNameForLogs(), scalar_type->getName(), TimeSeriesColumnNames::Value);
        }
        {
            WhichDataType id_which{*id_type};
            bool id_ok = id_which.isUInt64()
                || (id_which.isFixedString() && typeid_cast<const DataTypeFixedString &>(*id_type).getN() == 16)
                || id_which.isUUID()
                || id_which.isUInt128();
            if (!id_ok)
                throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "{}: Unexpected type {} of the {} column",
                    table_id.getNameForLogs(), id_type->getName(), TimeSeriesColumnNames::ID);
        }

        return ResolvedTimeSeriesTypes{
            .timestamp_type = std::move(timestamp_type),
            .scalar_type = std::move(scalar_type),
            .id_type = std::move(id_type),
        };
    }

    /// Adds missing required columns to an inner table's column list, building them in canonical order.
    /// Existing columns are taken from `inner_table_columns`; missing columns are created with the given type.
    /// Returns true if the column list was modified.
    bool normalizeInnerTableColumns(
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

        /// If `name` exists in `original`, move it to new_list (erasing from map) and return false.
        /// Otherwise create a new column with `type_ast`, mark `changed`, and return true.
        auto add_column_if_missing = [&](const String & name, ASTPtr type_ast) -> bool
        {
            if (auto it = original.find(name); it != original.end())
            {
                new_list->children.push_back(it->second);
                original.erase(it);
                return false;
            }
            auto decl = make_intrusive<ASTColumnDeclaration>();
            decl->name = name;
            decl->setType(std::move(type_ast));
            new_list->children.push_back(decl);
            changed = true;
            return true;
        };

        switch (inner_table_kind)
        {
            case ViewTarget::Samples:
            {
                /// Column "id" - no DEFAULT in the samples table: the identifier is computed in the "tags"
                /// inner table because it depends on columns like "metric_name" or "all_tags" which don't
                /// exist in samples.
                add_column_if_missing(TimeSeriesColumnNames::ID, dataTypeToAST(resolved_types.id_type));
                add_column_if_missing(TimeSeriesColumnNames::Timestamp, dataTypeToAST(resolved_types.timestamp_type));
                add_column_if_missing(TimeSeriesColumnNames::Value, dataTypeToAST(resolved_types.scalar_type));

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
                        new_decl.setDefaultExpression(TimeSeriesIDGenerator::getDefault(resolved_types.id_type, time_series_settings, table_id));
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

                /// Column "all_tags" is ephemeral - only used to calculate the "id" column.
                if (time_series_settings[TimeSeriesSetting::use_all_tags_column_to_generate_id])
                {
                    if (add_column_if_missing(TimeSeriesColumnNames::AllTags,
                        makeASTDataType("Map", makeASTDataType("String"), makeASTDataType("String"))))
                    {
                        auto & column = new_list->children.back();
                        column = column->clone();
                        auto & new_decl = column->as<ASTColumnDeclaration &>();
                        new_decl.default_specifier = ColumnDefaultSpecifier::Ephemeral;
                        new_decl.ephemeral_default = true;
                        changed = true;
                    }
                }

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

    /// Migrates a prealpha CREATE query: generates `INNER COLUMNS` for inner targets and
    /// replaces outer columns with a single `time_series` column carrying the resolved types.
    /// Function normalizeTimeSeriesDefinition() will rebuild the full list of the outer columns afterwards.
    void upgradeFromPrealpha(ASTCreateQuery & create_query, const TimeSeriesSettings & time_series_settings)
    {
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
            if (create_query.hasTargetTableID(inner_table_kind))
                continue;
            if (create_query.getTargetInnerColumns(inner_table_kind))
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
                    add_column(TimeSeriesColumnNames::ID, dataTypeToAST(id_type));

                    if (!time_series_settings[TimeSeriesSetting::id_generator].value)
                    {
                        auto & new_decl = new_list->children.back()->as<ASTColumnDeclaration &>();
                        new_decl.default_specifier = ColumnDefaultSpecifier::Default;
                        new_decl.ephemeral_default = false;
                        new_decl.setDefaultExpression(TimeSeriesIDGenerator::getDefault(id_type, time_series_settings, table_id));
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

                    /// Column "all_tags" is ephemeral - only used to calculate the "id" column.
                    if (time_series_settings[TimeSeriesSetting::use_all_tags_column_to_generate_id])
                    {
                        add_column(TimeSeriesColumnNames::AllTags,
                            makeASTDataType("Map", makeASTDataType("String"), makeASTDataType("String")));

                        {
                            auto & new_decl = new_list->children.back()->as<ASTColumnDeclaration &>();
                            new_decl.default_specifier = ColumnDefaultSpecifier::Ephemeral;
                            new_decl.ephemeral_default = true;
                        }
                    }

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


    /// Makes the definition of the default engine for an inner table.
    boost::intrusive_ptr<ASTStorage> generateInnerEngine(ViewTarget::Kind target_kind, const TimeSeriesSettings & settings)
    {
        auto storage = make_intrusive<ASTStorage>();

        if (target_kind == ViewTarget::Samples)
        {
            auto engine = makeASTFunction("MergeTree");
            engine->setNoEmptyArgs(false);
            storage->set(storage->engine, engine);
            storage->set(storage->order_by,
                makeASTOperator("tuple",
                    make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID),
                    make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Timestamp)));
        }
        else if (target_kind == ViewTarget::Tags)
        {
            std::string_view engine_name = settings[TimeSeriesSetting::aggregate_min_time_and_max_time]
                ? "AggregatingMergeTree"
                : "ReplacingMergeTree";
            auto engine = makeASTFunction(engine_name);
            engine->setNoEmptyArgs(false);
            storage->set(storage->engine, engine);

            storage->set(storage->primary_key, make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricName));

            ASTs order_by_list;
            order_by_list.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricName));
            order_by_list.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID));
            if (settings[TimeSeriesSetting::store_min_time_and_max_time] && !settings[TimeSeriesSetting::aggregate_min_time_and_max_time])
            {
                order_by_list.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MinTime));
                order_by_list.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MaxTime));
            }
            auto order_by_tuple = make_intrusive<ASTFunction>();
            order_by_tuple->name = "tuple";
            auto arguments_list = make_intrusive<ASTExpressionList>();
            arguments_list->children = std::move(order_by_list);
            order_by_tuple->arguments = arguments_list;
            storage->set(storage->order_by, order_by_tuple);
        }
        else if (target_kind == ViewTarget::Metrics)
        {
            auto engine = makeASTFunction("ReplacingMergeTree");
            engine->setNoEmptyArgs(false);
            storage->set(storage->engine, engine);
            storage->set(storage->order_by, make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricFamilyName));
        }

        return storage;
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

    /// If `create_query` has clause `AS <other_table>`,
    /// the function reads the CREATE query of the <other_table> and applies outer columns, inner columns, inner engines,
    /// and the `SETTINGS` clause to the current `create_query`.
    void applyASClause(ASTCreateQuery & create_query, const ContextPtr & context)
    {
        chassert (!create_query.as_table.empty());
        auto other_database = context->resolveDatabase(create_query.as_database);
        auto as_create_query = boost::static_pointer_cast<const ASTCreateQuery>(
            DatabaseCatalog::instance().getDatabase(other_database)->getCreateTableQuery(create_query.as_table, context));

        /// Copy settings from the other table.
        if (as_create_query->storage && as_create_query->storage->settings
            && (!create_query.storage || !create_query.storage->settings))
        {
            if (!create_query.storage)
                create_query.set(create_query.storage, make_intrusive<ASTStorage>());
            create_query.storage->set(create_query.storage->settings, as_create_query->storage->settings->clone());
        }

        /// Copy outer column from the other table.
        if (!create_query.columns_list && as_create_query->columns_list)
        {
            create_query.set(create_query.columns_list,
                boost::static_pointer_cast<ASTColumns>(as_create_query->columns_list->clone()));
        }

        /// Copy inner columns and inner engines from the other table.
        for (auto kind : getTargetKinds())
        {
            if (!create_query.getTargetInnerColumns(kind))
            {
                if (auto * as_inner_cols = as_create_query->getTargetInnerColumns(kind))
                    create_query.setTargetInnerColumns(kind, boost::static_pointer_cast<ASTColumns>(as_inner_cols->clone()));
            }

            if (!create_query.hasTargetTableID(kind) && !create_query.getTargetInnerEngine(kind))
            {
                if (as_create_query->hasTargetTableID(kind))
                {
                    StorageID other_table_id{as_create_query->getDatabase(), as_create_query->getTable()};
                    throw Exception(ErrorCodes::INCORRECT_QUERY,
                        "Cannot CREATE a table AS {} because it has external tables", other_table_id.getNameForLogs());
                }
                if (auto * other_inner_engine = as_create_query->getTargetInnerEngine(kind))
                    create_query.setTargetInnerEngine(kind, other_inner_engine->clone());
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
    /// Whether we're creating a new table.
    /// `is_new_table` is false if we're restoring from a backup.
    bool is_new_table = (mode <= LoadingStrictnessLevel::SECONDARY_CREATE) && !is_restore_from_backup;

    /// Upgrade the create_query if it was created by the old versions.
    if (!is_new_table && isPrealpha(create_query))
    {
        TimeSeriesSettings settings_for_prealpha;
        if (create_query.storage)
            settings_for_prealpha.loadFromQuery(*create_query.storage);
        upgradeFromPrealpha(create_query, settings_for_prealpha);
        chassert(!isPrealpha(create_query));
    }

    /// Apply the clause `AS <other_table>` if any.
    if (!create_query.as_table.empty())
    {
        chassert(is_new_table);
        applyASClause(create_query, context);
    }

    /// Resolve types timestamp_type, scalar_type, id_type.
    /// External targets are checked only at CREATE time; on ATTACH they may not be loaded yet.
    ResolvedTimeSeriesTypes resolved_types = resolveTimeSeriesTypes(create_query, context, /*check_external_targets=*/ is_new_table);

    /// For new tables: per-kind, check external tables or normalize the inner table's columns and assign its engine.
    if (is_new_table)
    {
        TimeSeriesSettings settings;
        if (create_query.storage)
            settings.loadFromQuery(*create_query.storage);
        checkTimeSeriesSettings(settings);

        for (auto kind : getTargetKinds())
        {
            if (create_query.hasTargetTableID(kind))
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
                if (normalizeInnerTableColumns(*inner_columns, kind, settings, resolved_types, table_id))
                    create_query.setTargetInnerColumns(kind, inner_columns);

                /// Validate the user-provided types of the inner columns the same way external targets are validated.
                auto inner_columns_description = InterpreterCreateQuery::getColumnsDescription(
                    *inner_columns->columns, context, mode);
                checkTargetTable(inner_columns_description, kind, settings, resolved_types, table_id);

                if (!create_query.getTargetInnerEngine(kind))
                    create_query.setTargetInnerEngine(kind, generateInnerEngine(kind, settings));
            }
        }
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
