#include <Storages/TimeSeries/normalizeTimeSeriesDefinition.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
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
#include <unordered_set>


namespace DB
{

namespace TimeSeriesSetting
{
    extern const TimeSeriesSettingsBool aggregate_min_time_and_max_time;
    extern const TimeSeriesSettingsASTFunction id_generator;
    extern const TimeSeriesSettingsDataType id_type;
    extern const TimeSeriesSettingsDataType scalar_type;
    extern const TimeSeriesSettingsBool store_min_time_and_max_time;
    extern const TimeSeriesSettingsMap tags_to_columns;
    extern const TimeSeriesSettingsDataType timestamp_type;
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

    /// Extracts the ID generator function from a column's default expression.
    /// Returns `nullptr` and logs a warning if the expression is not a function.
    boost::intrusive_ptr<ASTFunction> extractIDGeneratorFromDefaultExpression(ASTPtr default_expression, const StorageID & table_id)
    {
        if (!default_expression)
            return nullptr;

        if (!typeid_cast<ASTFunction *>(default_expression.get()))
        {
            /// If the expression to generate ID is not a function then something is wrong.
            LOG_WARNING(
                getLogger("TimeSeries"),
                "{}: Expression {} specified for generating ID (fingerprint) won't work because it's not a function. "
                "The expression will be replaced with the default one",
                table_id.getNameForLogs(), default_expression->formatForLogging());
            return nullptr;
        }

        return boost::static_pointer_cast<ASTFunction>(default_expression->clone());
    }

    /// Extracts types and generators from the columns if they are not specified in the settings.
    /// Returns true if any setting was changed.
    bool extractMissingSettingsFromColumns(TimeSeriesSettings & settings, const ASTCreateQuery & create_query)
    {
        if (settings[TimeSeriesSetting::timestamp_type] && settings[TimeSeriesSetting::scalar_type] && settings[TimeSeriesSetting::id_type]
            && settings[TimeSeriesSetting::id_generator])
            return false; /// Already got these settings

        if (!create_query.columns_list || !create_query.columns_list->columns)
            return false; /// Can't get these settings

        bool changed = false;
        for (const auto & column : create_query.columns_list->columns->children)
        {
            auto column_declaration = boost::static_pointer_cast<ASTColumnDeclaration>(column);
            if ((column_declaration->name == TimeSeriesColumnNames::Timestamp) && column_declaration->getType())
            {
                if (!settings[TimeSeriesSetting::timestamp_type])
                {
                    settings[TimeSeriesSetting::timestamp_type] = DataTypeFactory::instance().get(column_declaration->getType());
                    changed = true;
                }
            }
            else if ((column_declaration->name == TimeSeriesColumnNames::Value) && column_declaration->getType())
            {
                if (!settings[TimeSeriesSetting::scalar_type])
                {
                    settings[TimeSeriesSetting::scalar_type] = DataTypeFactory::instance().get(column_declaration->getType());
                    changed = true;
                }
            }
            else if ((column_declaration->name == TimeSeriesColumnNames::TimeSeries) && column_declaration->getType())
            {
                auto column_type = DataTypeFactory::instance().get(column_declaration->getType());
                const auto * array_type = typeid_cast<const DataTypeArray *>(column_type.get());
                const auto * tuple_type = array_type ? typeid_cast<const DataTypeTuple *>(array_type->getNestedType().get()) : nullptr;
                if (tuple_type && tuple_type->getElements().size() >= 2)
                {
                    const auto & tuple_elements = tuple_type->getElements();
                    if (!settings[TimeSeriesSetting::timestamp_type])
                    {
                        settings[TimeSeriesSetting::timestamp_type] = tuple_elements[0];
                        changed = true;
                    }
                    if (!settings[TimeSeriesSetting::scalar_type])
                    {
                        settings[TimeSeriesSetting::scalar_type] = tuple_elements[1];
                        changed = true;
                    }
                }
            }
            else if (column_declaration->name == TimeSeriesColumnNames::ID)
            {
                if (!settings[TimeSeriesSetting::id_type] && column_declaration->getType())
                {
                    settings[TimeSeriesSetting::id_type] = DataTypeFactory::instance().get(column_declaration->getType());
                    changed = true;
                }

                if (!settings[TimeSeriesSetting::id_generator] && column_declaration->getDefaultExpression())
                {
                    settings[TimeSeriesSetting::id_generator] = extractIDGeneratorFromDefaultExpression(
                        column_declaration->getDefaultExpression(), StorageID{create_query.getDatabase(), create_query.getTable()});
                    changed = true;
                }
            }
        }
        return changed;
    }

    /// Extracts types and generators from an external target table if they are not specified in the settings.
    /// Returns true if any setting was changed.
    bool extractMissingSettingsFromTarget(TimeSeriesSettings & settings, const ASTCreateQuery & create_query, ViewTarget::Kind kind, const ContextPtr & context)
    {
        if ((kind == ViewTarget::Samples) && settings[TimeSeriesSetting::timestamp_type] && settings[TimeSeriesSetting::scalar_type])
            return false; /// Already got these settings
        if ((kind == ViewTarget::Tags) && settings[TimeSeriesSetting::id_type] && settings[TimeSeriesSetting::id_generator])
            return false; /// Already got these settings

        if (!create_query.targets)
            return false; /// No external target table, can't get these settings

        auto target_table_id = create_query.targets->getTableID(kind);
        if (!target_table_id)
            return false; /// No external target table, can't get these settings

        auto target_table = DatabaseCatalog::instance().tryGetTable(context->tryResolveStorageID(target_table_id), context);
        if (!target_table)
        {
            /// External target table is specified and must exist.
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "TimeSeries: Target table {} doesn't exist", target_table_id.getNameForLogs());
        }

        auto metadata = target_table->getInMemoryMetadataPtr(context, false);

        bool changed = false;
        for (const auto & column : metadata->columns)
        {
            if (column.name == TimeSeriesColumnNames::Timestamp)
            {
                if (!settings[TimeSeriesSetting::timestamp_type])
                {
                    settings[TimeSeriesSetting::timestamp_type] = column.type;
                    changed = true;
                }
            }
            else if (column.name == TimeSeriesColumnNames::Value)
            {
                if (!settings[TimeSeriesSetting::scalar_type])
                {
                    settings[TimeSeriesSetting::scalar_type] = column.type;
                    changed = true;
                }
            }
            else if (column.name == TimeSeriesColumnNames::ID)
            {
                if (!settings[TimeSeriesSetting::id_type])
                {
                    settings[TimeSeriesSetting::id_type] = column.type;
                    changed = true;
                }

                /// The default expression for the "id" column is used to calculate it on insertion new time series,
                /// so we need it.
                if (!settings[TimeSeriesSetting::id_generator] && column.default_desc.expression)
                {
                    settings[TimeSeriesSetting::id_generator]
                        = extractIDGeneratorFromDefaultExpression(column.default_desc.expression, target_table_id);
                    changed = true;
                }
            }
        }
        return changed;
    }

    /// Extracts types and generators from external target tables (samples and tags) if they are not specified in the settings.
    /// Returns true if any setting was changed.
    bool extractMissingSettingsFromTargets(TimeSeriesSettings & settings, const ASTCreateQuery & create_query, const ContextPtr & context)
    {
        bool changed = false;
        changed |= extractMissingSettingsFromTarget(settings, create_query, ViewTarget::Samples, context);
        changed |= extractMissingSettingsFromTarget(settings, create_query, ViewTarget::Tags, context);
        return changed;
    }

    /// Generates a formula for calculating the identifier of a time series from the metric name and all the tags.
    boost::intrusive_ptr<ASTFunction> makeIDGenerator(const TimeSeriesSettings & settings, const ASTCreateQuery & create_query)
    {
        /// Build a list of arguments for a hash function.
        /// All hash functions below allow multiple arguments, so we use two arguments: metric_name, all_tags.
        ASTs arguments_for_hash_function;
        arguments_for_hash_function.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricName));

        if (settings[TimeSeriesSetting::use_all_tags_column_to_generate_id])
        {
            arguments_for_hash_function.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::AllTags));
        }
        else
        {
            const Map & tags_to_columns = settings[TimeSeriesSetting::tags_to_columns];
            for (const auto & tag_name_and_column_name : tags_to_columns)
            {
                const auto & tuple = tag_name_and_column_name.safeGet<Tuple>();
                const auto & column_name = tuple.at(1).safeGet<String>();
                arguments_for_hash_function.push_back(make_intrusive<ASTIdentifier>(column_name));
            }
            arguments_for_hash_function.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Tags));
        }

        auto make_hash_function = [&](const String & function_name) -> boost::intrusive_ptr<ASTFunction>
        {
            auto function = make_intrusive<ASTFunction>();
            function->name = function_name;
            auto arguments_list = make_intrusive<ASTExpressionList>();
            arguments_list->children = std::move(arguments_for_hash_function);
            function->arguments = arguments_list;
            return function;
        };

        /// The type of the hash function depends on the type of the 'id' column.
        DataTypePtr id_type = settings[TimeSeriesSetting::id_type];
        WhichDataType id_which(*id_type);

        if (id_which.isUInt64())
            return make_hash_function("sipHash64");

        if (id_which.isFixedString() && typeid_cast<const DataTypeFixedString &>(*id_type).getN() == 16)
            return make_hash_function("sipHash128");

        if (id_which.isUUID())
            return makeASTFunction("reinterpretAsUUID", make_hash_function("sipHash128"));

        if (id_which.isUInt128())
            return makeASTFunction("reinterpretAsUInt128", make_hash_function("sipHash128"));

        StorageID time_series_table_id{create_query.getDatabase(), create_query.getTable()};
        throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "{}: Unexpected type {} of the {} column", time_series_table_id.getNameForLogs(), id_type->getName(), TimeSeriesColumnNames::ID);
    }

    /// Fills in any missing required settings with their defaults.
    /// Returns true if any setting was changed.
    bool setDefaultSettings(TimeSeriesSettings & settings, const ASTCreateQuery & create_query)
    {
        bool changed = false;

        if (!settings[TimeSeriesSetting::timestamp_type])
        {
            settings[TimeSeriesSetting::timestamp_type] = std::make_shared<DataTypeDateTime64>(3);
            changed = true;
        }

        if (!settings[TimeSeriesSetting::scalar_type])
        {
            settings[TimeSeriesSetting::scalar_type] = std::make_shared<DataTypeFloat64>();
            changed = true;
        }

        if (!settings[TimeSeriesSetting::id_type])
        {
            settings[TimeSeriesSetting::id_type] = std::make_shared<DataTypeUUID>();
            changed = true;
        }

        if (!settings[TimeSeriesSetting::id_generator])
        {
            settings[TimeSeriesSetting::id_generator] = makeIDGenerator(settings, create_query);
            changed = true;
        }

        return changed;
    }

    /// Checks that the settings are correct.
    void validateSettings(const TimeSeriesSettings & settings, const ASTCreateQuery & create_query)
    {
        DataTypePtr timestamp_type = settings[TimeSeriesSetting::timestamp_type];
        WhichDataType timestamp_which{*timestamp_type};
        bool timestamp_ok = timestamp_which.isDateTime64() || timestamp_which.isDateTime() || timestamp_which.isUInt32();
        if (!timestamp_ok)
        {
            StorageID time_series_table_id{create_query.getDatabase(), create_query.getTable()};
            throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "{}: Unexpected type {} of the {} column", time_series_table_id.getNameForLogs(), timestamp_type->getName(), TimeSeriesColumnNames::Timestamp);
        }

        DataTypePtr scalar_type = settings[TimeSeriesSetting::scalar_type];
        WhichDataType scalar_which{*scalar_type};
        bool scalar_ok = scalar_which.isFloat64() || scalar_which.isFloat32();
        if (!scalar_ok)
        {
            StorageID time_series_table_id{create_query.getDatabase(), create_query.getTable()};
            throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "{}: Unexpected type {} of the {} column", time_series_table_id.getNameForLogs(), scalar_type->getName(), TimeSeriesColumnNames::Value);
        }

        DataTypePtr id_type = settings[TimeSeriesSetting::id_type];
        WhichDataType id_which{*id_type};
        bool id_ok = id_which.isUInt64() || (id_which.isFixedString() && typeid_cast<const DataTypeFixedString &>(*id_type).getN() == 16)
            || id_which.isUUID() || id_which.isUInt128();
        if (!id_ok)
        {
            StorageID time_series_table_id{create_query.getDatabase(), create_query.getTable()};
            throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "{}: Unexpected type {} of the {} column", time_series_table_id.getNameForLogs(), id_type->getName(), TimeSeriesColumnNames::ID);
        }
    }

    /// Adds missing required columns to an inner table's column list, building them in canonical order.
    /// Existing columns are taken from `inner_table_columns`; time_series_columns_map time series columns are copied when available.
    /// Returns true if the column list was modified.
    bool normalizeInnerTableColumns(
        ASTColumns & inner_table_columns,
        ViewTarget::Kind inner_table_kind,
        const ASTColumns * time_series_columns,
        const TimeSeriesSettings & time_series_settings)
    {
        /// Build a map of the existing inner columns by name.
        std::map<String, ASTPtr> original;
        if (inner_table_columns.columns)
        {
            for (auto & child : inner_table_columns.columns->children)
                original[child->as<ASTColumnDeclaration &>().name] = child;
        }

        /// Build a lookup map for the time_series_columns_map time series columns.
        std::map<String, ASTPtr> time_series_columns_map;
        if (time_series_columns && time_series_columns->columns)
        {
            for (const auto & child : time_series_columns->columns->children)
                time_series_columns_map[child->as<ASTColumnDeclaration>()->name] = child;
        }

        auto new_list = make_intrusive<ASTExpressionList>();
        bool changed = false;

        /// If `name` exists in `original`, move it to new_list (erasing from map) and return false.
        /// Otherwise copy from `time_series_columns_map` (if present) or create a new column with `type_ast`,
        /// mark `changed`, and return true.
        auto add_column_if_missing = [&](const String & name, ASTPtr type_ast) -> bool
        {
            if (auto it = original.find(name); it != original.end())
            {
                new_list->children.push_back(it->second);
                original.erase(it);
                return false;
            }
            if (auto it = time_series_columns_map.find(name); it != time_series_columns_map.end())
                new_list->children.push_back(it->second->clone());
            else
            {
                auto decl = make_intrusive<ASTColumnDeclaration>();
                decl->name = name;
                decl->setType(std::move(type_ast));
                new_list->children.push_back(decl);
            }
            changed = true;
            return true;
        };

        switch (inner_table_kind)
        {
            case ViewTarget::Samples:
            {
                /// Column "id" - no default expression in the samples table.
                /// Reset any default expression if the column was copied from the time series columns -
                /// the identifier of the samples table is computed in the "tags" inner table,
                /// because it depends on columns like "metric_name" or "all_tags" which don't exist in the samples table.
                if (add_column_if_missing(TimeSeriesColumnNames::ID, dataTypeToAST(time_series_settings[TimeSeriesSetting::id_type])))
                {
                    auto & column = new_list->children.back();
                    auto & decl = column->as<ASTColumnDeclaration &>();
                    if (decl.getDefaultExpression() || decl.ephemeral_default || decl.default_specifier != ColumnDefaultSpecifier::Empty)
                    {
                        column = column->clone();
                        auto & new_decl = column->as<ASTColumnDeclaration &>();
                        new_decl.default_specifier = ColumnDefaultSpecifier::Empty;
                        new_decl.ephemeral_default = false;
                        new_decl.resetDefaultExpression();
                        changed = true;
                    }
                }

                add_column_if_missing(TimeSeriesColumnNames::Timestamp, dataTypeToAST(time_series_settings[TimeSeriesSetting::timestamp_type]));
                add_column_if_missing(TimeSeriesColumnNames::Value, dataTypeToAST(time_series_settings[TimeSeriesSetting::scalar_type]));

                break;
            }

            case ViewTarget::Tags:
            {
                /// Column "id" - with the id_generator expression that computes the identifier from "metric_name" and tags.
                /// Set the id_generator expression that computes the identifier from "metric_name" and tags.
                if (add_column_if_missing(TimeSeriesColumnNames::ID, dataTypeToAST(time_series_settings[TimeSeriesSetting::id_type])))
                {
                    auto & column = new_list->children.back();
                    column = column->clone();
                    auto & new_decl = column->as<ASTColumnDeclaration &>();
                    new_decl.default_specifier = ColumnDefaultSpecifier::Default;
                    new_decl.ephemeral_default = false;
                    new_decl.setDefaultExpression(time_series_settings[TimeSeriesSetting::id_generator].value->clone());
                    changed = true;
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
                            DataTypePtr ts_type = makeNullable(time_series_settings[TimeSeriesSetting::timestamp_type]);
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
                            dataTypeToAST(makeNullable(time_series_settings[TimeSeriesSetting::timestamp_type])));
                        add_column_if_missing(TimeSeriesColumnNames::MaxTime,
                            dataTypeToAST(makeNullable(time_series_settings[TimeSeriesSetting::timestamp_type])));
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

    /// Generates the column list for an inner table from scratch, used for upgrading from old format
    /// where inner columns weren't stored in the query. Time series columns are copied when available.
    boost::intrusive_ptr<ASTColumns> generateInnerColumnsForOldVersion(
        ViewTarget::Kind inner_table_kind,
        const ASTColumns * time_series_columns,
        const TimeSeriesSettings & time_series_settings)
    {
        /// Build a lookup map for the time series columns.
        std::map<String, ASTPtr> time_series_columns_map;
        if (time_series_columns && time_series_columns->columns)
            for (const auto & child : time_series_columns->columns->children)
                time_series_columns_map[child->as<ASTColumnDeclaration>()->name] = child;

        auto new_list = make_intrusive<ASTExpressionList>();

        /// Copy from `time_series_columns_map` (if present) or create a new column with `type_ast`.
        auto add_column = [&](const String & name, ASTPtr type_ast)
        {
            if (auto it = time_series_columns_map.find(name); it != time_series_columns_map.end())
                new_list->children.push_back(it->second->clone());
            else
            {
                auto decl = make_intrusive<ASTColumnDeclaration>();
                decl->name = name;
                decl->setType(std::move(type_ast));
                new_list->children.push_back(decl);
            }
        };

        switch (inner_table_kind)
        {
            case ViewTarget::Samples:
            {
                /// Column "id" - no default expression in the samples table.
                /// Reset any default expression if the column was copied from the time series columns -
                /// the identifier of the samples table is computed in the "tags" inner table,
                /// because it depends on columns like "metric_name" or "all_tags" which don't exist in the samples table.
                add_column(TimeSeriesColumnNames::ID, dataTypeToAST(time_series_settings[TimeSeriesSetting::id_type]));

                {
                    auto & new_decl = new_list->children.back()->as<ASTColumnDeclaration &>();
                    new_decl.default_specifier = ColumnDefaultSpecifier::Empty;
                    new_decl.ephemeral_default = false;
                    new_decl.resetDefaultExpression();
                }

                add_column(TimeSeriesColumnNames::Timestamp, dataTypeToAST(time_series_settings[TimeSeriesSetting::timestamp_type]));
                add_column(TimeSeriesColumnNames::Value, dataTypeToAST(time_series_settings[TimeSeriesSetting::scalar_type]));

                break;
            }

            case ViewTarget::Tags:
            {
                /// Column "id" - with the id_generator expression that computes the identifier from "metric_name" and tags.
                add_column(TimeSeriesColumnNames::ID, dataTypeToAST(time_series_settings[TimeSeriesSetting::id_type]));

                {
                    auto & new_decl = new_list->children.back()->as<ASTColumnDeclaration &>();
                    new_decl.default_specifier = ColumnDefaultSpecifier::Default;
                    new_decl.ephemeral_default = false;
                    new_decl.setDefaultExpression(time_series_settings[TimeSeriesSetting::id_generator].value->clone());
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
                            DataTypePtr ts_type = makeNullable(time_series_settings[TimeSeriesSetting::timestamp_type]);
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
                            dataTypeToAST(makeNullable(time_series_settings[TimeSeriesSetting::timestamp_type])));
                        add_column(TimeSeriesColumnNames::MaxTime,
                            dataTypeToAST(makeNullable(time_series_settings[TimeSeriesSetting::timestamp_type])));
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
        return result;
    }


    /// Generates the engine definition for an inner table.
    /// Inherits it from `as_create_query` (AS <other_table>) if provided, or falls back to a default engine based on the target kind and settings.
    /// Returns `nullptr` if the target has an explicit external table ID (no inner table to generate for).
    boost::intrusive_ptr<ASTStorage> generateInnerEngine(ViewTarget::Kind target_kind, const ASTCreateQuery & create_query, const ASTCreateQuery * as_create_query, const TimeSeriesSettings & settings)
    {
        /// This function is only for inner tables (those without an explicit target table ID).
        if (create_query.hasTargetTableID(target_kind))
            return nullptr;

        /// If the engine is already specified in the query, use it as-is.
        auto * inner_target = create_query.getTargetInnerEngine(target_kind);
        if (inner_target)
            return inner_target;

        /// If the table is created AS <other_table>, try to inherit the engine from there.
        if (as_create_query)
        {
            if (as_create_query->hasTargetTableID(target_kind))
            {
                /// It's unlikely correct to use "CREATE table AS other_table" when "other_table" has external tables like this:
                /// CREATE TABLE other_table ENGINE=TimeSeries data mydata
                /// (because `table` would use the same table "mydata").
                /// Thus we just prohibit that.
                StorageID other_table_id{as_create_query->getDatabase(), as_create_query->getTable()};
                throw Exception(
                    ErrorCodes::INCORRECT_QUERY,
                    "Cannot CREATE a table AS {} because it has external tables",
                    other_table_id.getNameForLogs());
            }

            auto * other_inner_target = as_create_query->getTargetInnerEngine(target_kind);
            if (other_inner_target)
                return other_inner_target;
        }

        /// Neither the query nor the AS-source specified an engine — build a sensible default.
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

    /// Checks that an external target table has all the columns required by the TimeSeries table engine.
    void checkTargetTable(
        const StorageID & target_table_id,
        const ColumnsDescription & target_table_columns,
        ViewTarget::Kind target_kind,
        const TimeSeriesSettings & time_series_settings)
    {
        auto check_column = [&](std::string_view column_name)
        {
            if (!target_table_columns.tryGet(String(column_name)))
                throw Exception(
                    ErrorCodes::THERE_IS_NO_COLUMN,
                    "{}: Column {} is required for the {} table used by TimeSeries table engine",
                    target_table_id.getNameForLogs(),
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
                    target_table_id.getNameForLogs(),
                    column_name,
                    target_kind,
                    col->type->getName(),
                    expected_type->getName());
        };

        const auto is_string_like = [](const DataTypePtr & t)
        {
            WhichDataType w{*t};
            if (w.isString()) return true;
            if (w.isLowCardinality())
                return WhichDataType{*typeid_cast<const DataTypeLowCardinality &>(*t).getDictionaryType()}.isString();
            return false;
        };

        auto check_column_is_string_like = [&](std::string_view column_name)
        {
            check_column(column_name);
            const auto * col = target_table_columns.tryGet(String(column_name));
            if (!is_string_like(col->type))
                throw Exception(
                    ErrorCodes::BAD_TYPE_OF_FIELD,
                    "{}: Column {} in the {} table has type {}, but expected String or LowCardinality(String)",
                    target_table_id.getNameForLogs(),
                    column_name,
                    target_kind,
                    col->type->getName());
        };

        auto check_column_is_string_map = [&](std::string_view column_name)
        {
            check_column(column_name);
            const auto * col = target_table_columns.tryGet(String(column_name));
            WhichDataType which{*col->type};
            bool ok = false;
            if (which.isMap())
            {
                const auto & map_type = typeid_cast<const DataTypeMap &>(*col->type);
                ok = is_string_like(map_type.getKeyType()) && is_string_like(map_type.getValueType());
            }
            if (!ok)
                throw Exception(
                    ErrorCodes::BAD_TYPE_OF_FIELD,
                    "{}: Column {} in the {} table has type {}, but expected Map with String or LowCardinality(String) keys and values",
                    target_table_id.getNameForLogs(),
                    column_name,
                    target_kind,
                    col->type->getName());
        };

        switch (target_kind)
        {
            case ViewTarget::Samples:
            {
                check_column_type(TimeSeriesColumnNames::ID, time_series_settings[TimeSeriesSetting::id_type]);
                check_column_type(TimeSeriesColumnNames::Timestamp, time_series_settings[TimeSeriesSetting::timestamp_type]);
                check_column_type(TimeSeriesColumnNames::Value, time_series_settings[TimeSeriesSetting::scalar_type]);
                break;
            }

            case ViewTarget::Tags:
            {
                check_column_type(TimeSeriesColumnNames::ID, time_series_settings[TimeSeriesSetting::id_type]);
                check_column_is_string_like(TimeSeriesColumnNames::MetricName);

                const Map & tags_to_columns = time_series_settings[TimeSeriesSetting::tags_to_columns];
                for (const auto & tag_name_and_column_name : tags_to_columns)
                {
                    const auto & tuple = tag_name_and_column_name.safeGet<Tuple>();
                    const auto & column_name = tuple.at(1).safeGet<String>();
                    check_column_is_string_like(column_name);
                }

                check_column_is_string_map(TimeSeriesColumnNames::Tags);
                break;
            }

            case ViewTarget::Metrics:
            {
                check_column_is_string_like(TimeSeriesColumnNames::MetricFamilyName);
                check_column_is_string_like(TimeSeriesColumnNames::Type);
                check_column_is_string_like(TimeSeriesColumnNames::Unit);
                check_column_is_string_like(TimeSeriesColumnNames::Help);
                break;
            }

            default:
                UNREACHABLE();
        }
    }

    TimeSeriesSettings getNormalizedTimeSeriesSettingsImpl(
        const ASTCreateQuery & create_query, const SettingsChanges & settings_changes,
        const ASTCreateQuery * as_create_query, const ContextPtr & context, bool & changed)
    {
        TimeSeriesSettings settings;
        if (as_create_query && as_create_query->storage)
        {
            settings.loadFromQuery(*as_create_query->storage);
            changed = true;
        }
        if (create_query.storage)
            settings.loadFromQuery(*create_query.storage);
        if (!settings_changes.empty())
        {
            settings.applyChanges(settings_changes);
            changed = true;
        }
        changed |= extractMissingSettingsFromColumns(settings, as_create_query ? *as_create_query : create_query);
        changed |= extractMissingSettingsFromTargets(settings, create_query, context);
        changed |= setDefaultSettings(settings, create_query);
        validateSettings(settings, create_query);
        return settings;
    }
}


TimeSeriesSettings getNormalizedTimeSeriesSettings(
    const ASTCreateQuery & create_query, const ContextPtr & context, const SettingsChanges & settings_changes)
{
    boost::intrusive_ptr<const ASTCreateQuery> as_create_query;
    if (!create_query.as_table.empty())
    {
        auto other_database = context->resolveDatabase(create_query.as_database);
        as_create_query = boost::static_pointer_cast<const ASTCreateQuery>(
            DatabaseCatalog::instance().getDatabase(other_database)->getCreateTableQuery(create_query.as_table, context));
    }
    bool changed = false;
    return getNormalizedTimeSeriesSettingsImpl(create_query, settings_changes, as_create_query.get(), context, changed);
}


/// Generates the canonical column list for the TimeSeries table from the given settings.
ColumnsDescription generateTimeSeriesColumns(const TimeSeriesSettings & normalized_settings)
{
    ColumnsDescription result;

    auto add_column = [&](const String & name, DataTypePtr type)
    {
        result.add(ColumnDescription{name, std::move(type)});
    };

    add_column(TimeSeriesColumnNames::ID, normalized_settings[TimeSeriesSetting::id_type]);

    add_column(TimeSeriesColumnNames::TimeSeries,
        std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(
            DataTypes{normalized_settings[TimeSeriesSetting::timestamp_type], normalized_settings[TimeSeriesSetting::scalar_type]})));

    add_column(TimeSeriesColumnNames::MetricName, std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()));

    const Map & tags_to_columns = normalized_settings[TimeSeriesSetting::tags_to_columns];
    for (const auto & tag_name_and_column_name : tags_to_columns)
    {
        const auto & tuple = tag_name_and_column_name.safeGet<Tuple>();
        const auto & column_name = tuple.at(1).safeGet<String>();
        add_column(column_name, std::make_shared<DataTypeString>());
    }

    /// We use 'Map(LowCardinality(String), String)' as the default type of the `tags` column:
    /// it looks like a correct optimization because there are shouldn't be too many different tag names.
    add_column(TimeSeriesColumnNames::Tags,
        std::make_shared<DataTypeMap>(std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), std::make_shared<DataTypeString>()));

    /// The `all_tags` column is virtual (it's calculated on the fly and never stored anywhere)
    /// so here we don't need to use the LowCardinality optimization as for the `tags` column.
    add_column(TimeSeriesColumnNames::AllTags,
        std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()));

    if (normalized_settings[TimeSeriesSetting::store_min_time_and_max_time])
    {
        /// We use Nullable(DateTime64(3)) as the default type of the `min_time` and `max_time` columns.
        /// It's nullable because it allows the aggregation (see aggregate_min_time_and_max_time) work correctly even
        /// for rows in the "tags" table which doesn't have `min_time` and `max_time` (because they have no matching rows in the "samples" table).
        add_column(TimeSeriesColumnNames::MinTime, makeNullable(normalized_settings[TimeSeriesSetting::timestamp_type]));
        add_column(TimeSeriesColumnNames::MaxTime, makeNullable(normalized_settings[TimeSeriesSetting::timestamp_type]));
    }

    add_column(TimeSeriesColumnNames::MetricFamilyName, std::make_shared<DataTypeString>());
    add_column(TimeSeriesColumnNames::Type, std::make_shared<DataTypeString>());
    add_column(TimeSeriesColumnNames::Unit, std::make_shared<DataTypeString>());
    add_column(TimeSeriesColumnNames::Help, std::make_shared<DataTypeString>());

    return result;
}


bool normalizeTimeSeriesDefinition(ASTCreateQuery & create_query, const ContextPtr & context, LoadingStrictnessLevel mode, bool is_restore_from_backup)
{
    /// Whether we're creating a new table.
    /// `is_new_table` is false if we're restoring from a backup.
    bool is_new_table = (mode <= LoadingStrictnessLevel::SECONDARY_CREATE) && !is_restore_from_backup;

    /// Resolve AS <other_table> once, so we don't repeat the lookup in multiple places.
    boost::intrusive_ptr<const ASTCreateQuery> as_create_query;
    if (!create_query.as_table.empty())
    {
        auto other_database = context->resolveDatabase(create_query.as_database);
        as_create_query = boost::static_pointer_cast<const ASTCreateQuery>(
            DatabaseCatalog::instance().getDatabase(other_database)->getCreateTableQuery(create_query.as_table, context));
    }

    bool changed = false;

    /// Load and normalize settings from the query.
    TimeSeriesSettings settings
        = getNormalizedTimeSeriesSettingsImpl(create_query, /* settings_changes = */ {}, as_create_query.get(), context, changed);

    if (changed)
    {
        if (!create_query.storage)
            create_query.set(create_query.storage, make_intrusive<ASTStorage>());
        settings.copyToQuery(*create_query.storage);
    }

    /// For each target kind: check external tables or normalize inner table definitions.
    for (auto kind : getTargetKinds())
    {
        if (create_query.hasTargetTableID(kind))
        {
            /// An external target table is specified.
            /// If it's a new table, check that the specified target table has all the required columns.
            if (is_new_table)
            {
                auto target_table_id = create_query.getTargetTableID(kind);
                auto target_table = DatabaseCatalog::instance().getTable(target_table_id, context);
                auto target_metadata = target_table->getInMemoryMetadataPtr(context, false);
                checkTargetTable(target_table_id, target_metadata->columns, kind, settings);
            }
        }
        else
        {
            /// An inner target table should be used.
            /// Normalize its column definitions and assign a table engine if not specified.
            boost::intrusive_ptr<ASTColumns> inner_columns;
            bool inner_columns_changed = false;
            if (create_query.getTargetInnerColumns(kind))
                inner_columns = boost::static_pointer_cast<ASTColumns>(create_query.getTargetInnerColumns(kind)->clone());

            if (is_new_table)
            {
                if (!inner_columns)
                    inner_columns = make_intrusive<ASTColumns>();
                inner_columns_changed |= normalizeInnerTableColumns(*inner_columns, kind, create_query.columns_list, settings);
            }
            else if (!inner_columns)
            {
                /// Older versions didn't store inner table column definitions in the `CREATE` query, so reconstruct them now.
                inner_columns = generateInnerColumnsForOldVersion(kind, create_query.columns_list, settings);
                inner_columns_changed = true;
            }

            if (inner_columns_changed)
            {
                create_query.setTargetInnerColumns(kind, inner_columns);
                changed = true;
            }

            if (!create_query.getTargetInnerEngine(kind))
            {
                create_query.setTargetInnerEngine(kind, generateInnerEngine(kind, create_query, as_create_query.get(), settings));
                changed = true;
            }
        }
    }

    /// Regenerate the columns of TimeSeries table from the current settings.
    /// We can change the columns of TimeSeries table because these columns are designed to work
    /// as IO interface. They store no data, in fact the data is stored in target or inner columns.
    {
        auto new_columns_ast = make_intrusive<ASTColumns>();
        new_columns_ast->set(new_columns_ast->columns, InterpreterCreateQuery::formatColumns(generateTimeSeriesColumns(settings)));
        const auto * old_columns = create_query.columns_list;
        if (!old_columns
            || !old_columns->columns
            || old_columns->formatWithSecretsOneLine() != new_columns_ast->formatWithSecretsOneLine())
        {
            create_query.set(create_query.columns_list, new_columns_ast);
            changed = true;
        }
    }

    return changed;
}

}
