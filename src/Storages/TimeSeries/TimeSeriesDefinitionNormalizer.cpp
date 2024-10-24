#include <Storages/TimeSeries/TimeSeriesDefinitionNormalizer.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFixedString.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>


namespace DB
{

namespace TimeSeriesSetting
{
    extern const TimeSeriesSettingsBool aggregate_min_time_and_max_time;
    extern const TimeSeriesSettingsBool store_min_time_and_max_time;
    extern const TimeSeriesSettingsMap tags_to_columns;
    extern const TimeSeriesSettingsBool use_all_tags_column_to_generate_id;
}

namespace ErrorCodes
{
    extern const int INCOMPATIBLE_COLUMNS;
    extern const int INCORRECT_QUERY;
}


TimeSeriesDefinitionNormalizer::TimeSeriesDefinitionNormalizer(StorageID time_series_storage_id_,
                                                               std::reference_wrapper<const TimeSeriesSettings> time_series_settings_,
                                                               const ASTCreateQuery * as_create_query_)
    : time_series_storage_id(std::move(time_series_storage_id_))
    , time_series_settings(time_series_settings_)
    , as_create_query(as_create_query_)
{
}


void TimeSeriesDefinitionNormalizer::normalize(ASTCreateQuery & create_query) const
{
    reorderColumns(create_query);
    addMissingColumns(create_query);
    addMissingDefaultForIDColumn(create_query);

    if (as_create_query)
        addMissingInnerEnginesFromAsTable(create_query);

    addMissingInnerEngines(create_query);
}


void TimeSeriesDefinitionNormalizer::reorderColumns(ASTCreateQuery & create) const
{
    if (!create.columns_list || !create.columns_list->columns)
        return;

    auto & columns = create.columns_list->columns->children;

    /// Build a map "column_name -> column_declaration".
    std::unordered_map<std::string_view, std::shared_ptr<ASTColumnDeclaration>> columns_by_name;
    for (const auto & column : columns)
    {
        auto column_declaration = typeid_cast<std::shared_ptr<ASTColumnDeclaration>>(column);
        columns_by_name[column_declaration->name] = column_declaration;
    }

    /// Remove all columns and then add them again in the canonical order.
    columns.clear();

    auto add_column_in_correct_order = [&](std::string_view column_name)
    {
        auto it = columns_by_name.find(column_name);
        if (it != columns_by_name.end())
        {
            /// Add the column back to the list.
            columns.push_back(it->second);

            /// Remove the column from the map to allow the check at the end of this function
            /// that all columns from the original list are added back to the list.
            columns_by_name.erase(it);
        }
    };

    /// Reorder columns for the "data" table.
    add_column_in_correct_order(TimeSeriesColumnNames::ID);
    add_column_in_correct_order(TimeSeriesColumnNames::Timestamp);
    add_column_in_correct_order(TimeSeriesColumnNames::Value);

    /// Reorder columns for the "tags" table.
    add_column_in_correct_order(TimeSeriesColumnNames::MetricName);

    const Map & tags_to_columns = time_series_settings[TimeSeriesSetting::tags_to_columns];
    for (const auto & tag_name_and_column_name : tags_to_columns)
    {
        const auto & tuple = tag_name_and_column_name.safeGet<const Tuple &>();
        const auto & column_name = tuple.at(1).safeGet<String>();
        add_column_in_correct_order(column_name);
    }

    add_column_in_correct_order(TimeSeriesColumnNames::Tags);
    add_column_in_correct_order(TimeSeriesColumnNames::AllTags);

    if (time_series_settings[TimeSeriesSetting::store_min_time_and_max_time])
    {
        add_column_in_correct_order(TimeSeriesColumnNames::MinTime);
        add_column_in_correct_order(TimeSeriesColumnNames::MaxTime);
    }

    /// Reorder columns for the "metrics" table.
    add_column_in_correct_order(TimeSeriesColumnNames::MetricFamilyName);
    add_column_in_correct_order(TimeSeriesColumnNames::Type);
    add_column_in_correct_order(TimeSeriesColumnNames::Unit);
    add_column_in_correct_order(TimeSeriesColumnNames::Help);

    /// All columns from the original list must be added back to the list.
    if (!columns_by_name.empty())
    {
        throw Exception(
            ErrorCodes::INCOMPATIBLE_COLUMNS,
            "{}: Column {} can't be used in this table. "
            "The TimeSeries table engine supports only a limited set of columns (id, timestamp, value, metric_name, tags, metric_family_name, type, unit, help). "
            "Extra columns representing tags must be specified in the 'tags_to_columns' setting.",
            time_series_storage_id.getNameForLogs(), columns_by_name.begin()->first);
    }
}


void TimeSeriesDefinitionNormalizer::addMissingColumns(ASTCreateQuery & create) const
{
    if (!create.as_table.empty())
    {
        /// If the create query has the "AS other_table" clause ("CREATE TABLE table AS other_table")
        /// then all columns must be extracted from that "other_table".
        /// Function InterpreterCreateQuery::getTablePropertiesAndNormalizeCreateQuery() will do that for us,
        /// we don't need to fill missing columns by default in that case.
        return;
    }

    if (!create.columns_list)
        create.set(create.columns_list, std::make_shared<ASTColumns>());

    if (!create.columns_list->columns)
        create.columns_list->set(create.columns_list->columns, std::make_shared<ASTExpressionList>());
    auto & columns = create.columns_list->columns->children;

    /// Here in this function we rely on that the columns are already sorted in the canonical order (see the reorderColumns() function).
    /// NOTE: The order in which this function processes columns MUST be exactly the same as the order in reorderColumns().
    size_t position = 0;

    auto is_next_column_named = [&](std::string_view column_name)
    {
        if (position < columns.size() && (typeid_cast<const ASTColumnDeclaration &>(*columns[position]).name == column_name))
        {
            ++position;
            return true;
        }
        return false;
    };

    auto make_new_column = [&](const String & column_name, ASTPtr type)
    {
        auto new_column = std::make_shared<ASTColumnDeclaration>();
        new_column->name = column_name;
        new_column->type = type;
        columns.insert(columns.begin() + position, new_column);
        ++position;
    };

    auto get_uuid_type = [] { return makeASTDataType("UUID"); };
    auto get_datetime_type = [] { return makeASTDataType("DateTime64", std::make_shared<ASTLiteral>(3ul)); };
    auto get_float_type = [] { return makeASTDataType("Float64"); };
    auto get_string_type = [] { return makeASTDataType("String"); };
    auto get_lc_string_type = [&] { return makeASTDataType("LowCardinality", get_string_type()); };
    auto get_string_to_string_map_type = [&] { return makeASTDataType("Map", get_string_type(), get_string_type()); };
    auto get_lc_string_to_string_map_type = [&] { return makeASTDataType("Map", get_lc_string_type(), get_string_type()); };

    auto make_nullable = [&](std::shared_ptr<ASTDataType> type)
    {
        if (type->name == "Nullable")
            return type;
        return makeASTDataType("Nullable", type);
    };

    /// Add missing columns for the "data" table.
    if (!is_next_column_named(TimeSeriesColumnNames::ID))
        make_new_column(TimeSeriesColumnNames::ID, get_uuid_type());

    if (!is_next_column_named(TimeSeriesColumnNames::Timestamp))
        make_new_column(TimeSeriesColumnNames::Timestamp, get_datetime_type());

    auto timestamp_column = typeid_cast<std::shared_ptr<ASTColumnDeclaration>>(columns[position - 1]);
    auto timestamp_type = typeid_cast<std::shared_ptr<ASTDataType>>(timestamp_column->type->ptr());

    if (!is_next_column_named(TimeSeriesColumnNames::Value))
        make_new_column(TimeSeriesColumnNames::Value, get_float_type());

    /// Add missing columns for the "tags" table.
    if (!is_next_column_named(TimeSeriesColumnNames::MetricName))
    {
        /// We use 'LowCardinality(String)' as the default type of the `metric_name` column:
        /// it looks like a correct optimization because there are shouldn't be too many different metrics.
        make_new_column(TimeSeriesColumnNames::MetricName, get_lc_string_type());
    }

    const Map & tags_to_columns = time_series_settings[TimeSeriesSetting::tags_to_columns];
    for (const auto & tag_name_and_column_name : tags_to_columns)
    {
        const auto & tuple = tag_name_and_column_name.safeGet<const Tuple &>();
        const auto & column_name = tuple.at(1).safeGet<String>();
        if (!is_next_column_named(column_name))
            make_new_column(column_name, get_string_type());
    }

    if (!is_next_column_named(TimeSeriesColumnNames::Tags))
    {
        /// We use 'Map(LowCardinality(String), String)' as the default type of the `tags` column:
        /// it looks like a correct optimization because there are shouldn't be too many different tag names.
        make_new_column(TimeSeriesColumnNames::Tags, get_lc_string_to_string_map_type());
    }

    if (!is_next_column_named(TimeSeriesColumnNames::AllTags))
    {
        /// The `all_tags` column is virtual (it's calculated on the fly and never stored anywhere)
        /// so here we don't need to use the LowCardinality optimization as for the `tags` column.
        make_new_column(TimeSeriesColumnNames::AllTags, get_string_to_string_map_type());
    }

    if (time_series_settings[TimeSeriesSetting::store_min_time_and_max_time])
    {
        /// We use Nullable(DateTime64(3)) as the default type of the `min_time` and `max_time` columns.
        /// It's nullable because it allows the aggregation (see aggregate_min_time_and_max_time) work correctly even
        /// for rows in the "tags" table which doesn't have `min_time` and `max_time` (because they have no matching rows in the "data" table).

        if (!is_next_column_named(TimeSeriesColumnNames::MinTime))
            make_new_column(TimeSeriesColumnNames::MinTime, make_nullable(timestamp_type));
        if (!is_next_column_named(TimeSeriesColumnNames::MaxTime))
            make_new_column(TimeSeriesColumnNames::MaxTime, make_nullable(timestamp_type));
    }

    /// Add missing columns for the "metrics" table.
    if (!is_next_column_named(TimeSeriesColumnNames::MetricFamilyName))
        make_new_column(TimeSeriesColumnNames::MetricFamilyName, get_string_type());

    if (!is_next_column_named(TimeSeriesColumnNames::Type))
        make_new_column(TimeSeriesColumnNames::Type, get_string_type());

    if (!is_next_column_named(TimeSeriesColumnNames::Unit))
        make_new_column(TimeSeriesColumnNames::Unit, get_string_type());

    if (!is_next_column_named(TimeSeriesColumnNames::Help))
        make_new_column(TimeSeriesColumnNames::Help, get_string_type());

    /// If the following fails that means the order in which columns are processed in this function doesn't match the order of columns in reorderColumns().
    chassert(position == columns.size());
}


void TimeSeriesDefinitionNormalizer::addMissingDefaultForIDColumn(ASTCreateQuery & create) const
{
    /// Find the 'id' column and make a default expression for it.
    if (!create.columns_list || !create.columns_list->columns)
        return;

    auto & columns = create.columns_list->columns->children;
    auto * it = std::find_if(columns.begin(), columns.end(), [](const ASTPtr & column)
    {
        return typeid_cast<const ASTColumnDeclaration &>(*column).name == TimeSeriesColumnNames::ID;
    });

    if (it == columns.end())
        return;

    auto & column_declaration = typeid_cast<ASTColumnDeclaration &>(**it);

    /// We add a DEFAULT for the 'id' column only if it's not specified yet.
    if (column_declaration.default_specifier.empty() && !column_declaration.default_expression)
    {
        column_declaration.default_specifier = "DEFAULT";
        column_declaration.default_expression = chooseIDAlgorithm(column_declaration);
    }
}


ASTPtr TimeSeriesDefinitionNormalizer::chooseIDAlgorithm(const ASTColumnDeclaration & id_column) const
{
    /// Build a list of arguments for a hash function.
    /// All hash functions below allow multiple arguments, so we use two arguments: metric_name, all_tags.
    ASTs arguments_for_hash_function;
    arguments_for_hash_function.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::MetricName));

    if (time_series_settings[TimeSeriesSetting::use_all_tags_column_to_generate_id])
    {
        arguments_for_hash_function.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::AllTags));
    }
    else
    {
        const Map & tags_to_columns = time_series_settings[TimeSeriesSetting::tags_to_columns];
        for (const auto & tag_name_and_column_name : tags_to_columns)
        {
            const auto & tuple = tag_name_and_column_name.safeGet<const Tuple &>();
            const auto & column_name = tuple.at(1).safeGet<String>();
            arguments_for_hash_function.push_back(std::make_shared<ASTIdentifier>(column_name));
        }
        arguments_for_hash_function.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Tags));
    }

    auto make_hash_function = [&](const String & function_name)
    {
        auto function = std::make_shared<ASTFunction>();
        function->name = function_name;
        auto arguments_list = std::make_shared<ASTExpressionList>();
        arguments_list->children = std::move(arguments_for_hash_function);
        function->arguments = arguments_list;
        return function;
    };

    /// The type of a hash function depends on the type of the 'id' column.
    auto id_type = DataTypeFactory::instance().get(id_column.type);
    WhichDataType id_type_which(*id_type);

    if (id_type_which.isUInt64())
    {
        return make_hash_function("sipHash64");
    }
    if (id_type_which.isFixedString() && typeid_cast<const DataTypeFixedString &>(*id_type).getN() == 16)
    {
        return make_hash_function("sipHash128");
    }
    if (id_type_which.isUUID())
    {
        return makeASTFunction("reinterpretAsUUID", make_hash_function("sipHash128"));
    }
    if (id_type_which.isUInt128())
    {
        return makeASTFunction("reinterpretAsUInt128", make_hash_function("sipHash128"));
    }

    throw Exception(
        ErrorCodes::INCOMPATIBLE_COLUMNS,
        "{}: The DEFAULT expression for column {} must contain an expression "
        "which will be used to calculate the identifier of each time series: {} {} DEFAULT ... "
        "If the DEFAULT expression is not specified then it can be chosen implicitly but only if the column type is one of these: "
        "UInt64, UInt128, UUID. "
        "For type {} the DEFAULT expression can't be chosen automatically, so please specify it explicitly",
        time_series_storage_id.getNameForLogs(),
        id_column.name,
        id_column.name,
        id_type->getName(),
        id_type->getName());
}


void TimeSeriesDefinitionNormalizer::addMissingInnerEnginesFromAsTable(ASTCreateQuery & create) const
{
    if (!as_create_query)
        return;

    for (auto target_kind : {ViewTarget::Data, ViewTarget::Tags, ViewTarget::Metrics})
    {
        if (as_create_query->hasTargetTableID(target_kind))
        {
            /// It's unlikely correct to use "CREATE table AS other_table" when "other_table" has external tables like this:
            /// CREATE TABLE other_table ENGINE=TimeSeries data mydata
            /// (because `table` would use the same table "mydata").
            /// Thus we just prohibit that.
            QualifiedTableName as_table{as_create_query->getDatabase(), as_create_query->getTable()};
            throw Exception(
                ErrorCodes::INCORRECT_QUERY,
                "Cannot CREATE a table AS {}.{} because it has external tables",
                backQuoteIfNeed(as_table.database), backQuoteIfNeed(as_table.table));
        }

        auto inner_table_engine = create.getTargetInnerEngine(target_kind);
        if (!inner_table_engine)
        {
            /// Copy an inner engine's definition from the other table.
            inner_table_engine = as_create_query->getTargetInnerEngine(target_kind);
            if (inner_table_engine)
                create.setTargetInnerEngine(target_kind, typeid_cast<std::shared_ptr<ASTStorage>>(inner_table_engine->clone()));
        }
    }
}


void TimeSeriesDefinitionNormalizer::addMissingInnerEngines(ASTCreateQuery & create) const
{
    for (auto target_kind : {ViewTarget::Data, ViewTarget::Tags, ViewTarget::Metrics})
    {
        if (create.hasTargetTableID(target_kind))
            continue; /// External target is set, inner engine is not needed.

        auto inner_table_engine = create.getTargetInnerEngine(target_kind);
        if (inner_table_engine && inner_table_engine->engine)
            continue; /// Engine is set already, skip it.

        if (!inner_table_engine)
        {
            /// Some part of storage definition (such as PARTITION BY) is specified, but the inner ENGINE is not: just set default one.
            inner_table_engine = std::make_shared<ASTStorage>();
            create.setTargetInnerEngine(target_kind, inner_table_engine);
        }

        /// Set engine by default.
        setInnerEngineByDefault(target_kind, *inner_table_engine);
    }
}


void TimeSeriesDefinitionNormalizer::setInnerEngineByDefault(ViewTarget::Kind inner_table_kind, ASTStorage & inner_storage_def) const
{
    switch (inner_table_kind)
    {
        case ViewTarget::Data:
        {
            inner_storage_def.set(inner_storage_def.engine, makeASTFunction("MergeTree"));
            inner_storage_def.engine->no_empty_args = false;

            if (!inner_storage_def.order_by && !inner_storage_def.primary_key && inner_storage_def.engine->name.ends_with("MergeTree"))
            {
                inner_storage_def.set(inner_storage_def.order_by,
                                      makeASTFunction("tuple",
                                                      std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::ID),
                                                      std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp)));
            }
            break;
        }

        case ViewTarget::Tags:
        {
            String engine_name;
            if (time_series_settings[TimeSeriesSetting::aggregate_min_time_and_max_time])
                engine_name = "AggregatingMergeTree";
            else
                engine_name = "ReplacingMergeTree";

            inner_storage_def.set(inner_storage_def.engine, makeASTFunction(engine_name));
            inner_storage_def.engine->no_empty_args = false;

            if (!inner_storage_def.order_by && !inner_storage_def.primary_key && inner_storage_def.engine->name.ends_with("MergeTree"))
            {
                inner_storage_def.set(inner_storage_def.primary_key,
                                      std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::MetricName));

                ASTs order_by_list;
                order_by_list.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::MetricName));
                order_by_list.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::ID));

                if (time_series_settings[TimeSeriesSetting::store_min_time_and_max_time] && !time_series_settings[TimeSeriesSetting::aggregate_min_time_and_max_time])
                {
                    order_by_list.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::MinTime));
                    order_by_list.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::MaxTime));
                }

                auto order_by_tuple = std::make_shared<ASTFunction>();
                order_by_tuple->name = "tuple";
                auto arguments_list = std::make_shared<ASTExpressionList>();
                arguments_list->children = std::move(order_by_list);
                order_by_tuple->arguments = arguments_list;
                inner_storage_def.set(inner_storage_def.order_by, order_by_tuple);
            }
            break;
        }

        case ViewTarget::Metrics:
        {
            inner_storage_def.set(inner_storage_def.engine, makeASTFunction("ReplacingMergeTree"));
            inner_storage_def.engine->no_empty_args = false;

            if (!inner_storage_def.order_by && !inner_storage_def.primary_key && inner_storage_def.engine->name.ends_with("MergeTree"))
            {
                inner_storage_def.set(inner_storage_def.order_by, std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::MetricFamilyName));
            }
            break;
        }

        default:
            UNREACHABLE(); /// This function must not be called with any other `kind`.
    }
}

}
