#include <Storages/StorageTimeSeriesSelector.h>

#include <Common/quoteString.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/Prometheus/parseTimeSeriesTypes.h>
#include <Parsers/makeASTForLogicalFunction.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace TimeSeriesSetting
{
    extern const TimeSeriesSettingsMap tags_to_columns;
    extern const TimeSeriesSettingsBool filter_by_min_time_and_max_time;
}

StorageTimeSeriesSelector::Configuration StorageTimeSeriesSelector::getConfiguration(ASTs & args, const ContextPtr & context)
{
    std::string_view function_name = "timeSeriesSelector";

    size_t min_num_args = 4;
    size_t max_num_args = 5;

    if ((args.size() < min_num_args) || (args.size() > max_num_args))
    {
        std::string_view expected_args = "[database, ] time_series_table, selector, min_time, max_time";
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Table function '{}' requires {}..{} arguments: {}({})",
                        function_name, min_num_args, max_num_args, function_name, expected_args);
    }

    size_t argument_index = 0;

    StorageID time_series_storage_id = StorageID::createEmpty();

    if (args.size() == min_num_args)
    {
        /// timeSeriesSelector( [my_db.]my_time_series_table, ... )
        if (const auto * id = args[argument_index]->as<ASTIdentifier>())
        {
            if (auto table_id = id->createTable())
            {
                time_series_storage_id = table_id->getTableId();
                ++argument_index;
            }
        }
    }

    if (time_series_storage_id.empty())
    {
        if (args.size() == min_num_args)
        {
            /// timeSeriesSelector( 'my_time_series_table', ... )
            auto table_name_field = evaluateConstantExpression(args[argument_index++], context).first;

            if (table_name_field.getType() != Field::Types::String)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument 'table_name' must be a literal with type String, got {}", table_name_field.getType());

            time_series_storage_id.table_name = table_name_field.safeGet<String>();
        }
        else
        {
            /// timeSeriesSelector( 'mydb', 'my_time_series_table', ... )
            auto database_name_field = evaluateConstantExpression(args[argument_index++], context).first;
            auto table_name_field = evaluateConstantExpression(args[argument_index++], context).first;

            if (database_name_field.getType() != Field::Types::String)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument 'database_name' must be a literal with type String, got {}", database_name_field.getType());

            if (table_name_field.getType() != Field::Types::String)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument 'table_name' must be a literal with type String, got {}", table_name_field.getType());

            time_series_storage_id.database_name = database_name_field.safeGet<String>();
            time_series_storage_id.table_name = table_name_field.safeGet<String>();
        }
    }

    time_series_storage_id = context->resolveStorageID(time_series_storage_id);

    auto time_series_storage = storagePtrToTimeSeries(DatabaseCatalog::instance().getTable(time_series_storage_id, context));
    auto data_table_metadata = time_series_storage->getTargetTable(ViewTarget::Data, context)->getInMemoryMetadataPtr();
    auto id_data_type = data_table_metadata->columns.get(TimeSeriesColumnNames::ID).type;
    auto timestamp_data_type = data_table_metadata->columns.get(TimeSeriesColumnNames::Timestamp).type;
    auto scalar_data_type = data_table_metadata->columns.get(TimeSeriesColumnNames::Value).type;

    UInt32 timestamp_scale = tryGetDecimalScale(*timestamp_data_type).value_or(0);

    auto selector_field = evaluateConstantExpression(args[argument_index++], context).first;
    if (selector_field.getType() != Field::Types::String)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument 'selector' must be a literal with type String, got {}", selector_field.getType());

    PrometheusQueryTree selector{selector_field.safeGet<String>()};

    auto [min_time_field, min_time_type] = evaluateConstantExpression(args[argument_index++], context);
    auto [max_time_field, max_time_type] = evaluateConstantExpression(args[argument_index++], context);

    auto min_time = parseTimeSeriesTimestamp(min_time_field, min_time_type, timestamp_scale);
    auto max_time = parseTimeSeriesTimestamp(max_time_field, max_time_type, timestamp_scale);

    chassert(argument_index == args.size());

    Configuration config;
    config.time_series_storage_id = std::move(time_series_storage_id);
    config.id_data_type = std::move(id_data_type);
    config.timestamp_data_type = std::move(timestamp_data_type);
    config.scalar_data_type = std::move(scalar_data_type);
    config.selector = std::move(selector);
    config.min_time = min_time;
    config.max_time = max_time;
    return config;
}

StorageTimeSeriesSelector::StorageTimeSeriesSelector(
    const StorageID & table_id_, const ColumnsDescription & columns_, const Configuration & config_)
    : IStorage{table_id_}
    , config(config_)
{
    const auto * node = config.selector.getRoot();
    if (!node || (node->node_type != PrometheusQueryTree::NodeType::InstantSelector))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} is not an instant selector", quoteString(config.selector.toString()));

    if (config.min_time > config.max_time)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Max time {} is less than min time {}",
                        Field{config.min_time}, Field{config.max_time});

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    setInMemoryMetadata(storage_metadata);
}


namespace
{
    /// Makes an AST for the expression referencing a tag value.
    ASTPtr tagNameToAST(const String & tag_name, const std::unordered_map<String, String> & column_name_by_tag_name)
    {
        if (tag_name == TimeSeriesTagNames::MetricName)
            return make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricName);

        auto it = column_name_by_tag_name.find(tag_name);
        if (it != column_name_by_tag_name.end())
            return make_intrusive<ASTIdentifier>(it->second);

        /// arrayElement() can be used to extract a value from a Map too.
        return makeASTFunction("arrayElement", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Tags), make_intrusive<ASTLiteral>(tag_name));
    }

    ASTPtr matcherToAST(const PrometheusQueryTree::Matcher & matcher, const std::unordered_map<String, String> & column_name_by_tag_name)
    {
        std::string_view function_name;
        bool add_anchors = false;
        bool add_not = false;

        auto matcher_type = matcher.matcher_type;
        switch (matcher_type)
        {
            case PrometheusQueryTree::MatcherType::EQ:  function_name = "equals"; break;
            case PrometheusQueryTree::MatcherType::NE:  function_name = "notEquals"; break;
            case PrometheusQueryTree::MatcherType::RE:  function_name = "match"; add_anchors = true; break;
            case PrometheusQueryTree::MatcherType::NRE: function_name = "match"; add_anchors = true; add_not = true; break;
        }

        String value = matcher.label_value;
        if (add_anchors)
        {
            if (!value.starts_with('^'))
                value = '^' + value;
            if (!value.ends_with('$'))
                value += '$';
        }
        ASTPtr res = makeASTFunction(function_name, tagNameToAST(matcher.label_name, column_name_by_tag_name), make_intrusive<ASTLiteral>(value));
        if (add_not)
            res = makeASTFunction("not", res);
        return res;
    }

    ASTPtr makeWhereFilterForTagsTable(
        const PrometheusQueryTree::MatcherList & matchers,
        const std::unordered_map<String, String> & column_name_by_tag_name,
        const std::optional<DateTime64> & min_time,
        const std::optional<DateTime64> & max_time,
        const DataTypePtr & timestamp_data_type)
    {
        ASTs asts;
        for (const auto & matcher : matchers)
            asts.push_back(matcherToAST(matcher, column_name_by_tag_name));

        if (asts.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Instant selector without matchers is not allowed");

        if (min_time)
        {
            /// tags_table.max_time >= min_time
            asts.push_back(makeASTFunction(
                "greaterOrEquals",
                make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MaxTime),
                timeSeriesTimestampToAST(*min_time, timestamp_data_type)));
        }

        if (max_time)
        {
            /// tags_table.min_time <= max_time
            asts.push_back(makeASTFunction(
                "lessOrEquals",
                make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MinTime),
                timeSeriesTimestampToAST(*max_time, timestamp_data_type)));
        }

        return makeASTForLogicalAnd(std::move(asts));
    }

    ASTPtr makeSelectQueryFromTagsTable(
        const StorageID & tags_table_id,
        const PrometheusQueryTree::MatcherList & matchers,
        const std::unordered_map<String, String> & column_name_by_tag_name,
        const std::optional<DateTime64> & min_time,
        const std::optional<DateTime64> & max_time,
        const DataTypePtr & timestamp_data_type)
    {
        auto select_query = make_intrusive<ASTSelectQuery>();

        /// SELECT timeSeriesStoreTags(id, tags, '__name__', metric_name, tag_name1, tag_column1, ...)
        {
            auto select_list_exp = make_intrusive<ASTExpressionList>();
            auto & select_list = select_list_exp->children;

            ASTs args;
            args.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID));
            args.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Tags));
            args.push_back(make_intrusive<ASTLiteral>(TimeSeriesTagNames::MetricName));
            args.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricName));

            for (const auto & [tag_name, column_name] : column_name_by_tag_name)
            {
                args.push_back(make_intrusive<ASTLiteral>(tag_name));
                args.push_back(make_intrusive<ASTIdentifier>(column_name));
            }

            select_list.push_back(makeASTFunction("timeSeriesStoreTags", std::move(args)));
            select_query->setExpression(ASTSelectQuery::Expression::SELECT, select_list_exp);
        }

        /// FROM tags_table_id
        auto tables = make_intrusive<ASTTablesInSelectQuery>();

        {
            auto table = make_intrusive<ASTTablesInSelectQueryElement>();
            auto table_exp = make_intrusive<ASTTableExpression>();
            table_exp->database_and_table_name = make_intrusive<ASTTableIdentifier>(tags_table_id);
            table_exp->children.emplace_back(table_exp->database_and_table_name);

            table->table_expression = table_exp;
            tables->children.push_back(table);

            select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables);
        }

        /// WHERE <filter>
        {
            auto where_filter = makeWhereFilterForTagsTable(matchers, column_name_by_tag_name, min_time, max_time, timestamp_data_type);
            select_query->setExpression(ASTSelectQuery::Expression::WHERE, std::move(where_filter));
        }

        /// Wrap the select query into ASTSelectWithUnionQuery.
        auto select_with_union_query = make_intrusive<ASTSelectWithUnionQuery>();
        {
            select_with_union_query->union_mode = SelectUnionMode::UNION_DEFAULT;
            auto list_of_selects = make_intrusive<ASTExpressionList>();
            list_of_selects->children.push_back(std::move(select_query));
            select_with_union_query->children.push_back(std::move(list_of_selects));
            select_with_union_query->list_of_selects = select_with_union_query->children.back();
        }

        return select_with_union_query;
    }

    ASTPtr makeWhereFilterForDataTable(
        ASTPtr select_query_from_tags_table,
        DateTime64 min_time,
        DateTime64 max_time,
        const DataTypePtr & timestamp_data_type)
    {
        ASTs conditions;

        /// id IN (SELECT id FROM (select_id_query))
        conditions.push_back(makeASTFunction("in", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID), select_query_from_tags_table));

        /// timestamp >= min_time
        conditions.push_back(makeASTFunction(
            "greaterOrEquals",
            make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Timestamp),
            timeSeriesTimestampToAST(min_time, timestamp_data_type)));

        /// timestamp <= max_time
        conditions.push_back(makeASTFunction(
            "lessOrEquals",
            make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Timestamp),
            timeSeriesTimestampToAST(max_time, timestamp_data_type)));

        return makeASTForLogicalAnd(std::move(conditions));
    }

    ASTPtr makeSelectQueryFromDataTable(const StorageID & data_table_id,
                                        ASTPtr select_query_from_tags_table,
                                        DateTime64 min_time,
                                        DateTime64 max_time,
                                        const DataTypePtr & id_data_type,
                                        const DataTypePtr & timestamp_data_type,
                                        const DataTypePtr & scalar_data_type)
    {
        auto select_query = make_intrusive<ASTSelectQuery>();

        /// SELECT id, timestamp, value
        {
            auto select_list_exp = make_intrusive<ASTExpressionList>();
            auto & select_list = select_list_exp->children;

            select_list.push_back(makeASTFunction(
                "CAST", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID), make_intrusive<ASTLiteral>(id_data_type->getName())));
            select_list.back()->setAlias(TimeSeriesColumnNames::ID);

            select_list.push_back(
                timeSeriesTimestampASTCast(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Timestamp), timestamp_data_type));
            select_list.back()->setAlias(TimeSeriesColumnNames::Timestamp);

            select_list.push_back(timeSeriesScalarASTCast(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Value), scalar_data_type));
            select_list.back()->setAlias(TimeSeriesColumnNames::Value);

            select_query->setExpression(ASTSelectQuery::Expression::SELECT, select_list_exp);
        }

        /// FROM data_table_id
        auto tables = make_intrusive<ASTTablesInSelectQuery>();

        {
            auto table = make_intrusive<ASTTablesInSelectQueryElement>();
            auto table_exp = make_intrusive<ASTTableExpression>();
            table_exp->database_and_table_name = make_intrusive<ASTTableIdentifier>(data_table_id);
            table_exp->children.emplace_back(table_exp->database_and_table_name);

            table->table_expression = table_exp;
            tables->children.push_back(table);

            select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables);
        }

        /// WHERE id IN (SELECT id FROM (select_query_from_tags_table))
        {
            auto where_filter = makeWhereFilterForDataTable(select_query_from_tags_table, min_time, max_time, timestamp_data_type);
            select_query->setExpression(ASTSelectQuery::Expression::WHERE, std::move(where_filter));
        }

        /// Wrap the select query into ASTSelectWithUnionQuery.
        auto select_with_union_query = make_intrusive<ASTSelectWithUnionQuery>();
        select_with_union_query->union_mode = SelectUnionMode::UNION_DEFAULT;
        auto list_of_selects = make_intrusive<ASTExpressionList>();
        list_of_selects->children.push_back(std::move(select_query));
        select_with_union_query->children.push_back(std::move(list_of_selects));
        select_with_union_query->list_of_selects = select_with_union_query->children.back();

        return select_with_union_query;
    }

    /// Makes a mapping from a tag name to a column name.
    std::unordered_map<String, String> makeColumnNameByTagNameMap(const TimeSeriesSettings & storage_settings)
    {
        std::unordered_map<String, String> res;
        const Map & tags_to_columns = storage_settings[TimeSeriesSetting::tags_to_columns];
        for (const auto & tag_name_and_column_name : tags_to_columns)
        {
            const auto & tuple = tag_name_and_column_name.safeGet<Tuple>();
            const auto & tag_name = tuple.at(0).safeGet<String>();
            const auto & column_name = tuple.at(1).safeGet<String>();
            res[tag_name] = column_name;
        }
        return res;
    }
}


void StorageTimeSeriesSelector::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & /* storage_snapshot */,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /* processed_stage */,
    size_t /* max_block_size */,
    size_t /* num_streams */)
{
    auto time_series_storage = storagePtrToTimeSeries(DatabaseCatalog::instance().getTable(config.time_series_storage_id, context));
    const auto & time_series_settings = time_series_storage->getStorageSettings();

    const auto & matchers = typeid_cast<const PrometheusQueryTree::InstantSelector &>(*config.selector.getRoot()).matchers;

    auto data_table_id = time_series_storage->getTargetTableId(ViewTarget::Data);
    auto tags_table_id = time_series_storage->getTargetTableId(ViewTarget::Tags);

    auto column_name_by_tag_name = makeColumnNameByTagNameMap(time_series_settings);

    std::optional<DateTime64> min_time_to_filter_ids;
    std::optional<DateTime64> max_time_to_filter_ids;
    if (time_series_settings[TimeSeriesSetting::filter_by_min_time_and_max_time])
    {
        min_time_to_filter_ids = config.min_time;
        max_time_to_filter_ids = config.max_time;
    }

    ASTPtr select_query_from_tags_table = makeSelectQueryFromTagsTable(
        tags_table_id, matchers, column_name_by_tag_name, min_time_to_filter_ids, max_time_to_filter_ids, config.timestamp_data_type);

    ASTPtr select_query_from_data_table = makeSelectQueryFromDataTable(
        data_table_id,
        select_query_from_tags_table,
        config.min_time,
        config.max_time,
        config.id_data_type,
        config.timestamp_data_type,
        config.scalar_data_type);

    auto options = SelectQueryOptions(QueryProcessingStage::Complete, 0, false, query_info.settings_limit_offset_done);

    InterpreterSelectQueryAnalyzer interpreter(select_query_from_data_table, context, options, column_names);
    interpreter.addStorageLimits(*query_info.storage_limits);
    query_plan = std::move(interpreter).extractQueryPlan();
}

}
