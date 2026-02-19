#include <Storages/StorageTimeSeriesSelector.h>

#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/makeASTForLogicalFunction.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace TimeSeriesSetting
{
    extern const TimeSeriesSettingsMap tags_to_columns;
    extern const TimeSeriesSettingsBool filter_by_min_time_and_max_time;
}

StorageTimeSeriesSelector::StorageTimeSeriesSelector(
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const StorageID & time_series_storage_id_,
    const PrometheusQueryTree & instant_selector_,
    const Field & min_time_,
    const Field & max_time_)
    : IStorage{table_id_}
    , time_series_storage_id{time_series_storage_id_}
    , instant_selector{instant_selector_}
    , min_time{min_time_}
    , max_time{max_time_}
{
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
            return std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::MetricName);

        auto it = column_name_by_tag_name.find(tag_name);
        if (it != column_name_by_tag_name.end())
            return std::make_shared<ASTIdentifier>(it->second);

        /// arrayElement() can be used to extract a value from a Map too.
        return makeASTFunction("arrayElement", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Tags), std::make_shared<ASTLiteral>(tag_name));
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
        ASTPtr res = makeASTFunction(function_name, tagNameToAST(matcher.label_name, column_name_by_tag_name), std::make_shared<ASTLiteral>(value));
        if (add_not)
            res = makeASTFunction("not", res);
        return res;
    }

    ASTPtr makeWhereFilterForTagsTable(
        const PrometheusQueryTree::MatcherList & matchers,
        const std::unordered_map<String, String> & column_name_by_tag_name,
        const Field & min_time,
        const Field & max_time)
    {
        ASTs asts;
        for (const auto & matcher : matchers)
            asts.push_back(matcherToAST(matcher, column_name_by_tag_name));

        if (asts.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Instant selector without matchers is not allowed");

        if (!min_time.isNull())
        {
            /// tags_table.max_time >= min_time
            asts.push_back(makeASTFunction(
                "greaterOrEquals",
                std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::MaxTime),
                std::make_shared<ASTLiteral>(min_time)));
        }

        if (!max_time.isNull())
        {
            /// tags_table.min_time <= max_time
            asts.push_back(makeASTFunction(
                "lessOrEquals",
                std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::MinTime),
                std::make_shared<ASTLiteral>(max_time)));
        }

        return makeASTForLogicalAnd(std::move(asts));
    }

    ASTPtr makeSelectQueryFromTagsTable(
        const StorageID & tags_table_id,
        const PrometheusQueryTree::MatcherList & matchers,
        const std::unordered_map<String, String> & column_name_by_tag_name,
        const Field & min_time, const Field & max_time)
    {
        auto select_query = std::make_shared<ASTSelectQuery>();

        /// SELECT timeSeriesStoreTags(id, tags, '__name__', metric_name, tag_name1, tag_column1, ...)
        {
            auto select_list_exp = std::make_shared<ASTExpressionList>();
            auto & select_list = select_list_exp->children;

            ASTs args;
            args.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::ID));
            args.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Tags));
            args.push_back(std::make_shared<ASTLiteral>(TimeSeriesTagNames::MetricName));
            args.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::MetricName));

            for (const auto & [tag_name, column_name] : column_name_by_tag_name)
            {
                args.push_back(std::make_shared<ASTLiteral>(tag_name));
                args.push_back(std::make_shared<ASTIdentifier>(column_name));
            }

            select_list.push_back(makeASTFunction("timeSeriesStoreTags", std::move(args)));
            select_query->setExpression(ASTSelectQuery::Expression::SELECT, select_list_exp);
        }

        /// FROM tags_table_id
        auto tables = std::make_shared<ASTTablesInSelectQuery>();

        {
            auto table = std::make_shared<ASTTablesInSelectQueryElement>();
            auto table_exp = std::make_shared<ASTTableExpression>();
            table_exp->database_and_table_name = std::make_shared<ASTTableIdentifier>(tags_table_id);
            table_exp->children.emplace_back(table_exp->database_and_table_name);

            table->table_expression = table_exp;
            tables->children.push_back(table);

            select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables);
        }

        /// WHERE <filter>
        {
            auto where_filter = makeWhereFilterForTagsTable(matchers, column_name_by_tag_name, min_time, max_time);
            select_query->setExpression(ASTSelectQuery::Expression::WHERE, std::move(where_filter));
        }

        /// Wrap the select query into ASTSelectWithUnionQuery.
        auto select_with_union_query = std::make_shared<ASTSelectWithUnionQuery>();
        {
            select_with_union_query->union_mode = SelectUnionMode::UNION_DEFAULT;
            auto list_of_selects = std::make_shared<ASTExpressionList>();
            list_of_selects->children.push_back(std::move(select_query));
            select_with_union_query->children.push_back(std::move(list_of_selects));
            select_with_union_query->list_of_selects = select_with_union_query->children.back();
        }

        return select_with_union_query;
    }

    ASTPtr makeWhereFilterForDataTable(
        ASTPtr select_query_from_tags_table,
        const Field & min_time,
        const Field & max_time)
    {
        ASTs conditions;

        /// id IN (SELECT id FROM (select_id_query))
        conditions.push_back(makeASTFunction("in", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::ID), select_query_from_tags_table));

        if (!min_time.isNull())
        {
            /// timestamp >= min_time
            conditions.push_back(makeASTFunction(
                "greaterOrEquals",
                std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp),
                std::make_shared<ASTLiteral>(min_time)));
        }

        if (!max_time.isNull())
        {
            /// timestamp <= max_time
            conditions.push_back(makeASTFunction(
                "lessOrEquals",
                std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp),
                std::make_shared<ASTLiteral>(max_time)));
        }

        return makeASTForLogicalAnd(std::move(conditions));
    }

    ASTPtr makeSelectQueryFromDataTable(const StorageID & data_table_id,
                                        ASTPtr select_query_from_tags_table,
                                        const Field & min_time, const Field & max_time)
    {
        auto select_query = std::make_shared<ASTSelectQuery>();

        /// SELECT id, timestamp, value
        {
            auto select_list_exp = std::make_shared<ASTExpressionList>();
            auto & select_list = select_list_exp->children;

            select_list.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::ID));
            select_list.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp));
            select_list.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value));

            select_query->setExpression(ASTSelectQuery::Expression::SELECT, select_list_exp);
        }

        /// FROM data_table_id
        auto tables = std::make_shared<ASTTablesInSelectQuery>();

        {
            auto table = std::make_shared<ASTTablesInSelectQueryElement>();
            auto table_exp = std::make_shared<ASTTableExpression>();
            table_exp->database_and_table_name = std::make_shared<ASTTableIdentifier>(data_table_id);
            table_exp->children.emplace_back(table_exp->database_and_table_name);

            table->table_expression = table_exp;
            tables->children.push_back(table);

            select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables);
        }

        /// WHERE id IN (SELECT id FROM (select_query_from_tags_table))
        {
            auto where_filter = makeWhereFilterForDataTable(select_query_from_tags_table, min_time, max_time);
            select_query->setExpression(ASTSelectQuery::Expression::WHERE, std::move(where_filter));
        }

        /// Wrap the select query into ASTSelectWithUnionQuery.
        auto select_with_union_query = std::make_shared<ASTSelectWithUnionQuery>();
        select_with_union_query->union_mode = SelectUnionMode::UNION_DEFAULT;
        auto list_of_selects = std::make_shared<ASTExpressionList>();
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
    auto time_series_storage = storagePtrToTimeSeries(DatabaseCatalog::instance().getTable(time_series_storage_id, context));
    const auto & time_series_settings = time_series_storage->getStorageSettings();

    const auto * node = instant_selector.getRoot();
    if (!node || (node->node_type != PrometheusQueryTree::NodeType::InstantSelector))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "An instant selector is required");
    const auto & matchers = typeid_cast<const PrometheusQueryTree::InstantSelector &>(*node).matchers;

    auto data_table_id = time_series_storage->getTargetTableId(ViewTarget::Data);
    auto tags_table_id = time_series_storage->getTargetTableId(ViewTarget::Tags);

    auto column_name_by_tag_name = makeColumnNameByTagNameMap(time_series_settings);

    Field min_time_to_filter_ids;
    Field max_time_to_filter_ids;
    if (time_series_settings[TimeSeriesSetting::filter_by_min_time_and_max_time])
    {
        min_time_to_filter_ids = min_time;
        max_time_to_filter_ids = max_time;
    }
    ASTPtr select_query_from_tags_table = makeSelectQueryFromTagsTable(tags_table_id, matchers, column_name_by_tag_name, min_time_to_filter_ids, max_time_to_filter_ids);
    ASTPtr select_query_from_data_table = makeSelectQueryFromDataTable(data_table_id, select_query_from_tags_table, min_time, max_time);

    auto options = SelectQueryOptions(QueryProcessingStage::Complete, 0, false, query_info.settings_limit_offset_done);

    InterpreterSelectQueryAnalyzer interpreter(select_query_from_data_table, context, options, column_names);
    interpreter.addStorageLimits(*query_info.storage_limits);
    query_plan = std::move(interpreter).extractQueryPlan();
}

}
