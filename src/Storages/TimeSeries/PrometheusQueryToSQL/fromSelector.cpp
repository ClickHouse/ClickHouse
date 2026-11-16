#include <Storages/TimeSeries/PrometheusQueryToSQL/fromSelector.h>

#include <DataTypes/IDataType.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/Prometheus/TimeSeriesVersion.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionOverRange.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesNativeHistograms.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>


namespace DB::PrometheusQueryToSQL
{

namespace
{
    constexpr UInt64 STALE_NAN_BITS = 0x7ff0000000000002ULL;

    ASTPtr isStaleMarker(ASTPtr value)
    {
        return makeASTFunction(
            "equals",
            makeASTFunction("reinterpretAsUInt64", std::move(value)),
            make_intrusive<ASTLiteral>(STALE_NAN_BITS));
    }

    /// A native-histogram stale marker is flagged in the `flags` of its payload rather than carried in a value.
    ASTPtr isStaleHistogramMarker(ASTPtr flags)
    {
        return makeASTFunction(
            "notEquals",
            makeASTFunction("bitAnd", std::move(flags), make_intrusive<ASTLiteral>(UInt64{TimeSeriesHistogramFlags::StaleMarker})),
            make_intrusive<ASTLiteral>(UInt64{0}));
    }

    /// Makes a SELECT query reading from a table function, optionally filtered by `where`.
    ASTPtr makeSelectorArm(ASTs select_list, ASTPtr table_function, ASTPtr where = nullptr)
    {
        auto select_query = make_intrusive<ASTSelectQuery>();

        {
            auto select_list_exp = make_intrusive<ASTExpressionList>();
            select_list_exp->children = std::move(select_list);
            select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_list_exp));
        }

        {
            auto table_exp = make_intrusive<ASTTableExpression>();
            table_exp->table_function = std::move(table_function);
            table_exp->children.push_back(table_exp->table_function);

            auto table = make_intrusive<ASTTablesInSelectQueryElement>();
            table->table_expression = table_exp;
            table->children.push_back(std::move(table_exp));

            auto tables = make_intrusive<ASTTablesInSelectQuery>();
            tables->children.push_back(std::move(table));

            select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables));
        }

        if (where)
            select_query->setExpression(ASTSelectQuery::Expression::WHERE, std::move(where));

        return select_query;
    }

    /// Combines two SELECT queries into one: <float_arm> UNION ALL <histogram_arm>.
    ASTPtr makeUnionAll(ASTPtr float_arm, ASTPtr histogram_arm)
    {
        auto select_with_union_query = make_intrusive<ASTSelectWithUnionQuery>();
        select_with_union_query->union_mode = SelectUnionMode::UNION_ALL;
        select_with_union_query->is_normalized = true;

        auto list_of_selects = make_intrusive<ASTExpressionList>();
        list_of_selects->children.push_back(std::move(float_arm));
        list_of_selects->children.push_back(std::move(histogram_arm));
        select_with_union_query->children.push_back(list_of_selects);
        select_with_union_query->list_of_selects = select_with_union_query->children.back();

        return select_with_union_query;
    }

    SQLQueryPiece fromRangeSelector(std::string_view instant_selector_text,
                                    const Node * node,
                                    bool filter_stale_markers,
                                    ConverterContext & context)
    {
        auto node_range = context.node_range_getter.get(node);
        if (node_range.empty())
            return SQLQueryPiece{node, ResultType::RANGE_VECTOR, StoreMethod::EMPTY};

        /// The range is (start_time - window, end_time] at the result scale. The table functions convert the bounds to the scale
        /// of the table itself, rounding them towards the inside of the range.
        TimestampType min_time = node_range.start_time - node_range.window + 1;
        TimestampType max_time = node_range.end_time;

        /// `return_columns` is the last argument of timeSeriesSelector(); timeSeriesHistogramSelector() has none.
        auto make_table_function = [&](std::string_view function_name, std::string_view return_columns = {})
        {
            auto function = makeASTFunction(
                function_name,
                make_intrusive<ASTLiteral>(context.time_series_storage_id.getDatabaseName()),
                make_intrusive<ASTLiteral>(context.time_series_storage_id.getTableName()),
                make_intrusive<ASTLiteral>(String{instant_selector_text}),
                timeSeriesTimestampToAST(min_time, context.result_timestamp_type),
                timeSeriesTimestampToAST(max_time, context.result_timestamp_type));
            if (!return_columns.empty())
                function->arguments->children.push_back(make_intrusive<ASTLiteral>(String{return_columns}));
            return function;
        };

        /// Prometheus range selectors omit the dedicated stale-NaN payload while preserving
        /// ordinary NaN samples as data, and omit the histogram stale markers the same way.
        auto make_float_filter = [&]() -> ASTPtr
        {
            if (!filter_stale_markers)
                return nullptr;
            return makeASTFunction("not", isStaleMarker(make_intrusive<ASTIdentifier>(ColumnNames::Value)));
        };

        auto make_histogram_filter = [&]() -> ASTPtr
        {
            if (!filter_stale_markers)
                return nullptr;
            return makeASTFunction("not", isStaleHistogramMarker(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Flags)));
        };

        if (!context.storage_has_native_histograms)
        {
            /// The argument `return_columns` of the table function chooses the returned columns. The store method follows the layout of
            /// the samples table, so that the samples are returned as they are stored, without a conversion: the column `samples`
            /// with the samples of one time bucket for a table storing buckets of samples, the columns `timestamp` and `value`
            /// for a table of an earlier version storing one sample per row.
            const bool table_has_bucketed_samples = (context.time_series_version >= TimeSeriesVersion::MIN_WITH_BUCKETED_SAMPLES);
            SQLQueryPiece res{node, ResultType::RANGE_VECTOR, table_has_bucketed_samples ? StoreMethod::SAMPLES : StoreMethod::TIMESTAMP_AND_VALUE};

            /// SELECT timeSeriesIdToGroup(id) AS group, samples
            /// FROM timeSeriesSelector(<database>, <time_series_table>, <selector>, <min_time>, <max_time>, 'samples')
            /// or
            /// SELECT timeSeriesIdToGroup(id) AS group, timestamp, value
            /// FROM timeSeriesSelector(<database>, <time_series_table>, <selector>, <min_time>, <max_time>, 'timestamp_and_value')
            SelectQueryBuilder builder;

            builder.select_list.push_back(makeASTFunction("timeSeriesIdToGroup", make_intrusive<ASTIdentifier>(ColumnNames::ID)));
            builder.select_list.back()->setAlias(ColumnNames::Group);

            /// The samples keep the types they have in the table (see StoreMethod).
            if (table_has_bucketed_samples)
            {
                ASTPtr samples = make_intrusive<ASTIdentifier>(ColumnNames::Samples);

                /// arrayFilter(sample -> NOT <sample.2 is a stale marker>, samples) AS samples
                if (filter_stale_markers)
                {
                    const String iterator_name = "sample";
                    samples = makeASTFunction(
                        "arrayFilter",
                        makeASTFunction(
                            "lambda",
                            makeASTFunction("tuple", make_intrusive<ASTIdentifier>(iterator_name)),
                            makeASTFunction("not", isStaleMarker(makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>(iterator_name), make_intrusive<ASTLiteral>(UInt64{2}))))),
                        std::move(samples));
                    samples->setAlias(ColumnNames::Samples);
                }

                builder.select_list.push_back(std::move(samples));
            }
            else
            {
                builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Timestamp));
                builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Value));

                /// WHERE NOT <value is a stale marker>
                builder.where = make_float_filter();
            }

            builder.from_table_function = make_table_function("timeSeriesSelector", table_has_bucketed_samples ? "samples" : "timestamp_and_value");

            res.select_query = builder.getSelectQuery();
            return res;
        }

        SQLQueryPiece res{node, ResultType::RANGE_VECTOR, StoreMethod::HISTOGRAM_RAW_DATA};

        /// A combined selector stream (StoreMethod::HISTOGRAM_RAW_DATA): the float arm UNION ALL the histogram arm, one sample per row.
        /// UNION ALL unifies columns by position, so every fabricated default column below is an explicit cast to the exact column type.
        ASTs float_arm_select_list;
        {
            float_arm_select_list.push_back(makeASTFunction("timeSeriesIdToGroup", make_intrusive<ASTIdentifier>(ColumnNames::ID)));
            float_arm_select_list.back()->setAlias(ColumnNames::Group);

            float_arm_select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Timestamp));
            float_arm_select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Value));

            /// The float arm carries no histograms: the payload columns hold default values.
            for (const auto & [name, type] : getTimeSeriesHistogramPayloadColumns())
            {
                ASTPtr default_value = WhichDataType(type).isArray()
                    ? make_intrusive<ASTLiteral>(Array{})
                    : make_intrusive<ASTLiteral>(UInt64{0});
                float_arm_select_list.push_back(makeASTFunction(
                    "_CAST", std::move(default_value), make_intrusive<ASTLiteral>(type->getName())));
                float_arm_select_list.back()->setAlias(name);
            }

            float_arm_select_list.push_back(makeASTFunction(
                "_CAST", make_intrusive<ASTLiteral>(UInt64{0}), make_intrusive<ASTLiteral>("UInt8")));
            float_arm_select_list.back()->setAlias(ColumnNames::IsHistogram);
        }

        ASTs histogram_arm_select_list;
        {
            histogram_arm_select_list.push_back(makeASTFunction("timeSeriesIdToGroup", make_intrusive<ASTIdentifier>(ColumnNames::ID)));
            histogram_arm_select_list.back()->setAlias(ColumnNames::Group);

            histogram_arm_select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Timestamp));

            /// The histogram arm carries no float values: `value` is a dummy zero. The float arm keeps the type of the values
            /// in the table (see StoreMethod), UNION ALL converts `Float32` to `Float64` if needed.
            histogram_arm_select_list.push_back(makeASTFunction(
                "_CAST", make_intrusive<ASTLiteral>(UInt64{0}), make_intrusive<ASTLiteral>("Float64")));
            histogram_arm_select_list.back()->setAlias(ColumnNames::Value);

            for (const auto & [name, type] : getTimeSeriesHistogramPayloadColumns())
                histogram_arm_select_list.push_back(make_intrusive<ASTIdentifier>(name));

            histogram_arm_select_list.push_back(makeASTFunction(
                "_CAST", make_intrusive<ASTLiteral>(UInt64{1}), make_intrusive<ASTLiteral>("UInt8")));
            histogram_arm_select_list.back()->setAlias(ColumnNames::IsHistogram);
        }

        res.select_query = makeUnionAll(
            makeSelectorArm(std::move(float_arm_select_list), make_table_function("timeSeriesSelector", "timestamp_and_value"), make_float_filter()),
            makeSelectorArm(
                std::move(histogram_arm_select_list), make_table_function("timeSeriesHistogramSelector"), make_histogram_filter()));
        return res;
    }

    /// The same as replaceStaleMarkersWithNulls() below, but for a combined grid (StoreMethod::HISTOGRAM_GRID):
    /// a step whose newest sample is a stale marker, a float or a histogram one, gets no sample at all.
    SQLQueryPiece replaceStaleMarkersWithNullsInHistogramGrid(SQLQueryPiece && histogram_grid, ConverterContext & context)
    {
        auto make_lambda = [](std::initializer_list<const char *> arg_names, ASTPtr body)
        {
            auto args_tuple = makeASTFunction("tuple");
            for (const char * arg_name : arg_names)
                args_tuple->arguments->children.push_back(make_intrusive<ASTIdentifier>(arg_name));
            return makeASTFunction("lambda", std::move(args_tuple), std::move(body));
        };

        auto is_stale = [](const char * arg_name)
        {
            return makeASTFunction(
                "and",
                makeASTFunction("isNotNull", make_intrusive<ASTIdentifier>(arg_name)),
                isStaleMarker(makeASTFunction("assumeNotNull", make_intrusive<ASTIdentifier>(arg_name))));
        };

        /// The histogram arm holds `Nullable` payload tuples; `assumeNotNull` of a NULL one has zero `flags`.
        auto is_stale_histogram = [](const char * arg_name)
        {
            return isStaleHistogramMarker(makeASTFunction(
                "tupleElement",
                makeASTFunction("assumeNotNull", make_intrusive<ASTIdentifier>(arg_name)),
                make_intrusive<ASTLiteral>(UInt64{TimeSeriesHistogramPayloadTupleIndex::Flags + 1})));
        };

        /// SELECT group, values, histogram_values,
        ///        arrayMap((k, v, h) -> if(((k = 0) AND <v is a stale marker>) OR ((k = 1) AND <h is a stale marker>), NULL, k),
        ///                 sample_kinds, values, histogram_values) AS sample_kinds
        /// FROM <histogram_grid>
        SelectQueryBuilder kinds_builder;
        kinds_builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
        kinds_builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));
        kinds_builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::HistogramValues));
        kinds_builder.select_list.push_back(makeASTFunction(
            "arrayMap",
            make_lambda({"k", "v", "h"},
                makeASTFunction(
                    "if",
                    makeASTFunction(
                        "or",
                        makeASTFunction(
                            "and",
                            makeASTFunction("equals", make_intrusive<ASTIdentifier>("k"), make_intrusive<ASTLiteral>(UInt64{0})),
                            is_stale("v")),
                        makeASTFunction(
                            "and",
                            makeASTFunction("equals", make_intrusive<ASTIdentifier>("k"), make_intrusive<ASTLiteral>(UInt64{1})),
                            is_stale_histogram("h"))),
                    make_intrusive<ASTLiteral>(Field{}),
                    make_intrusive<ASTIdentifier>("k"))),
            make_intrusive<ASTIdentifier>(ColumnNames::SampleKinds),
            make_intrusive<ASTIdentifier>(ColumnNames::Values),
            make_intrusive<ASTIdentifier>(ColumnNames::HistogramValues)));
        kinds_builder.select_list.back()->setAlias(ColumnNames::SampleKinds);

        context.subqueries.emplace_back(context.subqueries.size(), std::move(histogram_grid.select_query), SQLSubqueryType::TABLE);
        kinds_builder.from_table = context.subqueries.back().name;
        context.subqueries.emplace_back(context.subqueries.size(), kinds_builder.getSelectQuery(), SQLSubqueryType::TABLE);

        /// SELECT group, arrayMap(x -> if(<x is a stale marker>, NULL, x), values) AS values,
        ///        arrayMap(x -> if(<x is a stale marker>, NULL, x), histogram_values) AS histogram_values, sample_kinds
        /// FROM <previous subquery>
        /// WHERE arrayExists(x -> isNotNull(x), sample_kinds)
        SelectQueryBuilder builder;
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
        builder.select_list.push_back(makeASTFunction(
            "arrayMap",
            make_lambda({"x"}, makeASTFunction("if", is_stale("x"), make_intrusive<ASTLiteral>(Field{}), make_intrusive<ASTIdentifier>("x"))),
            make_intrusive<ASTIdentifier>(ColumnNames::Values)));
        builder.select_list.back()->setAlias(ColumnNames::Values);
        builder.select_list.push_back(makeASTFunction(
            "arrayMap",
            make_lambda({"x"},
                makeASTFunction("if", is_stale_histogram("x"), make_intrusive<ASTLiteral>(Field{}), make_intrusive<ASTIdentifier>("x"))),
            make_intrusive<ASTIdentifier>(ColumnNames::HistogramValues)));
        builder.select_list.back()->setAlias(ColumnNames::HistogramValues);
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::SampleKinds));
        builder.from_table = context.subqueries.back().name;

        /// A row whose every step has no sample represents no series at all, see replaceStaleMarkersWithNulls().
        builder.where = makeASTFunction(
            "arrayExists",
            make_lambda({"x"}, makeASTFunction("isNotNull", make_intrusive<ASTIdentifier>("x"))),
            make_intrusive<ASTIdentifier>(ColumnNames::SampleKinds));

        histogram_grid.select_query = builder.getSelectQuery();
        return std::move(histogram_grid);
    }

    SQLQueryPiece replaceStaleMarkersWithNulls(SQLQueryPiece && vector_grid, ConverterContext & context)
    {
        if (vector_grid.store_method == StoreMethod::HISTOGRAM_GRID)
            return replaceStaleMarkersWithNullsInHistogramGrid(std::move(vector_grid), context);

        if (vector_grid.store_method != StoreMethod::VECTOR_GRID)
            return std::move(vector_grid);

        /// Instant selectors must let last_over_time observe the stale marker first.
        /// Once it is selected as the newest sample, turn it into absence before
        /// aggregations, vector matching, scalar(), absent(), or finalization see it.
        SelectQueryBuilder builder;
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

        const String iterator_name = "x";
        ASTPtr is_stale_marker = makeASTFunction(
            "and",
            makeASTFunction("isNotNull", make_intrusive<ASTIdentifier>(iterator_name)),
            isStaleMarker(
                makeASTFunction(
                    "assumeNotNull",
                    make_intrusive<ASTIdentifier>(iterator_name))));

        ASTPtr value = makeASTFunction(
            "if",
            std::move(is_stale_marker),
            make_intrusive<ASTLiteral>(Field{}),
            make_intrusive<ASTIdentifier>(iterator_name));

        auto values = makeASTFunction(
            "arrayMap",
            makeASTFunction(
                "lambda",
                makeASTFunction("tuple", make_intrusive<ASTIdentifier>(iterator_name)),
                std::move(value)),
            make_intrusive<ASTIdentifier>(ColumnNames::Values));
        values->setAlias(ColumnNames::Values);
        builder.select_list.push_back(std::move(values));

        context.subqueries.emplace_back(
            context.subqueries.size(),
            std::move(vector_grid.select_query),
            SQLSubqueryType::TABLE);
        builder.from_table = context.subqueries.back().name;

        /// A vector-grid row whose every step is NULL represents no series at all.
        /// Drop it here so downstream operators which validate uniqueness by row count
        /// don't mistake a stale series for a duplicate.
        context.subqueries.emplace_back(
            context.subqueries.size(),
            builder.getSelectQuery(),
            SQLSubqueryType::TABLE);

        SelectQueryBuilder filter_builder;
        filter_builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
        filter_builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));
        filter_builder.from_table = context.subqueries.back().name;

        const String filter_iterator_name = "x";
        filter_builder.where = makeASTFunction(
            "arrayExists",
            makeASTFunction(
                "lambda",
                makeASTFunction("tuple", make_intrusive<ASTIdentifier>(filter_iterator_name)),
                makeASTFunction("isNotNull", make_intrusive<ASTIdentifier>(filter_iterator_name))),
            make_intrusive<ASTIdentifier>(ColumnNames::Values));

        vector_grid.select_query = filter_builder.getSelectQuery();
        return std::move(vector_grid);
    }
}


SQLQueryPiece fromSelector(const PrometheusQueryTree::InstantSelector * instant_selector_node, ConverterContext & context)
{
    auto instant_selector_text = instant_selector_node->toString(*context.promql_tree);
    auto range_selector = fromRangeSelector(
        instant_selector_text, instant_selector_node, /* filter_stale_markers = */ false, context);
    auto vector_grid = applyFunctionOverRange(
        instant_selector_node, "last_over_time", {std::move(range_selector)}, context);
    return replaceStaleMarkersWithNulls(std::move(vector_grid), context);
}


SQLQueryPiece fromSelectorSampleTimestamps(const PrometheusQueryTree::InstantSelector * instant_selector_node, ConverterContext & context)
{
    auto instant_selector_text = instant_selector_node->toString(*context.promql_tree);
    auto range_selector = fromRangeSelector(
        instant_selector_text, instant_selector_node, /* filter_stale_markers = */ false, context);

    /// Replaces the value of every sample with its timestamp. Stale markers are kept so that last_over_time() can still
    /// select them and they are turned into absence below, exactly as for a plain instant selector.
    /// The new query reads the query of the range selector as a subquery.
    switch (range_selector.store_method)
    {
        case StoreMethod::SAMPLES:
        {
            /// SELECT group,
            ///        arrayMap(sample -> (sample.1, if(<sample.2 is a stale marker>, CAST(sample.2, 'Float64'), CAST(sample.1, 'Float64'))), samples) AS samples
            /// FROM <range_selector>
            context.subqueries.emplace_back(context.subqueries.size(), std::move(range_selector.select_query), SQLSubqueryType::TABLE);

            SelectQueryBuilder builder;
            builder.from_table = context.subqueries.back().name;
            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

            const String iterator_name = "sample";
            auto sample_timestamp = [&] { return makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>(iterator_name), make_intrusive<ASTLiteral>(UInt64{1})); };
            auto sample_value = [&] { return makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>(iterator_name), make_intrusive<ASTLiteral>(UInt64{2})); };

            ASTPtr new_value = makeASTFunction(
                "if",
                isStaleMarker(sample_value()),
                makeASTFunction("CAST", sample_value(), make_intrusive<ASTLiteral>("Float64")),
                makeASTFunction("CAST", sample_timestamp(), make_intrusive<ASTLiteral>("Float64")));

            auto samples = makeASTFunction(
                "arrayMap",
                makeASTFunction(
                    "lambda",
                    makeASTFunction("tuple", make_intrusive<ASTIdentifier>(iterator_name)),
                    makeASTFunction("tuple", sample_timestamp(), std::move(new_value))),
                make_intrusive<ASTIdentifier>(ColumnNames::Samples));
            samples->setAlias(ColumnNames::Samples);
            builder.select_list.push_back(std::move(samples));

            range_selector.select_query = builder.getSelectQuery();
            break;
        }

        case StoreMethod::TIMESTAMP_AND_VALUE:
        {
            /// SELECT group, timestamp,
            ///        if(<value is a stale marker>, CAST(value, 'Float64'), CAST(timestamp, 'Float64')) AS value
            /// FROM <range_selector>
            context.subqueries.emplace_back(context.subqueries.size(), std::move(range_selector.select_query), SQLSubqueryType::TABLE);

            SelectQueryBuilder builder;
            builder.from_table = context.subqueries.back().name;
            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Timestamp));

            ASTPtr value = makeASTFunction(
                "if",
                isStaleMarker(make_intrusive<ASTIdentifier>(ColumnNames::Value)),
                makeASTFunction("CAST", make_intrusive<ASTIdentifier>(ColumnNames::Value), make_intrusive<ASTLiteral>("Float64")),
                makeASTFunction("CAST", make_intrusive<ASTIdentifier>(ColumnNames::Timestamp), make_intrusive<ASTLiteral>("Float64")));
            value->setAlias(ColumnNames::Value);
            builder.select_list.push_back(std::move(value));

            range_selector.select_query = builder.getSelectQuery();
            break;
        }

        case StoreMethod::HISTOGRAM_RAW_DATA:
        {
            /// As TIMESTAMP_AND_VALUE without the payloads: a histogram sample yields its timestamp too, as in Prometheus,
            /// and a histogram stale marker becomes a float one.
            context.subqueries.emplace_back(context.subqueries.size(), std::move(range_selector.select_query), SQLSubqueryType::TABLE);

            SelectQueryBuilder builder;
            builder.from_table = context.subqueries.back().name;
            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Timestamp));

            ASTPtr value = makeASTFunction(
                "if",
                makeASTFunction(
                    "or",
                    isStaleMarker(make_intrusive<ASTIdentifier>(ColumnNames::Value)),
                    isStaleHistogramMarker(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Flags))),
                makeASTFunction("reinterpretAsFloat64", make_intrusive<ASTLiteral>(STALE_NAN_BITS)),
                makeASTFunction("CAST", make_intrusive<ASTIdentifier>(ColumnNames::Timestamp), make_intrusive<ASTLiteral>("Float64")));
            value->setAlias(ColumnNames::Value);
            builder.select_list.push_back(std::move(value));

            range_selector.select_query = builder.getSelectQuery();
            range_selector.store_method = StoreMethod::TIMESTAMP_AND_VALUE;
            break;
        }

        case StoreMethod::EMPTY:
        {
            /// No samples, nothing to replace.
            break;
        }

        case StoreMethod::CONST_SCALAR:
        case StoreMethod::CONST_STRING:
        case StoreMethod::SINGLE_SCALAR:
        case StoreMethod::SCALAR_GRID:
        case StoreMethod::VECTOR_GRID:
        case StoreMethod::HISTOGRAM_GRID:
        {
            /// Can't get in here because fromRangeSelector() returns only EMPTY, SAMPLES, TIMESTAMP_AND_VALUE or HISTOGRAM_RAW_DATA.
            throwUnexpectedStoreMethod(range_selector, context);
        }
    }

    auto vector_grid = applyFunctionOverRange(
        instant_selector_node, "last_over_time", {std::move(range_selector)}, context);
    return replaceStaleMarkersWithNulls(std::move(vector_grid), context);
}


SQLQueryPiece fromSelector(const PrometheusQueryTree::RangeSelector * range_selector_node, ConverterContext & context)
{
    auto instant_selector_text = range_selector_node->getInstantSelector()->toString(*context.promql_tree);
    return fromRangeSelector(
        instant_selector_text, range_selector_node, /* filter_stale_markers = */ true, context);
}

}
