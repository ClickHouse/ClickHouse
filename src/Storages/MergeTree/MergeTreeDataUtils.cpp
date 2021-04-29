#include <Storages/MergeTree/MergeTreeDataUtils.h>

#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectQuery.h>

namespace DB
{

bool getQueryProcessingStageWithAggregateProjection(
    ContextPtr query_context, const StorageMetadataPtr & metadata_snapshot, SelectQueryInfo & query_info)
{
    const auto & settings = query_context->getSettingsRef();
    if (!settings.allow_experimental_projection_optimization || query_info.ignore_projections)
        return false;

    const auto & query_ptr = query_info.query;

    InterpreterSelectQuery select(
        query_ptr, query_context, SelectQueryOptions{QueryProcessingStage::WithMergeableState}.ignoreProjections().ignoreAlias());
    const auto & analysis_result = select.getAnalysisResult();

    bool can_use_aggregate_projection = true;
    /// If the first stage of the query pipeline is more complex than Aggregating - Expression - Filter - ReadFromStorage,
    /// we cannot use aggregate projection.
    if (analysis_result.join != nullptr || analysis_result.array_join != nullptr)
        can_use_aggregate_projection = false;

    /// Check if all needed columns can be provided by some aggregate projection. Here we also try
    /// to find expression matches. For example, suppose an aggregate projection contains a column
    /// named sum(x) and the given query also has an expression called sum(x), it's a match. This is
    /// why we need to ignore all aliases during projection creation and the above query planning.
    /// It's also worth noting that, sqrt(sum(x)) will also work because we can treat sum(x) as a
    /// required column.

    /// The ownership of ProjectionDescription is hold in metadata_snapshot which lives along with
    /// InterpreterSelect, thus we can store the raw pointer here.
    std::vector<ProjectionCandidate> candidates;
    NameSet keys;
    std::unordered_map<std::string_view, size_t> key_name_pos_map;
    size_t pos = 0;
    for (const auto & desc : select.getQueryAnalyzer()->aggregationKeys())
    {
        keys.insert(desc.name);
        key_name_pos_map.insert({desc.name, pos++});
    }

    // All required columns should be provided by either current projection or previous actions
    // Let's traverse backward to finish the check.
    // TODO what if there is a column with name sum(x) and an aggregate sum(x)?
    auto rewrite_before_where =
        [&](ProjectionCandidate & candidate, const ProjectionDescription & projection,
            NameSet & required_columns, const Block & source_block, const Block & aggregates)
    {
        if (analysis_result.before_where)
        {
            candidate.before_where = analysis_result.before_where->clone();
            required_columns = candidate.before_where->foldActionsByProjection(
                required_columns,
                projection.sample_block_for_keys,
                query_ptr->as<const ASTSelectQuery &>().where()->getColumnName());
            if (required_columns.empty())
                return false;
            candidate.before_where->addAggregatesViaProjection(aggregates);
        }

        if (analysis_result.prewhere_info)
        {
            auto & prewhere_info = analysis_result.prewhere_info;
            candidate.prewhere_info = std::make_shared<PrewhereInfo>();
            candidate.prewhere_info->prewhere_column_name = prewhere_info->prewhere_column_name;
            candidate.prewhere_info->remove_prewhere_column = prewhere_info->remove_prewhere_column;
            candidate.prewhere_info->row_level_column_name = prewhere_info->row_level_column_name;
            candidate.prewhere_info->need_filter = prewhere_info->need_filter;

            auto actions_settings = ExpressionActionsSettings::fromSettings(query_context->getSettingsRef());
            auto prewhere_actions = prewhere_info->prewhere_actions->clone();
            NameSet prewhere_required_columns;
            prewhere_required_columns = prewhere_actions->foldActionsByProjection(
                prewhere_required_columns, projection.sample_block_for_keys, prewhere_info->prewhere_column_name);
            if (prewhere_required_columns.empty())
                return false;
            candidate.prewhere_info->prewhere_actions = std::make_shared<ExpressionActions>(prewhere_actions, actions_settings);

            if (prewhere_info->row_level_filter_actions)
            {
                auto row_level_filter_actions = prewhere_info->row_level_filter_actions->clone();
                prewhere_required_columns = row_level_filter_actions->foldActionsByProjection(
                    prewhere_required_columns, projection.sample_block_for_keys, prewhere_info->row_level_column_name);
                if (prewhere_required_columns.empty())
                    return false;
                candidate.prewhere_info->row_level_filter
                    = std::make_shared<ExpressionActions>(row_level_filter_actions, actions_settings);
            }

            if (prewhere_info->alias_actions)
            {
                auto alias_actions = prewhere_info->alias_actions->clone();
                prewhere_required_columns
                    = alias_actions->foldActionsByProjection(prewhere_required_columns, projection.sample_block_for_keys);
                if (prewhere_required_columns.empty())
                    return false;
                candidate.prewhere_info->alias_actions = std::make_shared<ExpressionActions>(alias_actions, actions_settings);
            }
            required_columns.insert(prewhere_required_columns.begin(), prewhere_required_columns.end());
        }

        bool match = true;
        for (const auto & column : required_columns)
        {
            /// There are still missing columns, fail to match
            if (!source_block.has(column))
            {
                match = false;
                break;
            }
        }
        return match;
    };

    for (const auto & projection : metadata_snapshot->projections)
    {
        ProjectionCandidate candidate{};
        candidate.desc = &projection;

        if (projection.type == ProjectionDescription::Type::Aggregate && analysis_result.need_aggregate && can_use_aggregate_projection)
        {
            bool match = true;
            Block aggregates;
            // Let's first check if all aggregates are provided by current projection
            for (const auto & aggregate : select.getQueryAnalyzer()->aggregates())
            {
                const auto * column = projection.sample_block.findByName(aggregate.column_name);
                if (column)
                {
                    aggregates.insert(*column);
                }
                else
                {
                    match = false;
                    break;
                }
            }

            if (!match)
                continue;

            // Check if all aggregation keys can be either provided by some action, or by current
            // projection directly. Reshape the `before_aggregation` action DAG so that it only
            // needs to provide aggregation keys, and certain children DAG might be substituted by
            // some keys in projection.
            candidate.before_aggregation = analysis_result.before_aggregation->clone();
            auto required_columns = candidate.before_aggregation->foldActionsByProjection(keys, projection.sample_block_for_keys);

            if (required_columns.empty())
                continue;

            // Reorder aggregation keys and attach aggregates
            candidate.before_aggregation->reorderAggregationKeysForProjection(key_name_pos_map);
            candidate.before_aggregation->addAggregatesViaProjection(aggregates);

            if (rewrite_before_where(candidate, projection, required_columns, projection.sample_block_for_keys, aggregates))
            {
                candidate.required_columns = {required_columns.begin(), required_columns.end()};
                for (const auto & aggregate : aggregates)
                    candidate.required_columns.push_back(aggregate.name);
                candidates.push_back(std::move(candidate));
            }
        }

        if (projection.type == ProjectionDescription::Type::Normal && (analysis_result.hasWhere() || analysis_result.hasPrewhere()))
        {
            NameSet required_columns;
            if (analysis_result.hasWhere())
            {
                for (const auto & column : analysis_result.before_where->getResultColumns())
                    required_columns.insert(column.name);
            }
            else
            {
                for (const auto & column : analysis_result.prewhere_info->prewhere_actions->getResultColumns())
                    required_columns.insert(column.name);
            }
            if (rewrite_before_where(candidate, projection, required_columns, projection.sample_block, {}))
            {
                candidate.required_columns = {required_columns.begin(), required_columns.end()};
                candidates.push_back(std::move(candidate));
            }
        }
    }

    // Let's select the best aggregate projection to execute the query.
    if (!candidates.empty())
    {
        size_t min_key_size = std::numeric_limits<size_t>::max();
        ProjectionCandidate * selected_candidate = nullptr;
        /// Favor aggregate projections
        for (auto & candidate : candidates)
        {
            // TODO We choose the projection with least key_size. Perhaps we can do better? (key rollups)
            if (candidate.desc->type == ProjectionDescription::Type::Aggregate && candidate.desc->key_size < min_key_size)
            {
                selected_candidate = &candidate;
                min_key_size = candidate.desc->key_size;
            }
        }

        /// TODO Select the best normal projection if no aggregate projection is available
        if (!selected_candidate)
        {
            for (auto & candidate : candidates)
                selected_candidate = &candidate;
        }

        if (!selected_candidate)
            return false;

        if (selected_candidate->desc->type == ProjectionDescription::Type::Aggregate)
        {
            selected_candidate->aggregation_keys = select.getQueryAnalyzer()->aggregationKeys();
            selected_candidate->aggregate_descriptions = select.getQueryAnalyzer()->aggregates();
        }
        query_info.projection = std::move(*selected_candidate);

        return true;
    }
    return false;
}
}
