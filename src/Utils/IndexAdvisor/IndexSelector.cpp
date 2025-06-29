#include <Utils/IndexAdvisor/IndexSelector.h>
#include <algorithm>
#include <unordered_set>
#include <Common/Exception.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/Context.h>
#include <Common/logger_useful.h>
#include <Core/Field.h>
#include <Storages/IStorage.h>
#include <Databases/IDatabase.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>

namespace DB
{

namespace
{
    size_t calculateStorageSize(MergeTreeData * merge_tree)
    {
        auto parts = merge_tree->getDataPartsForInternalUsage();
        LOG_DEBUG(&Poco::Logger::get("IndexSelector"), "Active parts count: {}", parts.size());
        size_t total_bytes = 0;
        for (const auto & part : parts)
        {
            total_bytes += part->getBytesOnDisk();
            LOG_DEBUG(&Poco::Logger::get("IndexSelector"), "Part: {}, bytes_on_disk: {}", part->name, part->getBytesOnDisk());
        }
        LOG_DEBUG(&Poco::Logger::get("IndexSelector"), "Total calculated storage size: {} bytes", total_bytes);
        return total_bytes;
    }

    ASTPtr extractSelectQuery(const ASTPtr & query_ast)
    {
        if (!query_ast)
        {
            return nullptr;
        }
        if (query_ast->as<ASTSelectWithUnionQuery>())
        {
            return query_ast;
        }
        if (query_ast->as<ASTSelectQuery>())
        {
            auto union_query = std::make_shared<ASTSelectWithUnionQuery>();
            auto list = std::make_shared<ASTExpressionList>();
            list->children.push_back(query_ast);
            union_query->list_of_selects = list;
            return union_query;
        }
        return nullptr;
    }
}

IndexBenefit HeuristicSearchIndexSelector::calculateIndexScore(
    const IndexCandidate & candidate,
    ContextMutablePtr context,
    const IndexMetrics & baseline_metrics,
    const ASTPtr & current_query_ast)
{
    try
    {
        // Set mutations to be sync
        context->setSetting("mutations_sync", 2);

        ASTPtr select_query = extractSelectQuery(current_query_ast);
        if (!select_query)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Only SELECT queries are supported for index analysis");

        LOG_DEBUG(&Poco::Logger::get("IndexSelector"), "Calculating index score for table: {}, column: {}", 
            candidate.table_name, candidate.column_name);

        // Get baseline storage size
        auto storage = DatabaseCatalog::instance().getTable({context->getCurrentDatabase(), candidate.table_name}, context);
        auto * merge_tree = dynamic_cast<MergeTreeData *>(storage.get());
        if (!merge_tree)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Table {} is not a MergeTree table", candidate.table_name);

        size_t baseline_bytes = calculateStorageSize(merge_tree);
        LOG_DEBUG(&Poco::Logger::get("IndexSelector"), "Baseline storage size: {} bytes", baseline_bytes);

        // Create the index
        std::string create_index_sql = "ALTER TABLE " + candidate.table_name + 
            " ADD INDEX " + candidate.column_name + "_idx " + candidate.column_name + " TYPE " + candidate.index_type;
        
        LOG_DEBUG(&Poco::Logger::get("IndexSelector"), "Creating index with SQL: {}", create_index_sql);
        
        ParserQuery create_parser(create_index_sql.data(), create_index_sql.data() + create_index_sql.size());
        ASTPtr create_index_ast = ::DB::parseQuery(create_parser, create_index_sql.data(), create_index_sql.data() + create_index_sql.size(), "create index", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
        auto create_interpreter = InterpreterFactory::instance().get(create_index_ast, context);
        create_interpreter->execute();

        // Materialize the index
        std::string materialize_sql = "ALTER TABLE " + candidate.table_name + 
            " MATERIALIZE INDEX " + candidate.column_name + "_idx";
        
        LOG_DEBUG(&Poco::Logger::get("IndexSelector"), "Materializing index with SQL: {}", materialize_sql);
        
        ParserQuery materialize_parser(materialize_sql.data(), materialize_sql.data() + materialize_sql.size());
        ASTPtr materialize_index_ast = ::DB::parseQuery(materialize_parser, materialize_sql.data(), materialize_sql.data() + materialize_sql.size(), "materialize index", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
        auto materialize_interpreter = InterpreterFactory::instance().get(materialize_index_ast, context);
        materialize_interpreter->execute();

        // Get index size and updated storage size
        auto index_sizes = merge_tree->getSecondaryIndexSizes();
        auto it = index_sizes.find(candidate.column_name + "_idx");
        if (it == index_sizes.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Index {} not found", candidate.column_name + "_idx");

        size_t indexed_bytes = calculateStorageSize(merge_tree);
        LOG_DEBUG(&Poco::Logger::get("IndexSelector"), "Indexed storage size: {} bytes", indexed_bytes);

        // Compute metrics with index
        LOG_DEBUG(&Poco::Logger::get("IndexSelector"), "Computing metrics with index");
        auto indexed_metrics = benefit_estimator->computeMetrics(select_query, context);
        LOG_DEBUG(&Poco::Logger::get("IndexSelector"), "Will calculate benefit from: baseline_metrics={{rows: {}, parts: {}}}, indexed_metrics={{rows: {}, parts: {}}}, baseline_bytes={}, indexed_bytes={}", 
            baseline_metrics.rows, baseline_metrics.parts, 
            indexed_metrics.rows, indexed_metrics.parts,
            baseline_bytes, indexed_bytes);
        auto benefit = benefit_estimator->computeBenefit(baseline_metrics, indexed_metrics, baseline_bytes, indexed_bytes);
        LOG_DEBUG(&Poco::Logger::get("IndexSelector"), "Calculated benefit: {}", benefit);

        // Drop the index
        std::string drop_index_sql = "ALTER TABLE " + candidate.table_name + 
            " DROP INDEX " + candidate.column_name + "_idx";
        
        LOG_DEBUG(&Poco::Logger::get("IndexSelector"), "Dropping index with SQL: {}", drop_index_sql);
        
        ParserQuery drop_parser(drop_index_sql.data(), drop_index_sql.data() + drop_index_sql.size());
        ASTPtr drop_index_ast = ::DB::parseQuery(drop_parser, drop_index_sql.data(), drop_index_sql.data() + drop_index_sql.size(), "drop index", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
        auto drop_interpreter = InterpreterFactory::instance().get(drop_index_ast, context);
        drop_interpreter->execute();

        return benefit;
    }
    catch (const Exception & e)
    {
        // Log error and return zero benefit for failed candidate
        LOG_ERROR(&Poco::Logger::get("IndexSelector"), "Error processing index candidate: {} (code: {}, message: {})", e.message(), e.code(), e.displayText());
        return 0.0;
    }
}

bool HeuristicSearchIndexSelector::selectIndex(
    size_t iteration,
    ASTPtr & current_query_ast,
    ContextMutablePtr context,
    IndexMetrics & baseline_metrics,
    std::unordered_set<std::string> & created_indexes,
    std::vector<IndexCandidate> & selected,
    std::vector<IndexCandidate> & candidates)
{
    // Remove candidates for already created indexes
    candidates.erase(
        std::remove_if(candidates.begin(), candidates.end(), [&](const IndexCandidate & c) {
            std::string key = c.table_name + "." + c.column_name;
            return created_indexes.contains(key);
        }),
        candidates.end());

    if (candidates.empty())
    {
        LOG_DEBUG(&Poco::Logger::get("IndexSelector"), "No more candidates after removing already created indexes");
        return false;
    }

    LOG_DEBUG(&Poco::Logger::get("IndexSelector"), "{} candidates remaining for iteration {}", candidates.size(), iteration);

    // Calculate benefits for remaining candidates
    std::vector<IndexBenefit> benefits;
    for (const auto & candidate : candidates)
    {
        benefits.push_back(calculateIndexScore(candidate, context, baseline_metrics, current_query_ast));
    }

    if (benefits.empty())
    {
        LOG_DEBUG(&Poco::Logger::get("IndexSelector"), "No benefits calculated");
        return false;
    }

    // Choose the best candidate
    size_t best_idx = benefit_estimator->chooseBest(benefits);
    double best_benefit = benefits[best_idx];
    
    LOG_DEBUG(&Poco::Logger::get("IndexSelector"), "Best benefit: {} at index {}", best_benefit, best_idx);
    
    if (best_benefit <= 0)
    {
        LOG_DEBUG(&Poco::Logger::get("IndexSelector"), "Best benefit is not positive");
        return false;
    }

    // Check if the improvement is significant
    if (iteration > 0 && benefit_estimator->isSmallDistance(best_benefit, benefits[0]))
    {
        LOG_DEBUG(&Poco::Logger::get("IndexSelector"), "Improvement is not significant");
        return false;
    }

    // Add the best candidate to selected indexes
    const auto & best_candidate = candidates[best_idx];
    std::string best_key = best_candidate.table_name + "." + best_candidate.column_name;
    created_indexes.insert(best_key);
    selected.push_back(best_candidate);

    LOG_DEBUG(&Poco::Logger::get("IndexSelector"), "Selected candidate: table={}, column={}", 
        best_candidate.table_name, best_candidate.column_name);

    // Update baseline metrics for next iteration
    baseline_metrics = benefit_estimator->computeMetrics(current_query_ast, context);
    return true;
}

std::vector<IndexCandidate> HeuristicSearchIndexSelector::selectIndexes(
    const ASTPtr & query_ast,
    ContextMutablePtr context,
    size_t max_index_count)
{
    ASTPtr select_query = extractSelectQuery(query_ast);
    if (!select_query)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Only SELECT queries are supported for index analysis");

    LOG_DEBUG(&Poco::Logger::get("IndexSelector"), "Starting index selection with max_count={}", max_index_count);

    // Generate all candidates first
    std::vector<IndexCandidate> candidates = candidate_generator->generateCandidates(select_query, context);
    if (candidates.empty())
    {
        LOG_DEBUG(&Poco::Logger::get("IndexSelector"), "No candidates generated");
        return {};
    }

    LOG_DEBUG(&Poco::Logger::get("IndexSelector"), "Generated {} candidates", candidates.size());
    for (const auto & candidate : candidates)
    {
        LOG_DEBUG(&Poco::Logger::get("IndexSelector"), "Candidate: table={}, column={}, index_type={}, reason={}", 
            candidate.table_name, candidate.column_name, candidate.index_type, candidate.reason);
    }

    std::vector<IndexCandidate> selected;
    std::unordered_set<std::string> created_indexes;
    ASTPtr current_query_ast = select_query->clone();

    // Get initial baseline metrics
    auto baseline_metrics = benefit_estimator->computeMetrics(select_query, context);
    LOG_DEBUG(&Poco::Logger::get("IndexSelector"), "Initial baseline metrics: rows={}, parts={}", baseline_metrics.rows, baseline_metrics.parts);

    for (size_t i = 0; i < max_index_count; ++i)
    {
        if (!selectIndex(i, current_query_ast, context, baseline_metrics, created_indexes, selected, candidates))
            break;
    }

    LOG_DEBUG(&Poco::Logger::get("IndexSelector"), "Selected {} indexes", selected.size());
    return selected;
}

} // namespace DB 
