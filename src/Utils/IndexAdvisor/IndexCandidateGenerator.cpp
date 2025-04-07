#include <Utils/IndexAdvisor/IndexCandidateGenerator.h>
#include <fmt/format.h>
#include <Parsers/ASTSelectWithUnionQuery.h>

namespace DB
{

void IndexCandidateRule::processIdentifier(
    const ASTIdentifier * identifier,
    const String & table_name,
    std::vector<IndexCandidate> & candidates,
    const String & rule_name,
    double score) const
{
    LOG_DEBUG(&Poco::Logger::get(rule_name), 
        "Processing identifier: name='{}', compound={}, name_parts size={}", 
        identifier->name(), identifier->compound(), identifier->name_parts.size());
        
    // Handle fully qualified column names (table.column)
    String column_name;
    String table_name_for_column = table_name;
    
    if (identifier->compound())
    {
        auto parts = identifier->name_parts;
        String parts_str;
        for (size_t i = 0; i < parts.size(); ++i)
        {
            if (i > 0)
                parts_str += ", ";
            parts_str += parts[i];
        }
        LOG_DEBUG(&Poco::Logger::get(rule_name), 
            "Compound identifier parts: [{}]", parts_str);
        
        if (!parts.empty())
        {
            // For "test_explain_estimate.name", parts would be ["test_explain_estimate", "name"]
            table_name_for_column = parts[0];  // First part is the table name
            if (parts.size() > 1)
            {
                column_name = parts[1];  // Second part is the column name
            }
        }
    }
    else
    {
        column_name = identifier->name();
    }
    
    LOG_DEBUG(&Poco::Logger::get(rule_name), 
        "Extracted: table='{}', column='{}'", 
        table_name_for_column, column_name);
    
    // Only add candidate if the column belongs to the table we're analyzing
    if (table_name_for_column == table_name)
    {
        LOG_DEBUG(&Poco::Logger::get(rule_name), 
            "Adding candidate for table '{}', column '{}'", 
            table_name, column_name);
        candidates.emplace_back(table_name, column_name, "minmax", rule_name, score);
    }
}

std::vector<IndexCandidate> WhereClauseRule::apply(
    const ASTPtr & query_ast,
    ContextMutablePtr /* context */,
    const String & table_name)
{
    std::vector<IndexCandidate> candidates;
    
    if (const auto * select = query_ast->as<ASTSelectQuery>())
    {
        if (select->where())
        {
            LOG_DEBUG(&Poco::Logger::get(getName()), 
                "Analyzing WHERE clause for table: {}", table_name);
            analyzeWhereClause(select->where(), table_name, candidates);
        }
    }
    
    return candidates;
}

void WhereClauseRule::analyzeWhereClause(
    const ASTPtr & where_clause,
    const String & table_name,
    std::vector<IndexCandidate> & candidates)
{
    if (!where_clause)
        return;
        
    if (const auto * func = where_clause->as<ASTFunction>())
    {
        LOG_DEBUG(&Poco::Logger::get(getName()), 
            "Found function in WHERE clause: {}", func->name);
        
        // Handle both 'equals' and '=' operators
        if (func->name == "equals" || func->name == "=")
        {
            if (func->arguments && func->arguments->children.size() == 2)
            {
                LOG_DEBUG(&Poco::Logger::get(getName()), 
                    "Found equality condition with 2 arguments");
                
                for (const auto & arg : func->arguments->children)
                {
                    if (const auto * identifier = arg->as<ASTIdentifier>())
                    {
                        processIdentifier(identifier, table_name, candidates, getName(), getScore());
                    }
                }
            }
        }
        else if (func->name == "greater" || func->name == "less" ||
                 func->name == "greaterOrEquals" || func->name == "lessOrEquals")
        {
            if (func->arguments && func->arguments->children.size() == 2)
            {
                LOG_DEBUG(&Poco::Logger::get(getName()), 
                    "Found comparison condition with 2 arguments");
                
                for (const auto & arg : func->arguments->children)
                {
                    if (const auto * identifier = arg->as<ASTIdentifier>())
                    {
                        processIdentifier(identifier, table_name, candidates, getName(), getScore());
                    }
                }
            }
        }
    }
    // Also handle direct column references in WHERE clause
    else if (const auto * identifier = where_clause->as<ASTIdentifier>())
    {
        LOG_DEBUG(&Poco::Logger::get(getName()), 
            "Found direct column reference in WHERE clause: {}", identifier->name());
        processIdentifier(identifier, table_name, candidates, getName(), getScore());
    }
}

std::vector<IndexCandidate> JoinConditionRule::apply(
    const ASTPtr & query_ast,
    ContextMutablePtr /* context */,
    const String & table_name)
{
    std::vector<IndexCandidate> candidates;
    
    if (const auto * select = query_ast->as<ASTSelectQuery>())
    {
        if (select->join())
        {
            LOG_DEBUG(&Poco::Logger::get(getName()), 
                "Analyzing JOIN conditions for table: {}", table_name);
            // Get the join conditions from the join element
            if (const auto * join_element = select->join()->as<ASTTablesInSelectQueryElement>())
            {
                if (join_element->table_join)
                {
                    // The join conditions are in the children of table_join
                    for (const auto & child : join_element->table_join->children)
                    {
                        if (const auto * func = child->as<ASTFunction>())
                        {
                            if (func->name == "equals" || func->name == "=")
                            {
                                if (func->arguments && func->arguments->children.size() == 2)
                                {
                                    LOG_DEBUG(&Poco::Logger::get(getName()), 
                                        "Found equality condition in JOIN with 2 arguments");
                                    
                                    for (const auto & arg : func->arguments->children)
                                    {
                                        if (const auto * identifier = arg->as<ASTIdentifier>())
                                        {
                                            processIdentifier(identifier, table_name, candidates, getName(), getScore());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    
    return candidates;
}

std::vector<IndexCandidate> OrderByRule::apply(
    const ASTPtr & query_ast,
    ContextMutablePtr /* context */,
    const String & table_name)
{
    std::vector<IndexCandidate> candidates;
    
    if (const auto * select = query_ast->as<ASTSelectQuery>())
    {
        if (select->orderBy())
        {
            LOG_DEBUG(&Poco::Logger::get(getName()), 
                "Analyzing ORDER BY clause for table: {}", table_name);
            analyzeOrderByClause(select->orderBy(), table_name, candidates);
        }
    }
    
    return candidates;
}

void OrderByRule::analyzeOrderByClause(
    const ASTPtr & order_by_clause,
    const String & table_name,
    std::vector<IndexCandidate> & candidates)
{
    if (!order_by_clause)
        return;
        
    for (const auto & child : order_by_clause->children)
    {
        if (const auto * identifier = child->as<ASTIdentifier>())
        {
            LOG_DEBUG(&Poco::Logger::get(getName()), 
                "Found column in ORDER BY: {}", identifier->name());
            processIdentifier(identifier, table_name, candidates, getName(), getScore());
        }
    }
}

std::vector<IndexCandidate> GroupByRule::apply(
    const ASTPtr & query_ast,
    ContextMutablePtr /* context */,
    const String & table_name)
{
    std::vector<IndexCandidate> candidates;
    
    if (const auto * select = query_ast->as<ASTSelectQuery>())
    {
        if (select->groupBy())
        {
            LOG_DEBUG(&Poco::Logger::get(getName()), 
                "Analyzing GROUP BY clause for table: {}", table_name);
            analyzeGroupByClause(select->groupBy(), table_name, candidates);
        }
    }
    
    return candidates;
}

void GroupByRule::analyzeGroupByClause(
    const ASTPtr & group_by_clause,
    const String & table_name,
    std::vector<IndexCandidate> & candidates)
{
    if (!group_by_clause)
        return;
        
    for (const auto & child : group_by_clause->children)
    {
        if (const auto * identifier = child->as<ASTIdentifier>())
        {
            LOG_DEBUG(&Poco::Logger::get(getName()), 
                "Found column in GROUP BY: {}", identifier->name());
            processIdentifier(identifier, table_name, candidates, getName(), getScore());
        }
    }
}

std::vector<IndexCandidate> HavingRule::apply(
    const ASTPtr & query_ast,
    ContextMutablePtr /* context */,
    const String & table_name)
{
    std::vector<IndexCandidate> candidates;
    
    if (const auto * select = query_ast->as<ASTSelectQuery>())
    {
        if (select->having())
        {
            LOG_DEBUG(&Poco::Logger::get(getName()), 
                "Analyzing HAVING clause for table: {}", table_name);
            analyzeHavingClause(select->having(), table_name, candidates);
        }
    }
    
    return candidates;
}

void HavingRule::analyzeHavingClause(
    const ASTPtr & having_clause,
    const String & table_name,
    std::vector<IndexCandidate> & candidates)
{
    if (!having_clause)
        return;
        
    if (const auto * func = having_clause->as<ASTFunction>())
    {
        LOG_DEBUG(&Poco::Logger::get(getName()), 
            "Found function in HAVING clause: {}", func->name);
        
        if (func->arguments)
        {
            for (const auto & arg : func->arguments->children)
            {
                if (const auto * identifier = arg->as<ASTIdentifier>())
                {
                    LOG_DEBUG(&Poco::Logger::get(getName()), 
                        "Found column in HAVING: {}", identifier->name());
                    processIdentifier(identifier, table_name, candidates, getName(), getScore());
                }
            }
        }
    }
    else if (const auto * identifier = having_clause->as<ASTIdentifier>())
    {
        LOG_DEBUG(&Poco::Logger::get(getName()), 
            "Found direct column reference in HAVING: {}", identifier->name());
        processIdentifier(identifier, table_name, candidates, getName(), getScore());
    }
}

RuleBasedCandidateGenerator::RuleBasedCandidateGenerator()
{
    // Add all rules
    rules.push_back(std::make_unique<WhereClauseRule>());
    rules.push_back(std::make_unique<JoinConditionRule>());
    rules.push_back(std::make_unique<OrderByRule>());
    rules.push_back(std::make_unique<GroupByRule>());
    rules.push_back(std::make_unique<HavingRule>());
}

std::vector<IndexCandidate> RuleBasedCandidateGenerator::generateCandidates(
    const ASTPtr & query_ast,
    ContextMutablePtr context)
{
    if (!query_ast)
        return {};
        
    if (const auto * select_query = query_ast->as<ASTSelectQuery>())
    {
        return processSelectQuery(*select_query, query_ast, context);
    }
    else if (const auto * union_query = query_ast->as<ASTSelectWithUnionQuery>())
    {
        // Process each SELECT query in the UNION
        std::vector<IndexCandidate> all_candidates;
        for (const auto & child : union_query->list_of_selects->children)
        {
            if (const auto * child_select = child->as<ASTSelectQuery>())
            {
                auto candidates = processSelectQuery(*child_select, child, context);
                all_candidates.insert(
                    all_candidates.end(),
                    std::make_move_iterator(candidates.begin()),
                    std::make_move_iterator(candidates.end()));
            }
        }
        return all_candidates;
    }
    
    return {};
}

std::vector<IndexCandidate> RuleBasedCandidateGenerator::processSelectQuery(
    const ASTSelectQuery & select_query,
    const ASTPtr & query_ast,
    ContextMutablePtr context)
{
    std::vector<IndexCandidate> all_candidates;
    
    // Get table expressions
    auto table_expressions = getTableExpressions(select_query);
    
    // Get tables with columns
    auto tables_with_columns = getDatabaseAndTablesWithColumns(table_expressions, context, true, true);
    
    // Process each table
    for (const auto & table_with_columns : tables_with_columns)
    {
        const auto & table_name = table_with_columns.table.table;
        LOG_DEBUG(&Poco::Logger::get("RuleBasedCandidateGenerator"), 
            "Processing table: {}", table_name);
        
        // Apply each rule
        for (const auto & rule : rules)
        {
            LOG_DEBUG(&Poco::Logger::get("RuleBasedCandidateGenerator"), 
                "Applying rule: {}", rule->getName());
                
            auto rule_candidates = rule->apply(query_ast, context, table_name);
            all_candidates.insert(
                all_candidates.end(),
                std::make_move_iterator(rule_candidates.begin()),
                std::make_move_iterator(rule_candidates.end()));
        }
    }
    
    return all_candidates;
}

} 
