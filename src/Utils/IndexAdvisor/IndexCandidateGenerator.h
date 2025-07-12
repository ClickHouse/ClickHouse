#pragma once

#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/getTableExpressions.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Common/logger_useful.h>
#include <memory>
#include <vector>

namespace DB
{

/// Represents a potential index candidate
struct IndexCandidate
{
    String table_name;
    String column_name;
    String index_type;
    String reason;
    double score;
    size_t baseline_rows = 0;
    size_t baseline_parts = 0;
    size_t indexed_rows = 0;
    size_t indexed_parts = 0;
    size_t index_size = 0;

    IndexCandidate(String table_name_, String column_name_, String index_type_, String reason_, double score_)
        : table_name(std::move(table_name_))
        , column_name(std::move(column_name_))
        , index_type(std::move(index_type_))
        , reason(std::move(reason_))
        , score(score_)
    {}
};

/// Base class for all index candidate rules
class IndexCandidateRule
{
public:
    virtual ~IndexCandidateRule() = default;
    
    /// Get the name of the rule
    virtual String getName() const = 0;
    
    /// Get the score for this rule
    virtual double getScore() const = 0;
    
    /// Apply the rule to generate candidates
    virtual std::vector<IndexCandidate> apply(
        const ASTPtr & query_ast,
        ContextMutablePtr context,
        const String & table_name) = 0;
        
protected:
    /// Helper method to process an identifier and add it as a candidate
    void processIdentifier(
        const ASTIdentifier * identifier,
        const String & table_name,
        std::vector<IndexCandidate> & candidates,
        const String & rule_name,
        double score) const;
};

/// Rule for analyzing WHERE clause
class WhereClauseRule : public IndexCandidateRule
{
public:
    String getName() const override { return "WHERE clause"; }
    double getScore() const override { return 1.0; }
    
    std::vector<IndexCandidate> apply(
        const ASTPtr & query_ast,
        ContextMutablePtr context,
        const String & table_name) override;
        
private:
    void analyzeWhereClause(
        const ASTPtr & where_clause,
        const String & table_name,
        std::vector<IndexCandidate> & candidates);
};

/// Rule for analyzing JOIN conditions
class JoinConditionRule : public IndexCandidateRule
{
public:
    String getName() const override { return "JOIN condition"; }
    double getScore() const override { return 0.9; }
    
    std::vector<IndexCandidate> apply(
        const ASTPtr & query_ast,
        ContextMutablePtr context,
        const String & table_name) override;
};

/// Rule for analyzing ORDER BY clause
class OrderByRule : public IndexCandidateRule
{
public:
    String getName() const override { return "ORDER BY clause"; }
    double getScore() const override { return 0.8; }
    
    std::vector<IndexCandidate> apply(
        const ASTPtr & query_ast,
        ContextMutablePtr context,
        const String & table_name) override;
        
private:
    void analyzeOrderByClause(
        const ASTPtr & order_by_clause,
        const String & table_name,
        std::vector<IndexCandidate> & candidates);
};

/// Rule for analyzing GROUP BY clause
class GroupByRule : public IndexCandidateRule
{
public:
    String getName() const override { return "GROUP BY clause"; }
    double getScore() const override { return 0.7; }
    
    std::vector<IndexCandidate> apply(
        const ASTPtr & query_ast,
        ContextMutablePtr context,
        const String & table_name) override;
        
private:
    void analyzeGroupByClause(
        const ASTPtr & group_by_clause,
        const String & table_name,
        std::vector<IndexCandidate> & candidates);
};

/// Rule for analyzing HAVING clause
class HavingRule : public IndexCandidateRule
{
public:
    String getName() const override { return "HAVING clause"; }
    double getScore() const override { return 0.6; }
    
    std::vector<IndexCandidate> apply(
        const ASTPtr & query_ast,
        ContextMutablePtr context,
        const String & table_name) override;
        
private:
    void analyzeHavingClause(
        const ASTPtr & having_clause,
        const String & table_name,
        std::vector<IndexCandidate> & candidates);
};

/// Interface for index candidate generators
class IIndexCandidateGenerator
{
public:
    virtual ~IIndexCandidateGenerator() = default;
    
    /// Generate index candidates from a query
    virtual std::vector<IndexCandidate> generateCandidates(
        const ASTPtr & query_ast,
        ContextMutablePtr context) = 0;
};

/// Rule-based implementation of index candidate generator
class RuleBasedCandidateGenerator : public IIndexCandidateGenerator
{
public:
    RuleBasedCandidateGenerator();
    
    std::vector<IndexCandidate> generateCandidates(
        const ASTPtr & query_ast,
        ContextMutablePtr context) override;
        
private:
    std::vector<std::unique_ptr<IndexCandidateRule>> rules;

    /// Process a SELECT query to generate candidates
    std::vector<IndexCandidate> processSelectQuery(
        const ASTSelectQuery & select_query,
        const ASTPtr & query_ast,
        ContextMutablePtr context);
};

} 
