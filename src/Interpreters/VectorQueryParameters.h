#pragma once

#include <Core/Field.h>
#include <Core/Types.h>
#include <Interpreters/Cache/VectorQueryPlanCache.h>
#include <Parsers/IAST_fwd.h>

#include <string_view>
#include <vector>
#include <string>

namespace DB
{

class QueryPlan;

enum class FunctionNames 
{
    COSINEDISTANCE,
    L2DISTANCE,
    HASTOKEN,
    CAST
};

/// Offsets into AST/DAG path names for extracting metadata.
/// The path names follow a format where certain positions contain encoded information.
/// Offset::StepType points to the step type character in the path:
/// - 'E': Expression step (used in filter expressions)
/// - 'F': Filter step
/// - 'T': TextSearch step
/// The position 28 is derived from the standard ClickHouse DAG node naming format:
/// [20-char-node-id].StepType[additional-info]
/// where the prefix before StepType is typically 28 characters total.
enum class Offset : size_t { StepType = 28 };

struct PlanConstantCandidate
{
    VectorQueryPlanCache::PlanConstantBinding binding;
    Field value;
    String identifier_names;
    std::vector<String> function_names;
    String plan_function_name;
    Int32 step_type = -1;
    
    // Position information for fast constant replacement in expression strings
    size_t constant_start_pos = 0;  // Start position in the original expression string
    size_t constant_end_pos = 0;    // End position (exclusive) in the original expression string
};

/// Structure to hold enhanced parameter information including original string and parsed array elements
struct ParameterInfo
{
    String original_string;
    std::vector<String> string_array;
    enum class Type { STRING, NUMERIC, NUMERIC_VECTOR } type;
    
    ParameterInfo() : type(Type::STRING) {}
    explicit ParameterInfo(String str, Type param_type = Type::STRING) 
        : original_string(std::move(str)), type(param_type) {}
    ParameterInfo(String str, std::vector<String> arr, Type param_type) 
        : original_string(std::move(str)), string_array(std::move(arr)), type(param_type) {}
};

/** Query normalizer and literal extractor.
  *
  * The normalizer replaces all literals in a SQL query with placeholders ('?'),
  * producing a normalized form suitable for use as a cache key.
  *
  * The extractor collects those literals in the order they appear, so they can be
  * re-injected later (e.g., into a cloned AST or QueryPlan).
  */
class VectorQueryParameters
{
public:
    struct NormalizedQueryResult
    {
        /// Hash of normalized_sql. A value of zero means normalization was skipped
        /// or the query was not eligible for cache-oriented normalization.
        UInt64 hash;
        /// Null-terminated normalized SQL text used as the cache key payload.
        /// Normalized SQL text used as the cache key payload.
        String normalized_sql;
        /// Literal tokens extracted from the original SQL in replacement order.
        std::vector<ParameterInfo> params;
        /// Typed literal values parsed from params in the same order. This vector is
        /// populated lazily and then reused by AST and QueryPlan replacement logic.
        std::vector<Field> parsed_params;
        /// Rewritten SQL text returned to callers that need the original query text
        /// shape preserved while vector literals have been normalized into explicit
        /// CAST(...) expressions.
        String new_sql;
    };

    std::vector<VectorQueryPlanCache::ASTLiteralPosition> collectASTLiteralPositions(
        const ASTPtr & query_ast,
        bool only_vector = false) const;

    String rewriteVectorLiteralsToCasts(
        const char * begin,
        const char * end) const;

    std::vector<Field> buildParameterValuesFromAST(
        const ASTPtr & query_ast,
        const std::vector<VectorQueryPlanCache::ASTLiteralPosition> & positions);
    
    bool applyParametersByASTLiteralPositions(
        ASTPtr & query_ast,
        NormalizedQueryResult & parameters,
        const std::vector<VectorQueryPlanCache::ASTLiteralPosition> & positions) const;

    bool parseNormalizedParamsWithAST(
        NormalizedQueryResult & parameters,
        const std::vector<VectorQueryPlanCache::ASTLiteralPosition> * positions = nullptr,
        bool only_vector = false) const;
        
    bool parseNormalizedParamsWithPlan(
        NormalizedQueryResult & parameters,
        const std::vector<VectorQueryPlanCache::PlanConstantBinding> * plan_constant_bindings = nullptr,
        bool only_vector = false) const;

    void replaceConstantsInQueryPlan(
        QueryPlan & plan,
        NormalizedQueryResult & parameters,
        const std::vector<VectorQueryPlanCache::PlanConstantBinding> & plan_constant_bindings,
        bool only_vector = false);

    NormalizedQueryResult normalizeQueryAndExtractParams(
        const char * begin,
        const char * end,
        bool only_vector = false,
        bool is_cast = false);

    /// Scan a built QueryPlan and match every mutable constant slot back to the
    /// ordered AST literal metadata collected earlier for the same query.
    ///
    /// Unlike the older value-only collector, this version uses:
    /// - AST literal metadata (`function_list`, `ast_path_name`, `identifier_name`, `step_type`)
    /// - normalized runtime parameters (`params`, `parsed_params`)
    /// - plan-side structure (step scope, DAG node index, QueryInfo payload type)
    ///
    /// A binding is emitted only when one AST literal maps to exactly one plan-side
    /// constant location. If any literal cannot be matched, or matches multiple
    /// locations, the function logs the mismatch and returns an empty binding list.
    std::vector<VectorQueryPlanCache::PlanConstantBinding> CollectQueryPlanConstants(
        QueryPlan & query_plan,
        const std::vector<VectorQueryPlanCache::ASTLiteralPosition> & ast_literal_positions,
        const NormalizedQueryResult & parameters,
        bool only_vector = false);

};

}
