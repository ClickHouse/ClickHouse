#pragma once

#include <Core/Field.h>
#include <Core/Types.h>
#include <Interpreters/Cache/VectorQueryPlanCache.h>
#include <Parsers/IAST_fwd.h>
#include <Common/SettingsChanges.h>

#include <string_view>
#include <vector>
#include <string>

namespace DB
{

class QueryPlan;

/// Enumeration of vector/search function names recognized by the query parameterizer.
/// These functions are the primary targets for vector query plan caching.
enum class FunctionNames
{
    COSINEDISTANCE,  ///< cosinedistance(vector1, vector2) — cosine similarity distance
    L2DISTANCE,      ///< l2distance(vector1, vector2) — Euclidean (L2) distance
    HASTOKEN,        ///< hastoken(column, token) — text search token matching
    CAST             ///< cast(expr, type) — explicit type cast, used to wrap vector literals
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

/// Intermediate candidate collected from a QueryPlan ActionsDAG during constant scanning.
/// Each candidate represents one mutable constant node found in an ExpressionStep or FilterStep.
/// After collection, candidates are matched against AST literal positions to produce
/// the final PlanConstantBinding list used for plan cache reuse.
struct PlanConstantCandidate
{
    VectorQueryPlanCache::PlanConstantBinding binding;  ///< Plan-level binding metadata (path, scope, node index)
    Field value;                                        ///< Current runtime value of the constant
    String identifier_names;                            ///< Column/identifier name from the parent function's INPUT child
    std::vector<String> function_names;                 ///< Chain of enclosing function names (innermost last)
    String plan_function_name;                          ///< Function name at the plan level (unused currently)
    Int32 step_type = -1;                               ///< Step type: 1=Expression, 2=Filter, 4=VectorExpression

    // Position information for fast constant replacement in expression strings
    size_t constant_start_pos = 0;  ///< Start position in the original expression string
    size_t constant_end_pos = 0;    ///< End position (exclusive) in the original expression string
};

/// Holds per-parameter metadata extracted during SQL normalization.
/// Tracks the original token text, parsed string array (for composite values),
/// and the classified parameter type (string, numeric, or numeric vector).
struct ParameterInfo
{
    String original_string;               ///< Raw token text from the original SQL
    std::vector<String> string_array;     ///< Decomposed elements (used for array/vector params)
    enum class Type { STRING, NUMERIC, NUMERIC_VECTOR } type;  ///< Classified parameter type

    ParameterInfo() : type(Type::STRING) {}
    explicit ParameterInfo(String str, Type param_type = Type::STRING)
        : original_string(std::move(str)), type(param_type) {}
    ParameterInfo(String str, std::vector<String> arr, Type param_type)
        : original_string(std::move(str)), string_array(std::move(arr)), type(param_type) {}
};

/** Query normalizer and literal extractor for vector query plan caching.
  *
  * This class provides two complementary capabilities:
  *
  * 1. **SQL normalization** — Replaces all (or vector-only) literals in a SQL query
  *    with placeholders ('?'), producing a normalized form suitable for use as a
  *    cache key.  The original literal values are preserved in the `params` vector
  *    so they can be re-injected into a cached AST or QueryPlan on cache hits.
  *
  * 2. **AST/QueryPlan constant binding** — Walks the AST or QueryPlan to collect
  *    constant positions, matches plan-side constants to AST-side literal positions,
  *    and rewrites cached plan constants with current runtime values.
  *
  * The overall flow is:
  *   normalizeQueryAndExtractParams()  →  cache lookup  →
  *   parseNormalizedParamsWithAST/Plan()  →  replaceConstantsInQueryPlan()
  */
class VectorQueryParameters
{
public:
    /// Result of normalizing a SQL query.  Contains the cache key (hash + normalized_sql),
    /// the extracted parameter stream, and optional rewritten SQL for CAST-based vector handling.
    struct NormalizedQueryResult
    {
        /// SipHash of normalized_sql. A value of zero means normalization was skipped
        /// or the query was not eligible for cache-oriented normalization.
        UInt64 hash = 0;
        /// Normalized SQL text used as the cache key payload.
        /// All replaceable literals are collapsed to '?' placeholders.
        String normalized_sql;
        /// Literal tokens extracted from the original SQL in replacement order.
        /// Each entry preserves the raw text and a type classification.
        std::vector<ParameterInfo> params;
        /// Typed literal values parsed from params in the same order. This vector is
        /// populated lazily (by parseNormalizedParamsWithAST or parseNormalizedParamsWithPlan)
        /// and then reused by AST and QueryPlan replacement logic.
        std::vector<Field> parsed_params;
        /// Rewritten SQL text where bare vector array literals (e.g. [1.0, 2.0, ...])
        /// are wrapped in explicit CAST(..., 'Array(Float)') expressions.
        /// Used when the `vector_use_cast` setting is enabled.
        String new_sql;
        /// Ordered list of AST literal positions collected during normalization.
        /// Each entry records the AST path, function chain, identifier name, step type,
        /// and target data type needed to later re-inject runtime values.
        std::vector<VectorQueryPlanCache::ASTLiteralPosition> ast_literal_position_list;
    };

    struct LightParseResult
    {
        SettingsChanges changes;
        bool is_select = false;
    };

    /// Walk the AST and collect positions of all cacheable literal nodes.
    /// Each position records the AST path, enclosing function chain, identifier name,
    /// step type, and target data type.  When `only_vector` is true, only literals
    /// inside vector search functions (cosinedistance/l2distance/hastoken) are collected.
    std::vector<VectorQueryPlanCache::ASTLiteralPosition> collectASTLiteralPositions(
        const ASTPtr & query_ast,
        bool only_vector = false) const;

    /// Normalize an AST in-place: replace each collectable literal with a placeholder
    /// value ('__VEC_PLACEHOLDER__') and record the original value in `parsed_params`.
    /// Returns the normalized SQL string, literal positions, and parsed parameter values.
    VectorQueryParameters::NormalizedQueryResult normalizedAST(
        const ASTPtr & query_ast,
        bool only_vector = false) const;

    /// Rewrite bare vector array literals in the SQL text into explicit CAST expressions.
    /// For example, `[1.0, 2.0, 3.0]` becomes `CAST([1.0, 2.0, 3.0], 'Array(Float)')`.
    /// This is used when `vector_use_cast` is enabled but plan caching is not active.
    String rewriteVectorLiteralsToCasts(
        const char * begin,
        const char * end) const;

    /// Extract literal values from the AST by following the recorded `positions` paths.
    /// Returns an empty vector if any path does not resolve to an ASTLiteral node.
    std::vector<Field> buildParameterValuesFromAST(
        const ASTPtr & query_ast,
        const std::vector<VectorQueryPlanCache::ASTLiteralPosition> & positions);

    /// Re-inject parsed parameter values into the AST at the recorded literal positions.
    /// Used to restore a cached AST template with the current query's runtime values.
    /// Returns true if at least one literal was successfully replaced.
    bool applyParametersByASTLiteralPositions(
        ASTPtr & query_ast,
        NormalizedQueryResult & parameters,
        const std::vector<VectorQueryPlanCache::ASTLiteralPosition> & positions) const;

    /// Parse the raw string tokens in `parameters.params` into typed Field values,
    /// using type hints from the AST literal positions (field_type, target_type).
    /// Populates `parameters.parsed_params`.  Returns true if parsing succeeded.
    bool parseNormalizedParamsWithAST(
        NormalizedQueryResult & parameters,
        const std::vector<VectorQueryPlanCache::ASTLiteralPosition> * positions = nullptr,
        bool only_vector = false) const;

    /// Parse the raw string tokens in `parameters.params` into typed Field values,
    /// using type hints from the QueryPlan constant bindings (field_type, target_type).
    /// Falls back to AST positions for type hints when bindings are incomplete.
    /// Returns true if parsing succeeded.
    bool parseNormalizedParamsWithPlan(
        NormalizedQueryResult & parameters,
        const std::vector<VectorQueryPlanCache::PlanConstantBinding> * plan_constant_bindings = nullptr,
        bool only_vector = false) const;

    /// Rewrite every constant slot in the cached QueryPlan with the current runtime
    /// parameter values.  Each binding in `plan_constant_bindings` points to a specific
    /// COLUMN node inside an ActionsDAG; the method replaces that node's const column
    /// with a new ColumnConst holding the parsed runtime Field value.
    /// Returns false if any replacement fails (type mismatch, missing node, etc.).
    bool replaceConstantsInQueryPlan(
        QueryPlan & plan,
        NormalizedQueryResult & parameters,
        const std::vector<VectorQueryPlanCache::PlanConstantBinding> & plan_constant_bindings);

    /// Tokenize the raw SQL text, replace literals with '?' placeholders, and collect
    /// the original literal values in order.  This is the primary entry point for the
    /// SQL-text-based cache path (as opposed to the AST-based path via normalizedAST).
    /// Returns a NormalizedQueryResult whose `hash` is zero if the query is not eligible.
    NormalizedQueryResult normalizeQueryAndExtractParams(
        const char * begin,
        const char * end,
        bool only_vector = false,
        bool is_cast = false,
        bool enable_vector_performance_test = false);

    /// Light-weight lexer-only scan for vector settings in top-level query SETTINGS clauses.
    /// This is used before the full SQL parse so vector cache/cast gates can see per-query overrides.
    LightParseResult parseVectorSettingsFromQuery(
        const char * begin,
        const char * end) const;

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
        const NormalizedQueryResult & parameters, bool only_vector = false);

};

}
