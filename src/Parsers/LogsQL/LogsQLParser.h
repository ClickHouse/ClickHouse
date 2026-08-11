#pragma once

#include <Parsers/IAST_fwd.h>
#include <Parsers/LogsQL/LogsQLLexer.h>
#include <Parsers/LogsQL/LogsQLUtils.h>

#include <functional>
#include <optional>

namespace DB
{

/// Parses a LogsQL query (the query language of VictoriaLogs, https://docs.victoriametrics.com/victorialogs/logsql/)
/// and translates it into a ClickHouse SELECT query over the table given by the `logsql_database`/`logsql_table` settings.
///
/// The filter expression is translated into a WHERE condition, and every pipe
/// (`| fields ...`, `| stats ...`, `| sort by (...)` etc.) either amends the current SELECT
/// or wraps it into a subquery.
class LogsQLParser
{
public:
    struct Context
    {
        String database;     /// Database of the logs table. Empty means the current database.
        String table;        /// Name of the logs table.
        String time_column;  /// The column referred to by the `_time` field.
        String msg_column;   /// The column referred to by the `_msg` field.
        size_t max_depth = 0;
        /// Whether the input was clipped by `max_query_size` (see ParserLogsQLQuery).
        bool truncated = false;
    };

    LogsQLParser(const char * begin_, const char * end_, Context context_);

    /// Parses a complete LogsQL query and returns an ASTSelectWithUnionQuery.
    /// Throws SYNTAX_ERROR on invalid queries and NOT_IMPLEMENTED on valid queries
    /// which use LogsQL features that cannot be translated yet.
    ASTPtr parse();

    /// The position right after the parsed query (before the trailing semicolon, if any).
    const char * getParsedEnd() const { return parsed_end; }

private:
    /// An intermediate representation of one SELECT level.
    struct Layer
    {
        ASTPtr source_subquery;      /// If not set, the layer reads from the logs table itself.
        ASTs select;                 /// Empty means SELECT *.
        ASTPtr where;
        ASTs group_by;
        ASTs order_by;               /// Elements are ASTOrderByElement.
        bool order_by_all = false;
        std::optional<UInt64> limit;
        std::optional<UInt64> offset;
        bool has_aggregation = false;
        bool has_projection = false;

        /// Set by the join pipe.
        ASTPtr join_subquery;                /// ASTSelectWithUnionQuery of the right side.
        std::vector<String> join_using;      /// The `by (...)` fields.
        bool join_inner = false;             /// LEFT JOIN by default.
    };

    /// A parsed stats function which can be materialized into an aggregate function AST
    /// with an optional condition (for the `func() if (filters)` form).
    struct StatsFunc
    {
        String canonical;                                  /// e.g. "count(*)" - used as the default result name.
        std::function<ASTPtr(ASTPtr condition)> build;
    };

    struct SortField
    {
        String name;
        bool is_desc = false;
    };

    LogsQLLexer lex;
    Context context;
    size_t depth = 0;
    const char * parsed_end = nullptr;

    /// Set by options(time_offset=...): shifts all `_time` filters into the past.
    Int64 options_time_offset_ns = 0;
    /// Set by options(global_filter=(...)): ANDed into the query and all its subqueries.
    ASTPtr options_global_filter;
    /// The effective lower and upper `_time` bounds accumulated from all top-level `_time`
    /// filters: standalone comparisons (`_time:>=...`, `_time:<...`) as well as self-contained
    /// windows (`_time:[a, b)`, `_time:5m`, `_time:2024-01-01Z`). When both are present, they
    /// define the `rate()` denominator as the intersection of all the filters: the maximum
    /// lower / minimum upper bound. The `ns` value is set when the epoch is known at parse
    /// time; absolute bounds without an explicit timezone depend on the session timezone,
    /// so they are carried as runtime expressions only.
    ASTPtr query_time_lower_bound_expr;
    std::optional<Int64> query_time_lower_bound_ns;
    ASTPtr query_time_upper_bound_expr;
    std::optional<Int64> query_time_upper_bound_ns;
    /// The `_time` bucket step of the stats pipe being parsed, if any: it takes precedence
    /// over the query time range as the denominator of `rate()` and `rate_sum()`.
    std::optional<Int64> current_stats_time_bucket_ns;
    /// The length in seconds of a `_time` bucket of variable length (a civil day or week,
    /// which is not fixed-length across DST transitions), as a runtime expression
    /// over the bucket key. Set instead of `current_stats_time_bucket_ns` for such buckets.
    ASTPtr current_stats_time_bucket_seconds_expr;
    /// Whether that bucket is a calendar step (month or year) of variable length,
    /// over which `rate()` and `rate_sum()` cannot use a constant denominator.
    bool current_stats_time_bucket_is_calendar = false;

    /// Guards against deeply nested queries.
    struct IncreaseDepth
    {
        LogsQLParser & parser;

        explicit IncreaseDepth(LogsQLParser & parser_);
        ~IncreaseDepth();
    };

    /// Saves and restores the query-scoped state around a recursive `parseQuery`,
    /// so that a subquery cannot leak its own time range, offset, bucket, or global
    /// filter into the parent. A subquery inherits only the real query options
    /// (`options_time_offset_ns` and `options_global_filter`), which apply to a query
    /// and all its subqueries. The `_time` bounds and the stats bucket of the parent
    /// are cleared instead: they are only used as the denominator of `rate()`, and the
    /// subquery does not get the parent's `_time` predicate or `GROUP BY` bucket.
    struct QueryScopeGuard
    {
        LogsQLParser & parser;
        Int64 saved_options_time_offset_ns;
        ASTPtr saved_options_global_filter;
        ASTPtr saved_query_time_lower_bound_expr;
        std::optional<Int64> saved_query_time_lower_bound_ns;
        ASTPtr saved_query_time_upper_bound_expr;
        std::optional<Int64> saved_query_time_upper_bound_ns;
        std::optional<Int64> saved_current_stats_time_bucket_ns;
        ASTPtr saved_current_stats_time_bucket_seconds_expr;
        bool saved_current_stats_time_bucket_is_calendar;

        QueryScopeGuard(LogsQLParser & parser_, bool is_subquery);
        ~QueryScopeGuard();
    };

    [[noreturn]] void throwSyntaxError(const String & message) const { lex.throwSyntaxError(message); }
    [[noreturn]] void throwNotImplemented(const String & what) const;

    /// The identifier for the column backing the given LogsQL field.
    ASTPtr columnExpr(const String & field_name) const;
    /// The numeric value of the field, for the numeric stats functions.
    ASTPtr numericValueExpr(const String & field_name) const;
    ASTPtr makeNumericComparison(const String & field_name, const String & function_name, ASTPtr literal, const String & original_text = {}) const;
    String columnName(const String & field_name) const;

    String parseFieldName();

    /// ---- Query structure ----

    Layer parseQuery(bool is_subquery);
    void parseQueryOptions();

    /// ---- Filters. A null ASTPtr means "match all". ----

    ASTPtr parseFilterOr(const String & field_name);
    ASTPtr parseFilterAnd(const String & field_name);
    ASTPtr parseFilterGeneric(const String & field_name);
    ASTPtr parseFilterPhrase(const String & field_name);
    ASTPtr parseFilterParens(const String & field_name);
    ASTPtr parseFilterNot(const String & field_name);
    ASTPtr parseFilterStar(const String & field_name);
    ASTPtr parseFilterTilda(const String & field_name, bool negative);
    ASTPtr parseFilterEQ(const String & field_name, bool negative);
    ASTPtr parseFilterGT(const String & field_name);
    ASTPtr parseFilterLT(const String & field_name);
    ASTPtr parseFilterRange(const String & field_name);
    ASTPtr parseFilterIn(const String & field_name);
    ASTPtr parseFilterContains(const String & field_name, bool need_all);
    ASTPtr parseFilterSequence(const String & field_name);
    ASTPtr parseFilterExact(const String & field_name);
    ASTPtr parseFilterRegexpFunc(const String & field_name);
    ASTPtr parseFilterAnyCase(const String & field_name);
    ASTPtr parseFilterStringRange(const String & field_name);
    ASTPtr parseFilterLenRange(const String & field_name);
    ASTPtr parseFilterIPv4Range(const String & field_name);
    ASTPtr parseFilterIPv6Range(const String & field_name);
    ASTPtr parseFilterFieldComparison(const String & field_name, const String & func);
    ASTPtr parseFilterStreamId();
    ASTPtr parseFilterCommonCase(const String & field_name, bool equals);
    ASTPtr parseFilterJSONArrayContainsAny(const String & field_name);
    ASTPtr parseFilterStream();
    ASTPtr parseFilterTime();
    ASTPtr parseFilterDayRange();
    ASTPtr parseFilterWeekRange();

    /// Parses `(arg1, ..., argN)`. Returns the decoded arguments.
    /// `wildcard` is set if one of the arguments is `*`.
    std::vector<String> parseArgsInParens(bool * wildcard = nullptr);

    /// Builders for the primitive filters.
    ASTPtr makePhraseFilter(const String & field_name, const String & phrase, bool case_insensitive = false);
    ASTPtr makePrefixFilter(const String & field_name, const String & prefix, bool case_insensitive = false);
    ASTPtr makeRegexpFilter(const String & field_name, const String & regexp);
    ASTPtr makeExactFilter(const String & field_name, const String & value);
    ASTPtr makeValueLiteral(const String & text, bool quoted);
    ASTPtr makeComparisonFilter(const String & field_name, const String & function_name, const String & value, bool quoted);

    /// A time bound: an expression for an instant, possibly with a second instant for the end
    /// of the period when the timestamp has a coarse precision (e.g. `2023-04Z` covers the whole month).
    struct TimeBound
    {
        ASTPtr start;
        ASTPtr end;
        /// Set when the bound is an absolute timestamp: nanoseconds since epoch in UTC.
        std::optional<Int64> start_ns;
        std::optional<Int64> end_ns;
    };

    TimeBound parseTimeBound();
    std::optional<Int64> parseOptionalTimeOffset();
    static ASTPtr makeIntervalAST(Int64 ns);
    static ASTPtr shiftTime(ASTPtr expr, Int64 offset_ns);
    static ASTPtr makeTimeRangeSecondsExpr(ASTPtr lower, ASTPtr upper);
    ASTPtr makeTimeCondition(ASTPtr lower, bool lower_inclusive, ASTPtr upper, bool upper_inclusive, Int64 offset_ns);
    void recordTimeLowerBound(ASTPtr expr, std::optional<Int64> ns, Int64 offset_ns);
    void recordTimeUpperBound(ASTPtr expr, std::optional<Int64> ns, Int64 offset_ns);

    /// ---- Pipes ----

    void parsePipes(Layer & layer);
    void parsePipe(Layer & layer);
    bool isLikelyStatsPipe();
    bool isLikelyFilterPipe();

    void parsePipeFields(Layer & layer);
    void parsePipeDelete(Layer & layer);
    void parsePipeCopy(Layer & layer);
    void parsePipeRename(Layer & layer);
    void parsePipeLimit(Layer & layer);
    void parsePipeOffset(Layer & layer);
    void parsePipeSort(Layer & layer);
    void parsePipeStats(Layer & layer, bool need_keyword);
    void parsePipeWhere(Layer & layer, bool need_keyword);
    void parsePipeUniq(Layer & layer);
    void parsePipeTop(Layer & layer);
    void parsePipeFirstLast(Layer & layer, bool is_last);
    void parsePipeMath(Layer & layer);
    void parsePipeLen(Layer & layer);
    void parsePipeCoalesce(Layer & layer);
    void parsePipeDecolorize(Layer & layer);
    void parsePipeSplit(Layer & layer);
    void parsePipeUnpackWords(Layer & layer);
    void parsePipeTimeAdd(Layer & layer);
    void parsePipeSample(Layer & layer);
    void parsePipeGenerateSequence(Layer & layer);
    void parsePipeFieldValues(Layer & layer);
    void parsePipeJSONArrayLen(Layer & layer);
    void parsePipeReplace(Layer & layer, bool is_regexp);
    void parsePipeUnion(Layer & layer);
    void parsePipeHash(Layer & layer);
    void parsePipeUnroll(Layer & layer);
    void parsePipePack(Layer & layer, bool is_logfmt);
    void parsePipeJoin(Layer & layer);
    void parsePipeExtract(Layer & layer, bool is_regexp);
    void parsePipeFormat(Layer & layer);
    void parsePipeUnpack(Layer & layer, bool is_logfmt);
    void parsePipeRunningStats(Layer & layer, bool is_total);

    ASTPtr parseFilterPatternMatch(const String & field_name, const String & func_name);

    /// One step of an extract/format pattern: a literal prefix followed by an optional field placeholder.
    struct PatternStep
    {
        String prefix;
        String field;      /// Empty for anonymous placeholders (<>, <_>, <*>) and for the trailing literal.
        bool plain = false;
    };
    std::vector<PatternStep> parsePatternSteps(const String & pattern);

    /// Applies extracted/formatted values to the layer: replaces existing columns
    /// (when `use_replace`) or appends new computed columns.
    void applyComputedFields(Layer & layer, const std::vector<std::pair<String, ASTPtr>> & fields, bool use_replace);

    /// Parses the optional `if (<filters>)` clause used by several pipes. Returns nullptr if absent or empty.
    ASTPtr parseOptionalIfCondition();

    /// Parses `(<filters>)` with the current token at '('. Returns nullptr for an empty `()`.
    ASTPtr parseParenthesizedFilter();

    /// Replaces the column with an expression at the current layer: `SELECT * REPLACE (<expression> AS <column>)`.
    void applyColumnReplacement(Layer & layer, const String & column, ASTPtr expression);

    /// Appends `expression AS alias` to the current layer's select list (keeping `*` as the base).
    void appendComputedColumn(Layer & layer, ASTPtr expression, const String & alias);

    std::vector<SortField> parseSortFields();
    UInt64 parseLimitValue();

    /// Applies sorting with the optional rank column and per-partition top-N.
    void applySortWithExtras(
        Layer & layer,
        const std::vector<SortField> & fields,
        bool global_desc,
        const std::vector<String> & partition_fields,
        const String & rank_name,
        std::optional<UInt64> sort_limit,
        std::optional<UInt64> sort_offset);

    /// An expression for sorting by the given field at the current layer. If the layer's select list
    /// has an expression aliased with this name, the expression itself is used: `ORDER BY alias`
    /// would otherwise resolve to the source table column when a column with the same name exists
    /// (e.g. the bucketed `_time` in `stats by (_time:1d)`).
    ASTPtr sortKeyExpr(const Layer & layer, const String & field_name) const;

    StatsFunc parseStatsFunc();
    ASTPtr parseMathExpr(int max_priority);
    ASTPtr parseMathExprOperand();

    /// ---- Assembling the resulting AST ----

    ASTPtr buildSelect(Layer & layer) const;
    ASTPtr buildSelectWithUnion(Layer & layer) const;
    void wrapLayer(Layer & layer) const;

    /// Wraps the layer if it already has any of the given aspects set.
    void wrapLayerIf(Layer & layer, bool condition) const;
};

}
