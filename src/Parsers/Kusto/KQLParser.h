#pragma once

#include <Parsers/Kusto/KQLAST.h>
#include <Parsers/Kusto/KQLLexer.h>

#include <map>
#include <set>
#include <vector>


namespace DB
{

/** Recursive-descent parser for the Kusto Query Language.
  *
  * Consumes the token vector produced by `KQLLexer` and produces a `KQLTabularExpression`.
  * Scalar expressions come out as ClickHouse AST nodes (see `KQLAST.h` for why).
  *
  * Errors are reported by throwing `Exception(SYNTAX_ERROR)` with the offending token's
  * offset, because there is only one grammar to try: unlike a `SET`-vs-`SELECT` choice in
  * the SQL parser, nothing here backtracks between alternatives on failure. That is also
  * why this file contains no `catch`.
  *
  * `let` bindings live in `scope`, an ordinary member. They are therefore scoped to one
  * parse, which is what makes them correct across concurrent queries.
  */
class KQLParser
{
public:
    KQLParser(const char * query_begin_, std::vector<KQLToken> tokens_, size_t max_depth_);

    /// Parses any number of `let` statements followed by one tabular expression.
    /// Stops after that expression, so a trailing `SET dialect = ...` is left for the caller.
    KQLTabularExpressionPtr parseQuery();

    /// Offset just past the last consumed character, for the caller to resume from.
    const char * getEndPosition() const;

private:
    /** A `let`-bound function: `let f = (x: long, n: long = 2) { x * n }`.
      *
      * The body is kept as a token range rather than a parsed tree, because a KQL function is
      * a *calculation*, not a value: it is re-evaluated at every call site, and its parameters
      * have to resolve to that call's arguments. Re-parsing the same tokens under a scope
      * where the parameter names are bound is exactly that, and it reuses the substitution
      * that plain scalar `let`s already go through.
      */
    struct TabularColumn
    {
        String name;
        /// The declared KQL type of the column, enforced on the argument's column.
        String type;
    };

    struct FunctionParameter
    {
        String name;
        /// The declared KQL type of a scalar parameter, enforced on the argument. Empty for
        /// a tabular parameter.
        String type;
        /// A tabular parameter, declared `T: (col: type, ...)` or `T: (*)`, takes a table
        /// rather than a value. Kusto requires these to come first.
        bool is_tabular = false;
        /// The columns a tabular parameter declares. Empty for `T: (*)`, which accepts any
        /// schema; otherwise the body sees exactly these columns of the argument.
        std::vector<TabularColumn> tabular_columns;
        /// Null when the parameter is required.
        ASTPtr default_value;
    };

    struct FunctionDefinition
    {
        std::vector<FunctionParameter> parameters;
        /// Token indices: the first token of the body, and the closing '}'.
        size_t body_begin = 0;
        size_t body_end = 0;
        /// Whether the body reads as a pipeline rather than a scalar expression. Decides how
        /// `let name = call(...);` binds, since nothing at that call site says which one it is.
        bool body_looks_tabular = false;
    };

    struct Scope
    {
        std::map<String, ASTPtr> scalars;
        std::map<String, KQLTabularExpressionPtr> tabulars;
        std::map<String, FunctionDefinition> functions;
    };

    /// Raises the recursion depth for its lifetime and rejects input that nests too deep.
    class DepthGuard
    {
    public:
        DepthGuard(KQLParser & parser_, const char * what);
        ~DepthGuard() { --parser.depth; }

    private:
        KQLParser & parser;
    };

    /// Token access.
    const KQLToken & current() const { return tokens[index]; }
    const KQLToken & lookahead(size_t offset = 1) const;
    bool at(KQLTokenType type) const { return current().type == type; }
    /// KQL keywords are case-insensitive; every keyword test goes through here.
    bool atKeyword(std::string_view keyword) const;
    bool tokenIsKeyword(size_t position, std::string_view keyword) const;
    bool consumeKeyword(std::string_view keyword);
    bool consume(KQLTokenType type);
    void expect(KQLTokenType type);
    void expectKeyword(std::string_view keyword);
    String expectIdentifierName();
    [[noreturn]] void fail(const String & message) const;
    [[noreturn]] void failAt(const KQLToken & token, const String & message) const;
    /// Maps a KQL scalar type name from a `datatable` schema, a `typeof(...)` or a function
    /// parameter onto the ClickHouse type name, rejecting names with no faithful mapping.
    const String & resolveScalarType(const KQLToken & type_token, const String & kql_type) const;

    /// Statements.
    void parseLetStatement();
    /// True when what follows a `let name =` is `( parameters ) {`, which no other form is.
    bool atFunctionDefinition() const;
    void parseFunctionDefinition(const String & name);
    /// Whether the token range `[begin, end)` - any number of `let` statements followed by one
    /// expression - reads as a pipeline rather than a scalar expression. `tabular_names` seeds
    /// the names known to stand for a table and `scalar_names` those known to stand for a
    /// scalar; the range's own `let`s add theirs while scanning. A bare name standing alone
    /// that neither set knows is a physical table when `unknown_name_is_table` says so -
    /// inside `in (...)` such a name is a column reference instead.
    bool bodyLooksTabular(size_t begin, size_t end, std::set<String> tabular_names, std::set<String> scalar_names);
    bool expressionLooksTabular(
        size_t begin,
        size_t end,
        const std::set<String> & tabular_names,
        const std::set<String> & scalar_names,
        bool unknown_name_is_table = true) const;
    /// The names the current scope knows to stand for a table: the tabular bindings and the
    /// tabular-bodied functions. Seeds the classifiers above.
    std::set<String> scopeTabularNames() const;
    /// Likewise for the scalars: the scalar bindings and the scalar-bodied functions.
    std::set<String> scopeScalarNames() const;
    /// The first ';' outside any brackets at or after `position`, or `end`.
    size_t statementEnd(size_t position, size_t end) const;
    /// Where the bracket open just before `position` closes, or `tokens.size()`.
    size_t closingBracket(size_t position) const;

    /// Calls. Which one applies is decided by where the name appears: a scalar expression
    /// position evaluates the body as an expression, a source position as a pipeline.
    ASTPtr callScalarFunction(const String & name, const KQLToken & call_token);
    KQLTabularExpressionPtr callTabularFunction(const String & name, const KQLToken & call_token);
    /// Reads the argument list and returns the scope the body should be parsed under.
    Scope bindArguments(const String & name, const FunctionDefinition & definition, const KQLToken & call_token);
    /// Projects a tabular argument onto the columns its parameter declares.
    static KQLTabularExpressionPtr
    restrictToDeclaredColumns(const KQLTabularExpressionPtr & argument, const FunctionParameter & parameter, const String & function_name);
    /// Wraps a scalar argument so that the declared parameter type is enforced when the
    /// lowered query is analyzed.
    static ASTPtr enforceParameterType(ASTPtr argument, const String & kql_type, String parameter_description);

    /// Tabular level.
    KQLTabularExpressionPtr parseTabularExpression();
    KQLSourcePtr parseSource();
    KQLSourcePtr parsePrintSource();
    KQLSourcePtr parseDataTableSource();
    KQLSourcePtr parseRangeSource();
    KQLOperatorPtr parsePipelineOperator();

    /// Individual operators.
    KQLOperatorPtr parseWhere();
    KQLOperatorPtr parseExtend();
    KQLOperatorPtr parseProject();
    KQLOperatorPtr parseProjectAwayOrKeep(KQLOperatorKind kind);
    KQLOperatorPtr parseProjectRename();
    KQLOperatorPtr parseSummarize();
    KQLOperatorPtr parseSort();
    KQLOperatorPtr parseTake();
    KQLOperatorPtr parseTop();
    KQLOperatorPtr parseDistinct();
    KQLOperatorPtr parseCount();
    KQLOperatorPtr parseMvExpand();
    KQLOperatorPtr parseJoin();
    KQLOperatorPtr parseUnion();
    KQLOperatorPtr parseAs();
    KQLOperatorPtr parseRender();

    std::vector<KQLSortItem> parseSortItems();
    std::vector<KQLNamedExpression> parseNamedExpressionList();
    KQLNamedExpression parseNamedExpression();

    /// Expressions, loosest binding first.
    ASTPtr parseExpression();
    ASTPtr parseOr();
    ASTPtr parseAnd();
    ASTPtr parseComparison();
    ASTPtr parseAdditive();
    ASTPtr parseMultiplicative();
    ASTPtr parseUnary();
    ASTPtr parsePostfix(ASTPtr operand);
    ASTPtr parsePrimary();
    ASTPtr parseFunctionCall(const String & name);
    ASTPtr parseDynamicLiteral();
    /// `bool(true)`, `int(1)`, `real(nan)`, `time(1d)`, ... - Kusto's typed literal forms.
    /// Returns nullptr when `name` is not one of them.
    ASTPtr tryParseTypedLiteral(const String & name);
    ASTPtr parseParenthesizedOrInList();

    /// The word operators (`contains`, `has`, `in`, `between`, ...) that sit at comparison level.
    /// Returns nullptr when `current()` is not one of them.
    ASTPtr tryParseWordOperator(const ASTPtr & left);

    /// A tabular expression is allowed in `join (...)`, `union (...)` and `x in (T | ...)`.
    KQLTabularExpressionPtr parseParenthesizedTabularExpression();

    const char * query_begin;
    std::vector<KQLToken> tokens;
    size_t index = 0;
    size_t depth = 0;
    const size_t max_depth;
    Scope scope;
    /// Guards against a function that calls itself; Kusto has no recursion either.
    std::set<String> functions_in_progress;
    /// Whether an aggregation is being parsed - the aggregation list of `summarize`. Only
    /// there may a KQL aggregate function be called.
    bool in_aggregation = false;
};

}
