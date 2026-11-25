#pragma once

#include <Core/NamesAndTypes.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/Q/QSyntax.h>

#include <map>
#include <optional>
#include <unordered_map>


namespace DB
{

/// The internals of the compiler of the `q` dialect. See `compileQProgram`.
///
/// A q expression is compiled into a value of one of the following kinds:
///
/// - Scalar: a value that does not depend on the rows of a table: an atom (`depth` 0) or a list
///   (an `Array` of depth `depth`). Inside a q-sql template, a scalar can contain aggregate functions
///   (`aggregated`); then it is one value per group.
/// - Column: a value of each row of the table of a q-sql template, like a column reference `price`
///   or `price*size`. A q column is a list, so aggregating it (`sum price`) makes a scalar, and a
///   uniform function (`sums price`, `prev price`) becomes a window function (`windowed`).
/// - Table: a table expression for `FROM`, with its columns (and the key columns of a keyed table).
/// - Function: a lambda, a builtin, a derived function (`+/`), or a projection (`+[1]`).
/// - Dictionary: a pair of lists of keys and values.
///
/// A q string is a list of chars, but it is compiled into a ClickHouse `String` atom (`is_string`).
namespace QCompilerInternals
{

struct QTable
{
    /// An `ASTTableExpression` for `FROM`.
    ASTPtr table_expression;
    NamesAndTypesList columns;
    /// The key columns of a keyed table (they come first in `columns`).
    Names keys;
};

using QTablePtr = std::shared_ptr<const QTable>;

struct QFunction;
using QFunctionPtr = std::shared_ptr<const QFunction>;

struct QValue;

enum class QValueKind : uint8_t
{
    Scalar,
    Column,
    Table,
    Function,
    Dictionary,
};

struct QValue
{
    QValueKind kind = QValueKind::Scalar;
    ASTPtr ast;
    /// The nesting depth of lists: of the value itself for a scalar, of the value of each row for a column.
    size_t depth = 0;
    bool is_string = false;
    /// Scalar: contains aggregate functions.
    bool aggregated = false;
    /// Column: contains window functions.
    bool windowed = false;
    /// The q type of the atoms, when known (for literals and temporal columns).
    std::optional<QType> qtype;

    QTablePtr table;
    QFunctionPtr function;
    /// Dictionary: the keys and the values.
    std::shared_ptr<const std::pair<QValue, QValue>> dictionary;

    [[nodiscard]] bool isList() const { return kind == QValueKind::Scalar && depth > 0; }
};

using QValues = std::vector<QValue>;

struct QFunction
{
    /// A builtin: a primitive verb (`+`) or a keyword (`sum`).
    String builtin;
    /// A lambda.
    QNodePtr lambda;
    /// A derived function: the adverb and the function it modifies.
    String adverb;
    QFunctionPtr operand;
    /// The arguments of a projection; an elided argument is `std::nullopt`.
    std::vector<std::optional<QValue>> bound;
};

/// The bindings of names to values: the locals of a lambda, or the globals of a program.
struct QEnvironment
{
    std::map<String, QValue> names;
    QEnvironment * globals = nullptr;
};

/// The table of a q-sql template, in which expressions are compiled.
struct QScope
{
    /// The columns that the expressions can refer to.
    NamesAndTypesList columns;

    /// Whether aggregations fold groups (`select ... by`): then a column is collected into a list per group.
    bool grouped = false;

    /// The partition of a window function that an aggregate becomes when it is combined with a column:
    /// the by phrase of `update ... by`, or empty for the whole table.
    ASTs partition;

    /// The columns computed before the aggregation (window functions under an aggregate function),
    /// as pairs of the expression and its name.
    std::vector<std::pair<ASTPtr, String>> * pre_aggregation = nullptr;

    /// Whether a window function was created: the rows have to be numbered to keep their order.
    bool uses_row_number = false;
};

class QCompiler
{
public:
    explicit QCompiler(ContextPtr context_);

    ASTPtr compileProgram(const QProgram & program);

    /// Compiles an expression. `scope` is the table of the enclosing q-sql template, if any.
    QValue compile(const QNodePtr & node, QEnvironment & env, QScope * scope);

    /// Applies a function value to arguments.
    QValue apply(const QFunctionPtr & function, QValues args, QEnvironment & env, QScope * scope, size_t offset);

    /// Applies a value (a function, or a list, dictionary, or table to index) to arguments.
    QValue applyValue(const QValue & target, QValues args, QEnvironment & env, QScope * scope, size_t offset);

    /// Calls a builtin with all the arguments given. Implemented in `QBuiltins.cpp`.
    QValue callBuiltin(const String & name, QValues args, QEnvironment & env, QScope * scope, size_t offset);

    /// The number of arguments of a builtin (1 or 2), or 0 for a builtin with a variable number of arguments.
    static size_t builtinArity(const String & name);
    static bool isBuiltin(const String & name);

    /// Applies a derived function (with an adverb).
    QValue applyDerived(const QFunction & function, QValues args, QEnvironment & env, QScope * scope, size_t offset);

    /// Helpers to make values.
    static QValue scalar(ASTPtr ast, size_t depth = 0);
    QValue makeTable(ASTPtr select_query, Names keys = {});
    QValue tableFromSelect(ASTPtr select_query, Names keys = {});

    /// Makes an atomic function: applied to the atoms of lists (through `arrayMap`), row by row to columns.
    QValue atomic(const String & q_name, QValues args, const std::function<ASTPtr(const ASTs &)> & make, QScope * scope);

    /// Converts an aggregated scalar into a column (a window function over the partition of the scope).
    QValue broadcast(const QValue & value, QScope * scope);

    /// Wraps the aggregate functions in `ast` into window functions with the given partition.
    ASTPtr windowize(const ASTPtr & ast, const ASTs & partition, QScope * scope);

    /// Makes a window function over the rows in their order: `function(args) OVER (PARTITION BY ... ORDER BY row ROWS ...)`.
    /// `preceding` is the number of preceding rows in the frame (`std::nullopt` for unbounded), `following` of the following rows.
    ASTPtr makeWindowFunction(const String & function, ASTs args, QScope * scope, std::optional<UInt64> preceding, UInt64 following);

    /// A column that a window function or an aggregate can refer to. A windowed column under an aggregate
    /// is computed before the aggregation.
    ASTPtr columnForAggregation(const QValue & column, QScope * scope);

    /// Converts a value to a scalar: a table with one column becomes the list of its values.
    QValue toScalar(const QValue & value, size_t offset);

    /// An aggregate that collects the values of a column into a list, keeping the nulls in their places.
    /// A null of q is a `NULL`, and `groupArray` skips those: the list would be shorter than the column,
    /// and the lists of two columns of one group would no longer match item by item.
    ASTPtr collectColumn(ASTPtr column);

    /// The value of a column in the row that an aggregate picks (`anyLast`, or `argMin` and `argMax` with `by`),
    /// which is a null if the row has a null: these aggregates skip the nulls as well.
    static ASTPtr pickFromColumn(const String & aggregate, ASTPtr column, ASTPtr by = nullptr);

    /// A query that reads all the columns of a table.
    ASTPtr selectAll(const QTable & table);

    String newName();
    String newLambdaParameter();

    /// The values that are known to have no nulls: the literals other than nulls, and the columns of the types that are
    /// not `Nullable`. They are told by their ASTs, so a value derived from one of them is not taken for it.
    /// A comparison of two such values is the comparison of ClickHouse as it is, see `compare` in `QBuiltins.cpp`.
    void markNotNull(const ASTPtr & ast);
    [[nodiscard]] bool isNotNull(const ASTPtr & ast) const;

    [[nodiscard]] ContextPtr getContext() const { return context; }

    /// The maximum nesting of inlined lambdas.
    static constexpr size_t max_inline_depth = 64;
    size_t inline_depth = 0;

private:
    ContextPtr context;
    size_t name_counter = 0;

    /// The limit of the expansion of the program (the setting `max_expanded_ast_elements`), or zero for no limit.
    ///
    /// The values of a q program share their ASTs: `a:a+a` refers to the previous `a` twice, and a function is inlined
    /// with its argument wherever it mentions the parameter. So a short program can denote an expression of exponential
    /// size, while the generated query, and every copy made on the way to it, holds a separate subtree per reference.
    /// Two things are limited: the size of every value when expanded (`checkExpandedSize`), and the total number of
    /// the elements copied (`cloneForQuery`). Both are checked before the work is done, so the work is bounded as well.
    size_t max_expanded_elements = 0;

    /// The expanded sizes of the ASTs of the values compiled so far. A new value is made of the previous ones and a few
    /// new elements, so only the new elements are counted. The ASTs are held here, so that an address is not reused.
    std::unordered_map<const IAST *, std::pair<ASTPtr, size_t>> expanded_sizes;

    void checkExpandedSize(const ASTPtr & ast);
    void checkExpandedSize(const QValue & value);

    /// See `markNotNull`. The ASTs are held, so that an address is not reused.
    std::unordered_map<const IAST *, ASTPtr> not_null_asts;

    QValue compileNode(const QNodePtr & node, QEnvironment & env, QScope * scope);
    QValue applyFunction(const QFunctionPtr & function, QValues args, QEnvironment & env, QScope * scope, size_t offset);

    QValue compileName(const QNodePtr & node, QEnvironment & env, QScope * scope, bool function_position);
    QValue compileLiteral(const QNode & node);
    QValue compileApply(const QNodePtr & node, QEnvironment & env, QScope * scope);
    QValue compileList(const QNodePtr & node, QEnvironment & env, QScope * scope);
    QValue compileTableLiteral(const QNode & node, QEnvironment & env, QScope * scope);
    QValue compileLambdaCall(const QNode & lambda, QValues args, QEnvironment & env, QScope * scope);
    QValue compileSQL(const QSQLTemplate & sql, QEnvironment & env, QScope * scope, size_t offset);

    QValue compileSelect(const QSQLTemplate & sql, const QTable & source, QEnvironment & env);
    QValue compileUpdate(const QSQLTemplate & sql, const QTable & source, QEnvironment & env);
    QValue compileDelete(const QSQLTemplate & sql, const QTable & source, QEnvironment & env);

    /// Applies the where phrase of a select: returns the filtered table.
    QTable applyWhere(const QTable & source, const std::vector<QNodePtr> & where, QEnvironment & env);

    std::optional<QValue> resolveTable(const String & name);

    ASTPtr finalQuery(const QValue & value, const QNodePtr & statement);
};

/// The name of the column produced by an unnamed phrase of a q-sql template, by the rules of q:
/// the leftmost name for a primitive verb, the last argument for a keyword, `x` for no name.
String deriveColumnName(const QNodePtr & node);

/// AST helpers.
ASTPtr makeIdentifier(const String & name);
ASTPtr makeLiteral(Field value);
ASTPtr makeLambda(const Names & params, ASTPtr body);
ASTPtr makeExpressionList(ASTs children);
ASTPtr makeSubqueryTableExpression(ASTPtr query, const String & alias = {});
ASTPtr makeTableIdentifierExpression(const String & database, const String & table);

/// Builds a `SELECT`.
struct QSelectBuilder
{
    ASTPtr from;
    /// Joined tables: pairs of a table expression and an `ASTTableJoin`.
    std::vector<std::pair<ASTPtr, ASTPtr>> joins;
    ASTs select;
    ASTPtr where;
    ASTs group_by;
    /// Pairs of the expression and whether the direction is descending.
    std::vector<std::pair<ASTPtr, bool>> order_by;
    ASTPtr limit;
    ASTPtr offset;
    /// Arrays for `ARRAY JOIN`.
    ASTs array_join;
    bool distinct = false;

    /// Returns an `ASTSelectWithUnionQuery`.
    [[nodiscard]] ASTPtr build() const;
};

/// A copy of an AST for the generated query. It is charged to the budget of the program being compiled:
/// see `QCompiler::max_expanded_elements`.
ASTPtr cloneForQuery(const ASTPtr & ast);

/// An expression with an alias.
ASTPtr withAlias(ASTPtr ast, const String & alias);

/// The ClickHouse type for a q type.
String qTypeToClickHouseType(QType type);

/// The q type for a ClickHouse type, if it has a q equivalent.
std::optional<QType> dataTypeToQType(const DataTypePtr & type);

/// The nesting depth of arrays in a type, and whether its innermost type is a string.
size_t arrayDepth(const DataTypePtr & type, bool & is_string);

[[noreturn]] void throwQError(int code, size_t offset, const String & message);

}

}
