#pragma once

#include <Parsers/IAST_fwd.h>
#include <base/types.h>

#include <memory>
#include <vector>


namespace DB
{

/** The KQL query representation that sits between the parser and the translator.
  *
  * Only the *tabular* level is modelled here - sources and the pipeline of operators.
  * Scalar expressions are represented directly as ClickHouse AST (`ASTPtr`), because KQL
  * expressions map one-to-one onto `ASTFunction` / `ASTLiteral` / `ASTIdentifier` and a
  * parallel expression hierarchy would buy nothing but conversion code.
  *
  * What matters is that a KQL expression becomes a *node*, never a fragment of SQL text.
  * A string literal becomes `ASTLiteral(Field(String))`, so its contents can never be
  * reinterpreted as SQL syntax no matter what the user put in it.
  */

struct KQLTabularExpression;
using KQLTabularExpressionPtr = std::shared_ptr<KQLTabularExpression>;

/// An expression with an optional output name: `Duration = end - start`.
struct KQLNamedExpression
{
    String alias;
    ASTPtr expression;
};

struct KQLSortItem
{
    ASTPtr expression;
    /// KQL sorts descending unless told otherwise - the opposite of SQL.
    bool descending = true;
    bool nulls_first = false;
};

enum class KQLSourceKind : uint8_t
{
    Table, /// `StormEvents`
    Print, /// `print a = 1, b = 2`
    DataTable, /// `datatable (a:long, b:string) [1, "x"]`
    Range, /// `range x from 1 to 10 step 2`
    Subquery, /// `(T | where a > 1)`
    Union, /// `union A, B`
};

struct KQLSource
{
    KQLSourceKind kind = KQLSourceKind::Table;

    /// Table
    String database;
    String table;

    /// Print
    std::vector<KQLNamedExpression> projections;

    /// DataTable: `column_names[i]` has type `column_types[i]`; `values` is row-major.
    std::vector<String> column_names;
    std::vector<String> column_types;
    std::vector<ASTPtr> values;

    /// Range
    String range_column;
    ASTPtr range_from;
    ASTPtr range_to;
    ASTPtr range_step;

    /// Subquery / Union
    std::vector<KQLTabularExpressionPtr> inputs;
};
using KQLSourcePtr = std::shared_ptr<KQLSource>;

enum class KQLOperatorKind : uint8_t
{
    Where,
    Extend,
    Project,
    ProjectAway,
    ProjectKeep,
    ProjectRename,
    Summarize,
    Sort,
    Take,
    Top,
    Distinct,
    Count,
    MvExpand,
    Join,
    Union,
    As,
    Render,
};

enum class KQLJoinKind : uint8_t
{
    InnerUnique,
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    LeftSemi,
    RightSemi,
    LeftAnti,
    RightAnti,
};

struct KQLOperator
{
    KQLOperatorKind kind = KQLOperatorKind::Where;

    /// The keyword as the user spelled it, for error messages.
    String name;

    /// Where
    ASTPtr predicate;

    /// Extend / Project / Summarize aggregations / Distinct / MvExpand
    std::vector<KQLNamedExpression> expressions;

    /// Summarize
    std::vector<KQLNamedExpression> by_expressions;

    /// ProjectAway / ProjectKeep: column names, possibly with `*` wildcards.
    std::vector<String> column_patterns;

    /// ProjectRename: `new_name = old_name`.
    std::vector<std::pair<String, String>> renames;

    /// Sort / Top
    std::vector<KQLSortItem> sort_items;

    /// Take / Top
    ASTPtr limit;

    /// Count / As
    String alias;

    /// Join / Union
    KQLJoinKind join_kind = KQLJoinKind::InnerUnique;
    std::vector<KQLTabularExpressionPtr> inputs;
    /// Equi-join keys. `on x` means both sides use `x`; `on $left.a == $right.b` names them.
    std::vector<std::pair<String, String>> join_keys;
    /// `union` may be given `kind=outer`, which we accept only in its default form.
    bool union_kind_outer = false;
};
using KQLOperatorPtr = std::shared_ptr<KQLOperator>;

/// A source followed by a pipeline: `T | where a > 1 | project b`.
struct KQLTabularExpression
{
    KQLSourcePtr source;
    std::vector<KQLOperatorPtr> operators;
};

}
