#pragma once

#include <Parsers/IAST.h>


namespace DB
{

/** List of zero, single or multiple JOIN-ed tables or subqueries in SELECT query, with ARRAY JOINs and SAMPLE, FINAL modifiers.
  *
  * Table expression is:
  *  [database_name.]table_name
  * or
  *  table_function(params)
  * or
  *  (subquery)
  *
  * Optionally with alias (correlation name):
  *  [AS] alias
  *
  * Table may contain FINAL and SAMPLE modifiers:
  *  FINAL
  *  SAMPLE 1 / 10
  *  SAMPLE 0.1
  *  SAMPLE 1000000
  *
  * Table expressions may be combined with JOINs of following kinds:
  *  [GLOBAL] [ANY|ALL|ASOF|SEMI] [INNER|LEFT|RIGHT|FULL] [OUTER] JOIN table_expr
  *  CROSS JOIN
  *  , (comma)
  *
  * In all kinds except cross and comma, there are join condition in one of following forms:
  *  USING (a, b c)
  *  USING a, b, c
  *  ON expr...
  *
  * Also, tables may be ARRAY JOIN-ed with one or more arrays or nested columns:
  *  [LEFT|INNER|] ARRAY JOIN name [AS alias], ...
  */


/// Table expression, optionally with alias.
struct ASTTableExpression : public IAST
{
    /// One of fields is non-nullptr.
    ASTPtr database_and_table_name;
    ASTPtr table_function;
    ASTPtr subquery;

    /// Modifiers
    bool final = false;
    ASTPtr sample_size;
    ASTPtr sample_offset;

    using IAST::IAST;
    String getID(char) const override { return "TableExpression"; }
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};


/// How to JOIN another table.
struct ASTTableJoin : public IAST
{
    /// Algorithm for distributed query processing.
    enum class Locality
    {
        Unspecified,
        Local,    /// Perform JOIN, using only data available on same servers (co-located data).
        Global    /// Collect and merge data from remote servers, and broadcast it to each server.
    };

    /// Allows more optimal JOIN for typical cases.
    enum class Strictness
    {
        Unspecified,
        RightAny, /// Old ANY JOIN. If there are many suitable rows in right table, use any from them to join.
        Any,    /// Semi Join with any value from filtering table. For LEFT JOIN with Any and RightAny are the same.
        All,    /// If there are many suitable rows to join, use all of them and replicate rows of "left" table (usual semantic of JOIN).
        Asof,   /// For the last JOIN column, pick the latest value
        Semi,   /// LEFT or RIGHT. SEMI LEFT JOIN filters left table by values exists in right table. SEMI RIGHT - otherwise.
        Anti,   /// LEFT or RIGHT. Same as SEMI JOIN but filter values that are NOT exists in other table.
    };

    /// Join method.
    enum class Kind
    {
        Inner, /// Leave only rows that was JOINed.
        Left, /// If in "right" table there is no corresponding rows, use default values instead.
        Right,
        Full,
        Cross, /// Direct product. Strictness and condition doesn't matter.
        Comma /// Same as direct product. Intended to be converted to INNER JOIN with conditions from WHERE.
    };

    Locality locality = Locality::Unspecified;
    Strictness strictness = Strictness::Unspecified;
    Kind kind = Kind::Inner;

    /// Condition. One of fields is non-nullptr.
    ASTPtr using_expression_list;
    ASTPtr on_expression;

    using IAST::IAST;
    String getID(char) const override { return "TableJoin"; }
    ASTPtr clone() const override;

    void formatImplBeforeTable(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const;
    void formatImplAfterTable(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const;
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

inline bool isLeft(ASTTableJoin::Kind kind)         { return kind == ASTTableJoin::Kind::Left; }
inline bool isRight(ASTTableJoin::Kind kind)        { return kind == ASTTableJoin::Kind::Right; }
inline bool isInner(ASTTableJoin::Kind kind)        { return kind == ASTTableJoin::Kind::Inner; }
inline bool isFull(ASTTableJoin::Kind kind)         { return kind == ASTTableJoin::Kind::Full; }
inline bool isCross(ASTTableJoin::Kind kind)        { return kind == ASTTableJoin::Kind::Cross; }
inline bool isComma(ASTTableJoin::Kind kind)        { return kind == ASTTableJoin::Kind::Comma; }
inline bool isRightOrFull(ASTTableJoin::Kind kind)  { return kind == ASTTableJoin::Kind::Right || kind == ASTTableJoin::Kind::Full; }
inline bool isLeftOrFull(ASTTableJoin::Kind kind)   { return kind == ASTTableJoin::Kind::Left  || kind == ASTTableJoin::Kind::Full; }
inline bool isInnerOrRight(ASTTableJoin::Kind kind) { return kind == ASTTableJoin::Kind::Inner || kind == ASTTableJoin::Kind::Right; }


/// Specification of ARRAY JOIN.
struct ASTArrayJoin : public IAST
{
    enum class Kind
    {
        Inner,   /// If array is empty, row will not present (default).
        Left,    /// If array is empty, leave row with default values instead of array elements.
    };

    Kind kind = Kind::Inner;

    /// List of array or nested names to JOIN, possible with aliases.
    ASTPtr expression_list;

    using IAST::IAST;
    String getID(char) const override { return "ArrayJoin"; }
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};


/// Element of list.
struct ASTTablesInSelectQueryElement : public IAST
{
    /** For first element of list, either table_expression or array_join element could be non-nullptr.
      * For former elements, either table_join and table_expression are both non-nullptr, or array_join is non-nullptr.
      */
    ASTPtr table_join;       /// How to JOIN a table, if table_expression is non-nullptr.
    ASTPtr table_expression; /// Table.
    ASTPtr array_join;       /// Arrays to JOIN.

    using IAST::IAST;
    String getID(char) const override { return "TablesInSelectQueryElement"; }
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};


/// The list. Elements are in 'children' field.
struct ASTTablesInSelectQuery : public IAST
{
    using IAST::IAST;
    String getID(char) const override { return "TablesInSelectQuery"; }
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
