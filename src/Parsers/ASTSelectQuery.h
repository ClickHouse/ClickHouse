#pragma once

#include <Parsers/IAST.h>
#include <Core/Names.h>

namespace DB
{

struct ASTTablesInSelectQueryElement;
struct StorageID;


/** SELECT query
  */
class ASTSelectQuery : public IAST
{
public:
    enum class Expression : uint8_t
    {
        WITH,
        SELECT,
        TABLES,
        PREWHERE,
        WHERE,
        GROUP_BY,
        HAVING,
        WINDOW,
        QUALIFY,
        ORDER_BY,
        LIMIT_BY_OFFSET,
        LIMIT_BY_LENGTH,
        LIMIT_BY,
        LIMIT_OFFSET,
        LIMIT_LENGTH,
        SETTINGS,
        INTERPOLATE
    };

    static String expressionToString(Expression expr)
    {
        switch (expr)
        {
            case Expression::WITH:
                return "WITH";
            case Expression::SELECT:
                return "SELECT";
            case Expression::TABLES:
                return "TABLES";
            case Expression::PREWHERE:
                return "PREWHERE";
            case Expression::WHERE:
                return "WHERE";
            case Expression::GROUP_BY:
                return "GROUP BY";
            case Expression::HAVING:
                return "HAVING";
            case Expression::WINDOW:
                return "WINDOW";
            case Expression::QUALIFY:
                return "QUALIFY";
            case Expression::ORDER_BY:
                return "ORDER BY";
            case Expression::LIMIT_BY_OFFSET:
                return "LIMIT BY OFFSET";
            case Expression::LIMIT_BY_LENGTH:
                return "LIMIT BY LENGTH";
            case Expression::LIMIT_BY:
                return "LIMIT BY";
            case Expression::LIMIT_OFFSET:
                return "LIMIT OFFSET";
            case Expression::LIMIT_LENGTH:
                return "LIMIT LENGTH";
            case Expression::SETTINGS:
                return "SETTINGS";
            case Expression::INTERPOLATE:
                return "INTERPOLATE";
        }
        return "";
    }

    /** Get the text that identifies this element. */
    String getID(char) const override { return "SelectQuery"; }

    ASTPtr clone() const override;

    bool recursive_with = false;
    bool distinct = false;
    bool group_by_all = false;
    bool group_by_with_totals = false;
    bool group_by_with_rollup = false;
    bool group_by_with_cube = false;
    bool group_by_with_constant_keys = false;
    bool group_by_with_grouping_sets = false;
    bool order_by_all = false;
    bool limit_with_ties = false;

    ASTPtr & refSelect()    { return getExpression(Expression::SELECT); }
    ASTPtr & refTables()    { return getExpression(Expression::TABLES); }
    ASTPtr & refPrewhere()  { return getExpression(Expression::PREWHERE); }
    ASTPtr & refWhere()     { return getExpression(Expression::WHERE); }
    ASTPtr & refHaving()    { return getExpression(Expression::HAVING); }
    ASTPtr & refQualify()    { return getExpression(Expression::QUALIFY); }

    ASTPtr with()           const { return getExpression(Expression::WITH); }
    ASTPtr select()         const { return getExpression(Expression::SELECT); }
    ASTPtr tables()         const { return getExpression(Expression::TABLES); }
    ASTPtr prewhere()       const { return getExpression(Expression::PREWHERE); }
    ASTPtr where()          const { return getExpression(Expression::WHERE); }
    ASTPtr groupBy()        const { return getExpression(Expression::GROUP_BY); }
    ASTPtr having()         const { return getExpression(Expression::HAVING); }
    ASTPtr window()         const { return getExpression(Expression::WINDOW); }
    ASTPtr qualify()         const { return getExpression(Expression::QUALIFY); }
    ASTPtr orderBy()        const { return getExpression(Expression::ORDER_BY); }
    ASTPtr limitByOffset()  const { return getExpression(Expression::LIMIT_BY_OFFSET); }
    ASTPtr limitByLength()  const { return getExpression(Expression::LIMIT_BY_LENGTH); }
    ASTPtr limitBy()        const { return getExpression(Expression::LIMIT_BY); }
    ASTPtr limitOffset()    const { return getExpression(Expression::LIMIT_OFFSET); }
    ASTPtr limitLength()    const { return getExpression(Expression::LIMIT_LENGTH); }
    ASTPtr settings()       const { return getExpression(Expression::SETTINGS); }
    ASTPtr interpolate()    const { return getExpression(Expression::INTERPOLATE); }

    bool hasFiltration() const { return where() || prewhere() || having() || qualify(); }

    /// Set/Reset/Remove expression.
    void setExpression(Expression expr, ASTPtr && ast);

    ASTPtr getExpression(Expression expr, bool clone = false) const
    {
        auto it = positions.find(expr);
        if (it != positions.end())
            return clone ? children[it->second]->clone() : children[it->second];
        return {};
    }

    /// Compatibility with old parser of tables list. TODO remove
    ASTPtr sampleSize() const;
    ASTPtr sampleOffset() const;
    std::pair<ASTPtr, bool> arrayJoinExpressionList() const;

    const ASTTablesInSelectQueryElement * join() const;
    bool hasJoin() const;
    bool final() const;
    bool withFill() const;
    void replaceDatabaseAndTable(const String & database_name, const String & table_name);
    void replaceDatabaseAndTable(const StorageID & table_id);
    void addTableFunction(ASTPtr & table_function_ptr);
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

    void setFinal();

    QueryKind getQueryKind() const override { return QueryKind::Select; }
    bool hasQueryParameters() const;

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

private:
    std::unordered_map<Expression, size_t> positions;

    /// This variable is optional as we want to set it on the first call to hasQueryParameters
    /// and return the same variable on future calls to hasQueryParameters
    /// its mutable as we set it in const function
    mutable std::optional<bool> has_query_parameters;

    ASTPtr & getExpression(Expression expr);
};

}
