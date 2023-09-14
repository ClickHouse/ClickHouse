#include <Functions/FunctionsComparison.h>
#include <Functions/FunctionsLogical.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/OuterToInnerJoinVisitor.h>
#include <Interpreters/misc.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserTablesInSelectQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Common/typeid_cast.h>

#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


namespace
{

    struct JoinedElement
    {
        explicit JoinedElement(const ASTTablesInSelectQueryElement & table_element) : element(table_element)
        {
            if (element.table_join)
            {
                join = element.table_join->as<ASTTableJoin>();
                original_kind = join->kind;
            }
        }

        void checkTableName(const DatabaseAndTableWithAlias & table, const String & current_database) const
        {
            if (!element.table_expression)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Not a table expression in JOIN (ARRAY JOIN?)");

            ASTTableExpression * table_expression = element.table_expression->as<ASTTableExpression>();
            if (!table_expression)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong table expression in JOIN");

            if (!table.same(DatabaseAndTableWithAlias(*table_expression, current_database)))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Inconsistent table names");
        }


        bool rewriteLeftOuterToInner(bool right_could_filter_null)
        {
            if (join->kind != JoinKind::Left)
                return false;

            if (!right_could_filter_null)
                return false;

            join->kind = JoinKind::Inner;
            return true;
        }

        bool rewriteRightOuterToInner(bool left_could_filter_null)
        {
            if (join->kind != JoinKind::Right)
                return false;

            if (!left_could_filter_null)
                return false;

            join->kind = JoinKind::Inner;
            return true;
        }

        bool rewriteFullOuterToInner(bool left_could_filter_null, bool right_could_filter_null)
        {
            if (join->kind != JoinKind::Full)
                return false;

            if (!left_could_filter_null && !right_could_filter_null)
                return false;

            if (left_could_filter_null && right_could_filter_null)
            {
                join->kind = JoinKind::Inner;
            }
            else if (left_could_filter_null && !right_could_filter_null)
            {
                join->kind = JoinKind::Left;
            }
            else if (!left_could_filter_null && right_could_filter_null)
            {
                join->kind = JoinKind::Right;
            }
            return true;
        }

        ASTPtr arrayJoin() const { return element.array_join; }
        const ASTTableJoin * tableJoin() const { return join; }

    private:
        const ASTTablesInSelectQueryElement & element;
        ASTTableJoin * join = nullptr;

        JoinKind original_kind;
    };

    bool couldFilterNull(const ASTFunction & function, const std::optional<size_t> & leftPos, const std::optional<size_t> & rightPos)
    {
        const DB::String & func_name = function.name;
        if (leftPos.has_value())
        {
            auto * literal = function.arguments->children[1]->as<ASTLiteral>();
            if (func_name == NameGreater::name)
            {
                return literal;
            }

            if (func_name == NameEquals::name || func_name == NameGreaterOrEquals::name)
            {
                return literal && !literal->isNull();
            }

            if (func_name == NameNotEquals::name)
            {
                return literal && literal->isNull();
            }
            return false;
        }
        else if (rightPos.has_value())
        {
            auto * literal = function.arguments->children[0]->as<ASTLiteral>();
            if (func_name == NameLess::name)
            {
                return literal;
            }

            if (func_name == NameEquals::name || func_name == NameLessOrEquals::name)
            {
                return literal && !literal->isNull();
            }

            if (func_name == NameNotEquals::name)
            {
                return literal && literal->isNull();
            }
            return false;
        }
        return false;
    }

    // whether the table have predicates that can filter null， 1: true, 0: false.
    std::vector<char>
    getFilterNullExpression(const ASTPtr & ast, const std::vector<TableWithColumnNamesAndTypes> & tables, const Aliases & aliases)
    {
        std::vector<char> filter_null_expression(tables.size(), false);
        for (const auto & node : splitConjunctionsAst(ast))
        {
            if (const auto * func = node->as<ASTFunction>(); func)
            {
                if (!func->arguments || func->arguments->children.size() != 2)
                    continue;

                auto left_table_pos = IdentifierSemantic::getIdentsMembership(func->arguments->children[0], tables, aliases);
                auto right_table_pos = IdentifierSemantic::getIdentsMembership(func->arguments->children[1], tables, aliases);

                if (left_table_pos.has_value() ^ right_table_pos.has_value())
                {
                    size_t table_pos = left_table_pos.has_value() ? *left_table_pos : *right_table_pos;
                    filter_null_expression[table_pos] |= couldFilterNull(*func, left_table_pos, right_table_pos);
                }
            }
        }
        return filter_null_expression;
    }


    std::vector<JoinedElement> getTables(const ASTSelectQuery & select)
    {
        if (!select.tables())
            return {};

        const auto * tables = select.tables()->as<ASTTablesInSelectQuery>();
        if (!tables)
            return {};

        size_t num_tables = tables->children.size();
        if (num_tables < 2)
            return {};

        std::vector<JoinedElement> joined_tables;
        joined_tables.reserve(num_tables);

        for (const auto & child : tables->children)
        {
            const auto * table_element = child->as<ASTTablesInSelectQueryElement>();
            if (!table_element)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: TablesInSelectQueryElement expected");

            JoinedElement & t = joined_tables.emplace_back(*table_element);
            if (t.arrayJoin())
                return {};
        }

        return joined_tables;
    }

}

bool OuterToInnerJoinMatcher::needChildVisit(ASTPtr & node, const ASTPtr &)
{
    return !node->as<ASTSubquery>();
}

void OuterToInnerJoinMatcher::visit(ASTPtr & ast, Data & data)
{
    if (auto * t = ast->as<ASTSelectQuery>())
        visit(*t, ast, data);
}

void OuterToInnerJoinMatcher::visit(ASTSelectQuery & select, ASTPtr &, Data & data)
{
    std::vector<JoinedElement> joined_tables = getTables(select);
    if (joined_tables.empty())
        return;

    {
        if (joined_tables.size() != data.tables_with_columns.size())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Logical error: inconsistent number of tables: {} != {}",
                joined_tables.size(),
                data.tables_with_columns.size());

        for (size_t i = 0; i < joined_tables.size(); ++i)
            joined_tables[i].checkTableName(data.tables_with_columns[i].table, data.current_database);
    }

    /// OUTER to INNER
    if (data.outer_to_inner_join_rewrite && select.where())
    {
        auto table_could_filter_null = getFilterNullExpression(select.where(), data.tables_with_columns, data.aliases);
        /// An AST with join is a left deep tree.
        /// left_table_could_filter_null indicates that the left subtree of the current join node contains a predicate that can filter null values.
        /// right_table_could_filter_null indicates that the right subtree of the current join node contains predicates that can filter null values.
        std::vector<char> left_table_could_filter_null(joined_tables.size(), false);
        std::vector<char> right_table_could_filter_null(joined_tables.size(), false);

        for (size_t i = 1; i < joined_tables.size(); ++i)
        {
            left_table_could_filter_null[i] = left_table_could_filter_null[i - 1] || table_could_filter_null[i - 1];
        }

        for (size_t i = joined_tables.size() - 1; i > 0; --i)
        {
            right_table_could_filter_null[i] = table_could_filter_null[i];
        }
        for (size_t i = joined_tables.size() - 1; i > 0; --i)
        {
            auto & joined = joined_tables[i];
            String query_before = queryToString(*joined.tableJoin());
            bool rewritten = false;

            if (!joined.tableJoin())
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong table expression in JOIN");
            }

            if (joined.tableJoin()->kind == JoinKind::Left)
            {
                rewritten = joined.rewriteLeftOuterToInner(right_table_could_filter_null[i]);
            }
            else if (joined.tableJoin()->kind == JoinKind::Right)
            {
                rewritten = joined.rewriteRightOuterToInner(left_table_could_filter_null[i]);
            }
            else if (joined.tableJoin()->kind == JoinKind::Full)
            {
                rewritten = joined.rewriteFullOuterToInner(left_table_could_filter_null[i], right_table_could_filter_null[i]);
            }

            if (rewritten)
            {
                LOG_DEBUG(
                    &Poco::Logger::get("OuterToInnerJoin"), "Rewritten '{}' to '{}'", query_before, queryToString(*joined.tableJoin()));
            }
        }
    }
}

}
