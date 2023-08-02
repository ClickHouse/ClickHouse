#include <Common/typeid_cast.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/Context.h>
#include <IO/WriteBufferFromString.h>
#include <Storages/transformQueryForExternalDatabase.h>
#include <Storages/MergeTree/KeyCondition.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
}

namespace
{

/// Everything except numbers is put as string literal.
class ReplacingConstantExpressionsMatcherNumOrStr
{
public:
    using Data = Block;

    static bool needChildVisit(ASTPtr &, const ASTPtr &)
    {
        return true;
    }

    static void visit(ASTPtr & node, Block & block_with_constants)
    {
        if (!node->as<ASTFunction>())
            return;

        std::string name = node->getColumnName();
        if (block_with_constants.has(name))
        {
            auto result = block_with_constants.getByName(name);
            if (!isColumnConst(*result.column))
                return;

            if (result.column->isNullAt(0))
            {
                node = std::make_shared<ASTLiteral>(Field());
            }
            else if (isNumber(result.type))
            {
                node = std::make_shared<ASTLiteral>(assert_cast<const ColumnConst &>(*result.column).getField());
            }
            else
            {
                /// Everything except numbers is put as string literal. This is important for Date, DateTime, UUID.

                const IColumn & inner_column = assert_cast<const ColumnConst &>(*result.column).getDataColumn();

                WriteBufferFromOwnString out;
                result.type->getDefaultSerialization()->serializeText(inner_column, 0, out, FormatSettings());
                node = std::make_shared<ASTLiteral>(out.str());
            }
        }
    }
};

class DropAliasesMatcher
{
public:
    struct Data {};
    Data data;

    static bool needChildVisit(ASTPtr &, const ASTPtr &)
    {
        return true;
    }

    static void visit(ASTPtr & node, Data)
    {
        if (!node->tryGetAlias().empty())
            node->setAlias({});
    }
};

void replaceConstantExpressions(ASTPtr & node, ContextPtr context, const NamesAndTypesList & all_columns)
{
    auto syntax_result = TreeRewriter(context).analyze(node, all_columns);
    Block block_with_constants = KeyCondition::getBlockWithConstants(node, syntax_result, context);

    InDepthNodeVisitor<ReplacingConstantExpressionsMatcherNumOrStr, true> visitor(block_with_constants);
    visitor.visit(node);
}

void dropAliases(ASTPtr & node)
{
    DropAliasesMatcher::Data data;
    InDepthNodeVisitor<DropAliasesMatcher, true> visitor(data);
    visitor.visit(node);
}


bool isCompatible(IAST & node)
{
    if (auto * function = node.as<ASTFunction>())
    {
        if (function->parameters)   /// Parametric aggregate functions
            return false;

        if (!function->arguments)
            throw Exception("Logical error: function->arguments is not set", ErrorCodes::LOGICAL_ERROR);

        String name = function->name;

        if (!(name == "and"
            || name == "or"
            || name == "not"
            || name == "equals"
            || name == "notEquals"
            || name == "less"
            || name == "greater"
            || name == "lessOrEquals"
            || name == "greaterOrEquals"
            || name == "like"
            || name == "notLike"
            || name == "in"
            || name == "notIn"
            || name == "isNull"
            || name == "isNotNull"
            || name == "tuple"))
            return false;

        /// A tuple with zero or one elements is represented by a function tuple(x) and is not compatible,
        /// but a normal tuple with more than one element is represented as a parenthesized expression (x, y) and is perfectly compatible.
        /// So to support tuple with zero or one elements we can clear function name to get (x) instead of tuple(x)
        if (name == "tuple")
        {
            if (function->arguments->children.size() <= 1)
            {
                function->name.clear();
            }
        }

        /// If the right hand side of IN is a table identifier (example: x IN table), then it's not compatible.
        if ((name == "in" || name == "notIn")
            && (function->arguments->children.size() != 2 || function->arguments->children[1]->as<ASTTableIdentifier>()))
            return false;

        for (const auto & expr : function->arguments->children)
            if (!isCompatible(*expr))
                return false;

        return true;
    }

    if (const auto * literal = node.as<ASTLiteral>())
    {
        /// Foreign databases often have no support for Array. But Tuple literals are passed to support IN clause.
        return literal->value.getType() != Field::Types::Array;
    }

    return node.as<ASTIdentifier>();
}

bool removeUnknownSubexpressions(ASTPtr & node, const NameSet & known_names);

void removeUnknownChildren(ASTs & children, const NameSet & known_names)
{

    ASTs new_children;
    for (auto & child : children)
    {
        bool leave_child = removeUnknownSubexpressions(child, known_names);
        if (leave_child)
            new_children.push_back(child);
    }
    children = std::move(new_children);
}

/// return `true` if we should leave node in tree
bool removeUnknownSubexpressions(ASTPtr & node, const NameSet & known_names)
{
    if (const auto * ident = node->as<ASTIdentifier>())
        return known_names.contains(ident->name());

    if (node->as<ASTLiteral>() != nullptr)
        return true;

    auto * func = node->as<ASTFunction>();
    if (func && (func->name == "and" || func->name == "or"))
    {
        removeUnknownChildren(func->arguments->children, known_names);
        /// all children removed, current node can be removed too
        if (func->arguments->children.size() == 1)
        {
            /// if only one child left, pull it on top level
            node = func->arguments->children[0];
            return true;
        }
        return !func->arguments->children.empty();
    }

    bool leave_child = true;
    for (auto & child : node->children)
    {
        leave_child = leave_child && removeUnknownSubexpressions(child, known_names);
        if (!leave_child)
            break;
    }
    return leave_child;
}

// When a query references an external table such as table from MySQL database,
// the corresponding table storage has to execute the relevant part of the query. We
// send the query to the storage as AST. Before that, we have to remove the conditions
// that reference other tables from `WHERE`, so that the external engine is not confused
// by the unknown columns.
bool removeUnknownSubexpressionsFromWhere(ASTPtr & node, const NamesAndTypesList & available_columns)
{
    if (!node)
        return false;

    NameSet known_names;
    for (const auto & col : available_columns)
        known_names.insert(col.name);

    if (auto * expr_list = node->as<ASTExpressionList>(); expr_list && !expr_list->children.empty())
    {
        /// traverse expression list on top level
        removeUnknownChildren(expr_list->children, known_names);
        return !expr_list->children.empty();
    }
    return removeUnknownSubexpressions(node, known_names);
}

}

String transformQueryForExternalDatabase(
    const SelectQueryInfo & query_info,
    const NamesAndTypesList & available_columns,
    IdentifierQuotingStyle identifier_quoting_style,
    const String & database,
    const String & table,
    ContextPtr context)
{
    auto clone_query = query_info.query->clone();
    const Names used_columns = query_info.syntax_analyzer_result->requiredSourceColumns();
    bool strict = context->getSettingsRef().external_table_strict_query;

    auto select = std::make_shared<ASTSelectQuery>();

    select->replaceDatabaseAndTable(database, table);

    auto select_expr_list = std::make_shared<ASTExpressionList>();
    for (const auto & name : used_columns)
        select_expr_list->children.push_back(std::make_shared<ASTIdentifier>(name));

    select->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_expr_list));

    /** If there was WHERE,
      * copy it to transformed query if it is compatible,
      * or if it is AND expression,
      * copy only compatible parts of it.
      */

    ASTPtr original_where = clone_query->as<ASTSelectQuery &>().where();
    bool where_has_known_columns = removeUnknownSubexpressionsFromWhere(original_where, available_columns);
    if (original_where && where_has_known_columns)
    {
        replaceConstantExpressions(original_where, context, available_columns);

        if (isCompatible(*original_where))
        {
            select->setExpression(ASTSelectQuery::Expression::WHERE, std::move(original_where));
        }
        else if (strict)
        {
            throw Exception("Query contains non-compatible expressions (and external_table_strict_query=true)", ErrorCodes::INCORRECT_QUERY);
        }
        else if (const auto * function = original_where->as<ASTFunction>())
        {
            if (function->name == "and")
            {
                auto new_function_and = makeASTFunction("and");
                for (const auto & elem : function->arguments->children)
                {
                    if (isCompatible(*elem))
                        new_function_and->arguments->children.push_back(elem);
                }
                if (new_function_and->arguments->children.size() == 1)
                    select->setExpression(ASTSelectQuery::Expression::WHERE, std::move(new_function_and->arguments->children[0]));
                else if (new_function_and->arguments->children.size() > 1)
                    select->setExpression(ASTSelectQuery::Expression::WHERE, std::move(new_function_and));
            }
        }
    }
    else if (strict && original_where)
    {
        throw Exception("Query contains non-compatible expressions (and external_table_strict_query=true)", ErrorCodes::INCORRECT_QUERY);
    }

    auto * literal_expr = typeid_cast<ASTLiteral *>(original_where.get());
    UInt64 value;
    if (literal_expr && literal_expr->value.tryGet<UInt64>(value) && (value == 0 || value == 1))
    {
        /// WHERE 1 -> WHERE 1=1, WHERE 0 -> WHERE 1=0.
        if (value)
            original_where = makeASTFunction("equals", std::make_shared<ASTLiteral>(1), std::make_shared<ASTLiteral>(1));
        else
            original_where = makeASTFunction("equals", std::make_shared<ASTLiteral>(1), std::make_shared<ASTLiteral>(0));
        select->setExpression(ASTSelectQuery::Expression::WHERE, std::move(original_where));
    }

    ASTPtr select_ptr = select;
    dropAliases(select_ptr);

    WriteBufferFromOwnString out;
    IAST::FormatSettings settings(out, true);
    settings.identifier_quoting_style = identifier_quoting_style;
    settings.always_quote_identifiers = identifier_quoting_style != IdentifierQuotingStyle::None;

    select->format(settings);

    return out.str();
}

}
