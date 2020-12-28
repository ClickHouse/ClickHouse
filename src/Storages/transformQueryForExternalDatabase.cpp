#include <Common/typeid_cast.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTExpressionList.h>
#include <Interpreters/misc.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <IO/WriteBufferFromString.h>
#include <Storages/transformQueryForExternalDatabase.h>
#include <Storages/MergeTree/KeyCondition.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
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
                result.type->serializeAsText(inner_column, 0, out, FormatSettings());
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

class FillInSetContents
{
public:
    struct Data {
        const SelectQueryInfo & query_info;
        Data(const SelectQueryInfo & query_info_in) : query_info(query_info_in) {} 
    };


    Data data;

    static bool needChildVisit(ASTPtr &, const ASTPtr &)
    {
        return true;
    }

    static void visit(ASTPtr & node, Data & data)
    {
        if (const auto * func = node->as<ASTFunction>())
        {
            std::string func_name = func->name;
            if (functionIsInOrGlobalInOperator(func_name))
            {
                LOG_WARNING(&Poco::Logger::get("setTransform"), "Found IN");
                const ASTs & args = func->arguments->children;
                const ASTPtr & right_arg = args[1];

                if (right_arg->as<ASTSubquery>() || right_arg->as<ASTIdentifier>())
                {
                    LOG_WARNING(&Poco::Logger::get("setTransform"), "Found Subquery");
                    auto set_it = data.query_info.sets.find(PreparedSetKey::forSubquery(*right_arg));
                    if (set_it == data.query_info.sets.end())
                        return;
                    LOG_WARNING(&Poco::Logger::get("setTransform"), "Found prepared set");

                    SetPtr prepared_set = set_it->second;
                    if (!prepared_set->hasExplicitSetElements())
                        return;

                    const Columns & set_columns = prepared_set->getSetElements();

                    LOG_WARNING(&Poco::Logger::get("setTransform"), "Found explicit prepared set {} {}", set_columns.size(), set_columns[0]->size());

                    node = std::make_shared<ASTFunction>();
                    node->as<ASTFunction>()->name = func_name;

                    auto in_args = std::make_shared<ASTExpressionList>();
                    node->as<ASTFunction>()->arguments = in_args;
                    in_args->children.emplace_back(args[0]);

                    auto inner_node = std::make_shared<ASTFunction>();
                    inner_node->as<ASTFunction>()->name = "tuple";

                    auto contents_node = std::make_shared<ASTExpressionList>();
                    inner_node->as<ASTFunction>()->arguments = contents_node;
                    inner_node->children.push_back(contents_node);
                    for (size_t j = 0; j < set_columns[0]->size(); j++) {
                        for (size_t i = 0; i < set_columns.size(); i++) {
                            contents_node->children.emplace_back(std::make_shared<ASTLiteral>((*set_columns[i])[j]));
                        }
                    }
                    in_args->children.emplace_back(contents_node);

                    LOG_WARNING(&Poco::Logger::get("setTransform"), "Made it to the end");
                }
            }
        }
    }
};

void replaceConstantExpressions(ASTPtr & node, const Context & context, const NamesAndTypesList & all_columns)
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

void fillInSetContents(ASTPtr & node, const SelectQueryInfo & query_info)
{
    LOG_WARNING(&Poco::Logger::get("setTransform"), "Running fillInSetContents");
    FillInSetContents::Data data(query_info);
    InDepthNodeVisitor<FillInSetContents, true> visitor(data);
    visitor.visit(node);
}

bool isCompatible(const IAST & node)
{
    if (const auto * function = node.as<ASTFunction>())
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
            || name == "tuple"))
            return false;

        /// A tuple with zero or one elements is represented by a function tuple(x) and is not compatible,
        /// but a normal tuple with more than one element is represented as a parenthesized expression (x, y) and is perfectly compatible.
        if (name == "tuple" && function->arguments->children.size() <= 1)
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

}


String transformQueryForExternalDatabase(
    const SelectQueryInfo & query_info,
    const NamesAndTypesList & available_columns,
    IdentifierQuotingStyle identifier_quoting_style,
    const String & database,
    const String & table,
    const Context & context)
{
    auto clone_query = query_info.query->clone();
    const Names used_columns = query_info.syntax_analyzer_result->requiredSourceColumns();

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
    if (original_where)
    {
        replaceConstantExpressions(original_where, context, available_columns);

        if (isCompatible(*original_where))
        {
            select->setExpression(ASTSelectQuery::Expression::WHERE, std::move(original_where));
        }
        else if (const auto * function = original_where->as<ASTFunction>())
        {
            if (function->name == "and")
            {
                bool compatible_found = false;
                auto new_function_and = makeASTFunction("and");
                for (const auto & elem : function->arguments->children)
                {
                    if (isCompatible(*elem))
                    {
                        new_function_and->arguments->children.push_back(elem);
                        compatible_found = true;
                    }
                }
                if (new_function_and->arguments->children.size() == 1)
                    new_function_and->name = "";

                if (compatible_found)
                    select->setExpression(ASTSelectQuery::Expression::WHERE, std::move(new_function_and));
            }
        }
    }

    ASTPtr select_ptr = select;
    dropAliases(select_ptr);
    fillInSetContents(select_ptr, query_info);

    WriteBufferFromOwnString out;
    IAST::FormatSettings settings(out, true);
    settings.identifier_quoting_style = identifier_quoting_style;
    settings.always_quote_identifiers = identifier_quoting_style != IdentifierQuotingStyle::None;

    select->format(settings);

    return out.str();
}

}
