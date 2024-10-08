#include <Interpreters/OptimizeDateOrDateTimeConverterWithPreimageVisitor.h>

#include <Core/Field.h>
#include <Core/NamesAndTypes.h>
#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/** Given a monotonic non-decreasing function f(x), which satisfies f(x) = c for any value x within [b, e).
 *  We could convert it into its equivalent form, x >= b AND x < e, which is free from the invocation of the function.
 *  And we could apply the similar transformation to other comparisons. The suggested transformations list:
 *
 *  f(x) == c -> x >= b AND x <  e
 *  f(x) != c -> x <  b OR  x >= e
 *  f(x) >  c -> x >= e
 *  f(x) >= c -> x >= b
 *  f(x) <  c -> x <  b
 *  f(x) <= c -> x <  e
 *
 *  This function generates a new AST with the transformed relation.
 */
ASTPtr generateOptimizedDateFilterAST(const String & comparator, const NameAndTypePair & column, const std::pair<Field, Field>& range)
{
    const DateLUTImpl & date_lut = DateLUT::instance("UTC");

    const String & column_name = column.name;
    String start_date_or_date_time;
    String end_date_or_date_time;

    if (isDateOrDate32(column.type.get()))
    {
        start_date_or_date_time = date_lut.dateToString(range.first.safeGet<DateLUTImpl::Time>());
        end_date_or_date_time = date_lut.dateToString(range.second.safeGet<DateLUTImpl::Time>());
    }
    else if (isDateTime(column.type.get()) || isDateTime64(column.type.get()))
    {
        start_date_or_date_time = date_lut.timeToString(range.first.safeGet<DateLUTImpl::Time>());
        end_date_or_date_time = date_lut.timeToString(range.second.safeGet<DateLUTImpl::Time>());
    }
    else [[unlikely]] return {};

    if (comparator == "equals")
    {
        return makeASTFunction("and",
                                makeASTFunction("greaterOrEquals",
                                            std::make_shared<ASTIdentifier>(column_name),
                                            std::make_shared<ASTLiteral>(start_date_or_date_time)
                                            ),
                                makeASTFunction("less",
                                            std::make_shared<ASTIdentifier>(column_name),
                                            std::make_shared<ASTLiteral>(end_date_or_date_time)
                                            )
                                );
    }
    if (comparator == "notEquals")
    {
        return makeASTFunction(
            "or",
            makeASTFunction("less", std::make_shared<ASTIdentifier>(column_name), std::make_shared<ASTLiteral>(start_date_or_date_time)),
            makeASTFunction(
                "greaterOrEquals", std::make_shared<ASTIdentifier>(column_name), std::make_shared<ASTLiteral>(end_date_or_date_time)));
    }
    if (comparator == "greater")
    {
        return makeASTFunction(
            "greaterOrEquals", std::make_shared<ASTIdentifier>(column_name), std::make_shared<ASTLiteral>(end_date_or_date_time));
    }
    if (comparator == "lessOrEquals")
    {
        return makeASTFunction("less", std::make_shared<ASTIdentifier>(column_name), std::make_shared<ASTLiteral>(end_date_or_date_time));
    }
    if (comparator == "less" || comparator == "greaterOrEquals")
    {
        return makeASTFunction(
            comparator, std::make_shared<ASTIdentifier>(column_name), std::make_shared<ASTLiteral>(start_date_or_date_time));
    }
    [[unlikely]] {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Expected equals, notEquals, less, lessOrEquals, greater, greaterOrEquals. Actual {}", comparator);
    }
}

void OptimizeDateOrDateTimeConverterWithPreimageMatcher::visit(const ASTFunction & function, ASTPtr & ast, const Data & data)
{
    const static std::unordered_map<String, String> swap_relations = {
        {"equals", "equals"},
        {"notEquals", "notEquals"},
        {"less", "greater"},
        {"greater", "less"},
        {"lessOrEquals", "greaterOrEquals"},
        {"greaterOrEquals", "lessOrEquals"},
    };

    if (!swap_relations.contains(function.name))
        return;

    if (!function.arguments || function.arguments->children.size() != 2)
        return;

    size_t func_id = function.arguments->children.size();

    for (size_t i = 0; i < function.arguments->children.size(); i++)
        if (const auto * /*func*/ _ = function.arguments->children[i]->as<ASTFunction>())
            func_id = i;

    if (func_id == function.arguments->children.size())
        return;

    size_t literal_id = 1 - func_id;
    const auto * literal = function.arguments->children[literal_id]->as<ASTLiteral>();

    if (!literal || literal->value.getType() != Field::Types::UInt64)
        return;

    String comparator = literal_id > func_id ? function.name : swap_relations.at(function.name);

    const auto * ast_func = function.arguments->children[func_id]->as<ASTFunction>();
    /// Currently we only handle single-argument functions.
    if (!ast_func || !ast_func->arguments || ast_func->arguments->children.size() != 1)
        return;

    const auto * column_id = ast_func->arguments->children.at(0)->as<ASTIdentifier>();
    if (!column_id)
        return;

    auto pos = IdentifierSemantic::getMembership(*column_id);
    if (!pos)
        pos = IdentifierSemantic::chooseTableColumnMatch(*column_id, data.tables, true);
    if (!pos)
        return;

    if (*pos >= data.tables.size())
        return;

    auto data_type_and_name = data.tables[*pos].columns.tryGetByName(column_id->shortName());
    if (!data_type_and_name)
        return;

    const auto column_type = data_type_and_name->type;
    if (!column_type || (!isDateOrDate32(*column_type) && !isDateTime(*column_type) && !isDateTime64(*column_type)))
        return;

    const auto & converter = FunctionFactory::instance().tryGet(ast_func->name, data.context);
    if (!converter)
        return;

    ColumnsWithTypeAndName args;
    args.emplace_back(column_type, "tmp");
    auto converter_base = converter->build(args);
    if (!converter_base || !converter_base->hasInformationAboutPreimage())
        return;

    auto preimage_range = converter_base->getPreimage(*column_type, literal->value);
    if (!preimage_range)
        return;

    const auto new_ast = generateOptimizedDateFilterAST(comparator, *data_type_and_name, *preimage_range);
    if (!new_ast)
        return;

    ast = new_ast;
}

bool OptimizeDateOrDateTimeConverterWithPreimageMatcher::needChildVisit(ASTPtr & ast, ASTPtr & /*child*/)
{
    const static std::unordered_set<String> relations = {
        "equals",
        "notEquals",
        "less",
        "greater",
        "lessOrEquals",
        "greaterOrEquals",
    };

    if (const auto * ast_function = ast->as<ASTFunction>())
    {
        return !relations.contains(ast_function->name);
    }

    return true;
}

}
