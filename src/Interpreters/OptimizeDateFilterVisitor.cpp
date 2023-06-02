#include <Interpreters/OptimizeDateFilterVisitor.h>

#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>


namespace DB
{

ASTPtr generateOptimizedDateFilterAST(const String & comparator, const String & converter, const String & column, UInt64 compare_to)
{
    const DateLUTImpl & date_lut = DateLUT::instance();

    String start_date;
    String end_date;

    if (converter == "toYear")
    {
        UInt64 year = compare_to;
        start_date = date_lut.dateToString(date_lut.makeDayNum(year, 1, 1));
        end_date = date_lut.dateToString(date_lut.makeDayNum(year, 12, 31));
    }
    else if (converter == "toYYYYMM")
    {
        UInt64 year = compare_to / 100;
        UInt64 month = compare_to % 100;

        if (month == 0 || month > 12) return {};

        static constexpr UInt8 days_of_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

        bool leap_year = (year & 3) == 0 && (year % 100 || (year % 400 == 0 && year));

        start_date = date_lut.dateToString(date_lut.makeDayNum(year, month, 1));
        end_date = date_lut.dateToString(date_lut.makeDayNum(year, month, days_of_month[month - 1] + (leap_year && month == 2)));
    }
    else
    {
        return {};
    }

    if (comparator == "equals")
    {
        return makeASTFunction("and",
                                makeASTFunction("greaterOrEquals",
                                            std::make_shared<ASTIdentifier>(column),
                                            std::make_shared<ASTLiteral>(start_date)
                                            ),
                                makeASTFunction("lessOrEquals",
                                            std::make_shared<ASTIdentifier>(column),
                                            std::make_shared<ASTLiteral>(end_date)
                                            )
                                );
    }
    else if (comparator == "notEquals")
    {
        return makeASTFunction("or",
                                makeASTFunction("less",
                                            std::make_shared<ASTIdentifier>(column),
                                            std::make_shared<ASTLiteral>(start_date)
                                            ),
                                makeASTFunction("greater",
                                            std::make_shared<ASTIdentifier>(column),
                                            std::make_shared<ASTLiteral>(end_date)
                                            )
                                );
    }
    else if (comparator == "less" || comparator == "greaterOrEquals")
    {
        return makeASTFunction(comparator,
                    std::make_shared<ASTIdentifier>(column),
                    std::make_shared<ASTLiteral>(start_date)
                    );
    }
    else
    {
        return makeASTFunction(comparator,
                    std::make_shared<ASTIdentifier>(column),
                    std::make_shared<ASTLiteral>(end_date)
                    );
    }
}

bool rewritePredicateInPlace(ASTFunction & function, ASTPtr & ast)
{
    const static std::unordered_map<String, String> swap_relations = {
        {"equals", "equals"},
        {"notEquals", "notEquals"},
        {"less", "greater"},
        {"greater", "less"},
        {"lessOrEquals", "greaterOrEquals"},
        {"greaterOrEquals", "lessOrEquals"},
    };

    if (!swap_relations.contains(function.name)) return false;

    if (!function.arguments || function.arguments->children.size() != 2) return false;

    size_t func_id = function.arguments->children.size();

    for (size_t i = 0; i < function.arguments->children.size(); i++)
    {
        if (const auto * func = function.arguments->children[i]->as<ASTFunction>(); func)
        {
            if (func->name == "toYear" || func->name == "toYYYYMM")
            {
                func_id = i;
            }
        }
    }

    if (func_id == function.arguments->children.size()) return false;

    size_t literal_id = 1 - func_id;
    const auto * literal = function.arguments->children[literal_id]->as<ASTLiteral>();

    if (!literal || literal->value.getType() != Field::Types::UInt64) return false;

    UInt64 compare_to = literal->value.get<UInt64>();
    String comparator = literal_id > func_id ? function.name : swap_relations.at(function.name);

    const auto * func = function.arguments->children[func_id]->as<ASTFunction>();
    const auto * column_id = func->arguments->children.at(0)->as<ASTIdentifier>();

    if (!column_id) return false;

    String column = column_id->name();

    const auto new_ast = generateOptimizedDateFilterAST(comparator, func->name, column, compare_to);

    if (!new_ast) return false;

    ast = new_ast;
    return true;
}

void OptimizeDateFilterInPlaceData::visit(ASTFunction & function, ASTPtr & ast) const
{
    rewritePredicateInPlace(function, ast);
}
}
