#include "QueryFuzzer.h"

#include <unordered_set>

#include <pcg_random.hpp>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <Core/Types.h>
#include <IO/Operators.h>
#include <IO/UseSSL.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTUseQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>

namespace DB
{

Field QueryFuzzer::getRandomField(int type)
{
    switch (type)
    {
    case 0:
    {
        static constexpr Int64 values[]
                = {-2, -1, 0, 1, 2, 3, 7, 10, 100, 255, 256, 257, 1023, 1024,
                   1025, 65535, 65536, 65537, 1024 * 1024 - 1, 1024 * 1024,
                   1024 * 1024 + 1, INT64_MIN, INT64_MAX};
        return values[fuzz_rand() % (sizeof(values) / sizeof(*values))];
    }
    case 1:
    {
        static constexpr float values[]
                = {NAN, INFINITY, -INFINITY, 0., 0.0001, 0.5, 0.9999,
                   1., 1.0001, 2., 10.0001, 100.0001, 1000.0001};
        return values[fuzz_rand() % (sizeof(values) / sizeof(*values))];
    }
    case 2:
    {
        static constexpr Int64 values[]
                = {-2, -1, 0, 1, 2, 3, 7, 10, 100, 255, 256, 257, 1023, 1024,
                   1025, 65535, 65536, 65537, 1024 * 1024 - 1, 1024 * 1024,
                   1024 * 1024 + 1, INT64_MIN, INT64_MAX};
        static constexpr UInt64 scales[] = {0, 1, 2, 10};
        return DecimalField<Decimal64>(
                    values[fuzz_rand() % (sizeof(values) / sizeof(*values))],
                scales[fuzz_rand() % (sizeof(scales) / sizeof(*scales))]
                );
    }
    default:
        assert(false);
        return Null{};
    }
}

Field QueryFuzzer::fuzzField(Field field)
{
    const auto type = field.getType();

    int type_index = -1;

    if (type == Field::Types::Int64
        || type == Field::Types::UInt64)
    {
        type_index = 0;
    }
    else if (type == Field::Types::Float64)
    {
        type_index = 1;
    }
    else if (type == Field::Types::Decimal32
             || type == Field::Types::Decimal64
             || type == Field::Types::Decimal128)
    {
        type_index = 2;
    }

    if (fuzz_rand() % 20 == 0)
    {
        return Null{};
    }

    if (type_index >= 0)
    {
        if (fuzz_rand() % 20 == 0)
        {
            // Change type sometimes, but not often, because it mostly leads to
            // boring errors.
            type_index = fuzz_rand() % 3;
        }
        return getRandomField(type_index);
    }

    if (type == Field::Types::String)
    {
        auto & str = field.get<std::string>();
        UInt64 action = fuzz_rand() % 10;
        switch (action)
        {
        case 0:
            str = "";
            break;
        case 1:
            str = str + str;
            break;
        case 2:
            str = str + str + str + str;
            break;
        case 4:
            if (!str.empty())
            {
                str[fuzz_rand() % str.size()] = '\0';
            }
            break;
        default:
            // Do nothing
            break;
        }
    }
    else if (type == Field::Types::Array || type == Field::Types::Tuple)
    {
        auto & arr = field.reinterpret<FieldVector>();

        if (fuzz_rand() % 5 == 0 && !arr.empty())
        {
            size_t pos = fuzz_rand() % arr.size();
            arr.erase(arr.begin() + pos);
            fprintf(stderr, "erased\n");
        }

        if (fuzz_rand() % 5 == 0)
        {
            if (!arr.empty())
            {
                size_t pos = fuzz_rand() % arr.size();
                arr.insert(arr.begin() + pos, fuzzField(arr[pos]));
                fprintf(stderr, "inserted (pos %zd)\n", pos);
            }
            else
            {
                arr.insert(arr.begin(), getRandomField(0));
                fprintf(stderr, "inserted (0)\n");
            }

        }

        for (auto & element : arr)
        {
            element = fuzzField(element);
        }
    }

    return field;
}

ASTPtr QueryFuzzer::getRandomColumnLike()
{
    if (column_like.empty())
    {
        return nullptr;
    }

    ASTPtr new_ast = column_like[fuzz_rand() % column_like.size()]->clone();
    new_ast->setAlias("");

    return new_ast;
}

void QueryFuzzer::replaceWithColumnLike(ASTPtr & ast)
{
    if (column_like.empty())
    {
        return;
    }

    std::string old_alias = ast->tryGetAlias();
    ast = getRandomColumnLike();
    ast->setAlias(old_alias);
}

void QueryFuzzer::replaceWithTableLike(ASTPtr & ast)
{
    if (table_like.empty())
    {
        return;
    }

    ASTPtr new_ast = table_like[fuzz_rand() % table_like.size()]->clone();

    std::string old_alias = ast->tryGetAlias();
    new_ast->setAlias(old_alias);

    ast = new_ast;
}

void QueryFuzzer::fuzzColumnLikeExpressionList(ASTPtr ast)
{
    if (!ast)
    {
        return;
    }

    auto * impl = assert_cast<ASTExpressionList *>(ast.get());
    if (fuzz_rand() % 50 == 0 && impl->children.size() > 1)
    {
        // Don't remove last element -- this leads to questionable
        // constructs such as empty select.
        impl->children.erase(impl->children.begin()
                             + fuzz_rand() % impl->children.size());
    }
    if (fuzz_rand() % 50 == 0)
    {
        auto pos = impl->children.empty()
                ? impl->children.begin()
                : impl->children.begin() + fuzz_rand() % impl->children.size();
        auto col = getRandomColumnLike();
        if (col)
        {
            impl->children.insert(pos, col);
        }
        else
        {
            fprintf(stderr, "no random col!\n");
        }
    }
}

void QueryFuzzer::fuzz(ASTs & asts)
{
    for (auto & ast : asts)
    {
        fuzz(ast);
    }
}

void QueryFuzzer::fuzz(ASTPtr & ast)
{
    if (!ast)
        return;

    if (auto * with_union = typeid_cast<ASTSelectWithUnionQuery *>(ast.get()))
    {
        fuzz(with_union->list_of_selects);
    }
    else if (auto * tables = typeid_cast<ASTTablesInSelectQuery *>(ast.get()))
    {
        fuzz(tables->children);
    }
    else if (auto * tables_element = typeid_cast<ASTTablesInSelectQueryElement *>(ast.get()))
    {
        fuzz(tables_element->table_join);
        fuzz(tables_element->table_expression);
        fuzz(tables_element->array_join);
    }
    else if (auto * table_expr = typeid_cast<ASTTableExpression *>(ast.get()))
    {
        fuzz(table_expr->database_and_table_name);
        fuzz(table_expr->subquery);
        fuzz(table_expr->table_function);
    }
    else if (auto * expr_list = typeid_cast<ASTExpressionList *>(ast.get()))
    {
        fuzz(expr_list->children);
    }
    else if (auto * fn = typeid_cast<ASTFunction *>(ast.get()))
    {
        fuzzColumnLikeExpressionList(fn->arguments);
        fuzzColumnLikeExpressionList(fn->parameters);

        fuzz(fn->children);
    }
    else if (auto * select = typeid_cast<ASTSelectQuery *>(ast.get()))
    {
        fuzzColumnLikeExpressionList(select->select());
        fuzzColumnLikeExpressionList(select->groupBy());

        fuzz(select->children);
    }
    else if (auto * literal = typeid_cast<ASTLiteral *>(ast.get()))
    {
        // Only change the queries sometimes.
        int r = fuzz_rand() % 10;
        if (r == 0)
        {
            literal->value = fuzzField(literal->value);
        }
        else if (r == 1)
        {
            /* replace with a random function? */
        }
        else if (r == 2)
        {
            /* replace with something column-like */
            replaceWithColumnLike(ast);
        }
    }
    else
    {
        fuzz(ast->children);
    }
}

/*
 * This functions collects various parts of query that we can then substitute
 * to a query being fuzzed.
 *
 * TODO: we just stop remembering new parts after our corpus reaches certain size.
 * This is boring, should implement a random replacement of existing parst with
 * small probability. Do this after we add this fuzzer to CI and fix all the
 * problems it can routinely find even in this boring version.
 */
void QueryFuzzer::collectFuzzInfoMain(const ASTPtr ast)
{
    collectFuzzInfoRecurse(ast);

    aliases.clear();
    for (const auto & alias : aliases_set)
    {
        aliases.push_back(alias);
    }

    column_like.clear();
    for (const auto & [name, value] : column_like_map)
    {
        column_like.push_back(value);
    }

    table_like.clear();
    for (const auto & [name, value] : table_like_map)
    {
        table_like.push_back(value);
    }
}

void QueryFuzzer::addTableLike(const ASTPtr ast)
{
    if (table_like_map.size() > 1000)
    {
        return;
    }

    const auto name = ast->formatForErrorMessage();
    if (name.size() < 200)
    {
        table_like_map.insert({name, ast});
    }
}

void QueryFuzzer::addColumnLike(const ASTPtr ast)
{
    if (column_like_map.size() > 1000)
    {
        return;
    }

    const auto name = ast->formatForErrorMessage();
    if (name.size() < 200)
    {
        column_like_map.insert({name, ast});
    }
}

void QueryFuzzer::collectFuzzInfoRecurse(const ASTPtr ast)
{
    if (auto * impl = dynamic_cast<ASTWithAlias *>(ast.get()))
    {
        if (aliases_set.size() < 1000)
        {
            aliases_set.insert(impl->alias);
        }
    }

    if (typeid_cast<ASTLiteral *>(ast.get()))
    {
        addColumnLike(ast);
    }
    else if (typeid_cast<ASTIdentifier *>(ast.get()))
    {
        addColumnLike(ast);
    }
    else if (typeid_cast<ASTFunction *>(ast.get()))
    {
        addColumnLike(ast);
    }
    else if (typeid_cast<ASTTableExpression *>(ast.get()))
    {
        addTableLike(ast);
    }
    else if (typeid_cast<ASTSubquery *>(ast.get()))
    {
        addTableLike(ast);
    }

    for (const auto & child : ast->children)
    {
        collectFuzzInfoRecurse(child);
    }
}

void QueryFuzzer::fuzzMain(ASTPtr & ast)
{
    collectFuzzInfoMain(ast);
    fuzz(ast);

    std::cout << std::endl;
    formatAST(*ast, std::cout, false /*highlight*/);
    std::cout << std::endl << std::endl;
}

}
