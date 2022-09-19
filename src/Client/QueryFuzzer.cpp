#include "QueryFuzzer.h"

#include <unordered_set>

#include <pcg_random.hpp>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <Core/Types.h>
#include <IO/Operators.h>
#include <IO/UseSSL.h>
#include <IO/WriteBufferFromOStream.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTUseQuery.h>
#include <Parsers/ASTWindowDefinition.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_DEEP_RECURSION;
}

Field QueryFuzzer::getRandomField(int type)
{
    static constexpr Int64 bad_int64_values[]
        = {-2, -1, 0, 1, 2, 3, 7, 10, 100, 255, 256, 257, 1023, 1024,
           1025, 65535, 65536, 65537, 1024 * 1024 - 1, 1024 * 1024,
           1024 * 1024 + 1, INT_MIN - 1ll, INT_MIN, INT_MIN + 1,
           INT_MAX - 1, INT_MAX, INT_MAX + 1ll, INT64_MIN, INT64_MIN + 1,
           INT64_MAX - 1, INT64_MAX};
    switch (type)
    {
    case 0:
    {
        return bad_int64_values[fuzz_rand() % (sizeof(bad_int64_values)
                / sizeof(*bad_int64_values))];
    }
    case 1:
    {
        static constexpr float values[]
                = {NAN, INFINITY, -INFINITY, 0., -0., 0.0001, 0.5, 0.9999,
                   1., 1.0001, 2., 10.0001, 100.0001, 1000.0001, 1e10, 1e20,
                  FLT_MIN, FLT_MIN + FLT_EPSILON, FLT_MAX, FLT_MAX + FLT_EPSILON}; return values[fuzz_rand() % (sizeof(values) / sizeof(*values))];
    }
    case 2:
    {
        static constexpr UInt64 scales[] = {0, 1, 2, 10};
        return DecimalField<Decimal64>(
            bad_int64_values[fuzz_rand() % (sizeof(bad_int64_values)
                / sizeof(*bad_int64_values))],
            scales[fuzz_rand() % (sizeof(scales) / sizeof(*scales))]);
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
            std::cerr << "erased\n";
        }

        if (fuzz_rand() % 5 == 0)
        {
            if (!arr.empty())
            {
                size_t pos = fuzz_rand() % arr.size();
                arr.insert(arr.begin() + pos, fuzzField(arr[pos]));
                std::cerr << fmt::format("inserted (pos {})\n", pos);
            }
            else
            {
                arr.insert(arr.begin(), getRandomField(0));
                std::cerr << "inserted (0)\n";
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

void QueryFuzzer::fuzzOrderByElement(ASTOrderByElement * elem)
{
    switch (fuzz_rand() % 10)
    {
        case 0:
            elem->direction = -1;
            break;
        case 1:
            elem->direction = 1;
            break;
        case 2:
            elem->nulls_direction = -1;
            elem->nulls_direction_was_explicitly_specified = true;
            break;
        case 3:
            elem->nulls_direction = 1;
            elem->nulls_direction_was_explicitly_specified = true;
            break;
        case 4:
            elem->nulls_direction = elem->direction;
            elem->nulls_direction_was_explicitly_specified = false;
            break;
        default:
            // do nothing
            break;
    }
}

void QueryFuzzer::fuzzOrderByList(IAST * ast)
{
    if (!ast)
    {
        return;
    }

    auto * list = assert_cast<ASTExpressionList *>(ast);

    // Remove element
    if (fuzz_rand() % 50 == 0 && list->children.size() > 1)
    {
        // Don't remove last element -- this leads to questionable
        // constructs such as empty select.
        list->children.erase(list->children.begin()
                             + fuzz_rand() % list->children.size());
    }

    // Add element
    if (fuzz_rand() % 50 == 0)
    {
        auto pos = list->children.empty()
                ? list->children.begin()
                : list->children.begin() + fuzz_rand() % list->children.size();
        auto col = getRandomColumnLike();
        if (col)
        {
            auto elem = std::make_shared<ASTOrderByElement>();
            elem->children.push_back(col);
            elem->direction = 1;
            elem->nulls_direction = 1;
            elem->nulls_direction_was_explicitly_specified = false;
            elem->with_fill = false;

            list->children.insert(pos, elem);
        }
        else
        {
            std::cerr << "No random column.\n";
        }
    }

    // We don't have to recurse here to fuzz the children, this is handled by
    // the generic recursion into IAST.children.
}

void QueryFuzzer::fuzzColumnLikeExpressionList(IAST * ast)
{
    if (!ast)
    {
        return;
    }

    auto * impl = assert_cast<ASTExpressionList *>(ast);

    // Remove element
    if (fuzz_rand() % 50 == 0 && impl->children.size() > 1)
    {
        // Don't remove last element -- this leads to questionable
        // constructs such as empty select.
        impl->children.erase(impl->children.begin()
                             + fuzz_rand() % impl->children.size());
    }

    // Add element
    if (fuzz_rand() % 50 == 0)
    {
        auto pos = impl->children.empty()
                ? impl->children.begin()
                : impl->children.begin() + fuzz_rand() % impl->children.size();
        auto col = getRandomColumnLike();
        if (col)
            impl->children.insert(pos, col);
        else
            std::cerr << "No random column.\n";
    }

    // We don't have to recurse here to fuzz the children, this is handled by
    // the generic recursion into IAST.children.
}

void QueryFuzzer::fuzzWindowFrame(ASTWindowDefinition & def)
{
    switch (fuzz_rand() % 40)
    {
        case 0:
        {
            const auto r = fuzz_rand() % 3;
            def.frame_type = r == 0 ? WindowFrame::FrameType::ROWS
                : r == 1 ? WindowFrame::FrameType::RANGE
                    : WindowFrame::FrameType::GROUPS;
            break;
        }
        case 1:
        {
            const auto r = fuzz_rand() % 3;
            def.frame_begin_type = r == 0 ? WindowFrame::BoundaryType::Unbounded
                : r == 1 ? WindowFrame::BoundaryType::Current
                    : WindowFrame::BoundaryType::Offset;

            if (def.frame_begin_type == WindowFrame::BoundaryType::Offset)
            {
                // The offsets are fuzzed normally through 'children'.
                def.frame_begin_offset
                    = std::make_shared<ASTLiteral>(getRandomField(0));
            }
            else
            {
                def.frame_begin_offset = nullptr;
            }
            break;
        }
        case 2:
        {
            const auto r = fuzz_rand() % 3;
            def.frame_end_type = r == 0 ? WindowFrame::BoundaryType::Unbounded
                : r == 1 ? WindowFrame::BoundaryType::Current
                    : WindowFrame::BoundaryType::Offset;

            if (def.frame_end_type == WindowFrame::BoundaryType::Offset)
            {
                def.frame_end_offset
                    = std::make_shared<ASTLiteral>(getRandomField(0));
            }
            else
            {
                def.frame_end_offset = nullptr;
            }
            break;
        }
        case 5:
        {
            def.frame_begin_preceding = fuzz_rand() % 2;
            break;
        }
        case 6:
        {
            def.frame_end_preceding = fuzz_rand() % 2;
            break;
        }
        default:
            break;
    }

    if (def.frame_type == WindowFrame::FrameType::RANGE
        && def.frame_begin_type == WindowFrame::BoundaryType::Unbounded
        && def.frame_begin_preceding
        && def.frame_end_type == WindowFrame::BoundaryType::Current)
    {
        def.frame_is_default = true; /* NOLINT clang-tidy could you just shut up please */
    }
    else
    {
        def.frame_is_default = false;
    }
}

void QueryFuzzer::fuzz(ASTs & asts)
{
    for (auto & ast : asts)
    {
        fuzz(ast);
    }
}

struct ScopedIncrement
{
    size_t & counter;

    explicit ScopedIncrement(size_t & counter_) : counter(counter_) { ++counter; }
    ~ScopedIncrement() { --counter; }
};

void QueryFuzzer::fuzz(ASTPtr & ast)
{
    if (!ast)
        return;

    // Check for exceeding max depth.
    ScopedIncrement depth_increment(current_ast_depth);
    if (current_ast_depth > 500)
    {
        // The AST is too deep (see the comment for current_ast_depth). Throw
        // an exception to fail fast and not use this query as an etalon, or we'll
        // end up in a very slow and useless loop. It also makes sense to set it
        // lower than the default max parse depth on the server (1000), so that
        // we don't get the useless error about parse depth from the server either.
        throw Exception(ErrorCodes::TOO_DEEP_RECURSION,
            "AST depth exceeded while fuzzing ({})", current_ast_depth);
    }

    // Check for loops.
    auto [_, inserted] = debug_visited_nodes.insert(ast.get());
    if (!inserted)
    {
        fmt::print(stderr, "The AST node '{}' was already visited before."
            " Depth {}, {} visited nodes, current top AST:\n{}\n",
            static_cast<void *>(ast.get()), current_ast_depth,
            debug_visited_nodes.size(), (*debug_top_ast)->dumpTree());
        assert(false);
    }

    // The fuzzing.
    if (auto * with_union = typeid_cast<ASTSelectWithUnionQuery *>(ast.get()))
    {
        fuzz(with_union->list_of_selects);
    }
    else if (auto * with_intersect_except = typeid_cast<ASTSelectIntersectExceptQuery *>(ast.get()))
    {
        auto selects = with_intersect_except->getListOfSelects();
        fuzz(selects);
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
        fuzz(table_expr->children);
    }
    else if (auto * expr_list = typeid_cast<ASTExpressionList *>(ast.get()))
    {
        fuzz(expr_list->children);
    }
    else if (auto * order_by_element = typeid_cast<ASTOrderByElement *>(ast.get()))
    {
        fuzzOrderByElement(order_by_element);
    }
    else if (auto * fn = typeid_cast<ASTFunction *>(ast.get()))
    {
        fuzzColumnLikeExpressionList(fn->arguments.get());
        fuzzColumnLikeExpressionList(fn->parameters.get());

        if (fn->is_window_function && fn->window_definition)
        {
            auto & def = fn->window_definition->as<ASTWindowDefinition &>();
            fuzzColumnLikeExpressionList(def.partition_by.get());
            fuzzOrderByList(def.order_by.get());
            fuzzWindowFrame(def);
        }

        fuzz(fn->children);
    }
    else if (auto * select = typeid_cast<ASTSelectQuery *>(ast.get()))
    {
        fuzzColumnLikeExpressionList(select->select().get());
        fuzzColumnLikeExpressionList(select->groupBy().get());
        fuzzOrderByList(select->orderBy().get());

        fuzz(select->children);
    }
    /*
     * The time to fuzz the settings has not yet come.
     * Apparently we don't have any infractructure to validate the values of
     * the settings, and the first query with max_block_size = -1 breaks
     * because of overflows here and there.
     *//*
     * else if (auto * set = typeid_cast<ASTSetQuery *>(ast.get()))
     * {
     *      for (auto & c : set->changes)
     *      {
     *          if (fuzz_rand() % 50 == 0)
     *          {
     *              c.value = fuzzField(c.value);
     *          }
     *      }
     * }
     */
    else if (auto * literal = typeid_cast<ASTLiteral *>(ast.get()))
    {
        // There is a caveat with fuzzing the children: many ASTs also keep the
        // links to particular children in own fields. This means that replacing
        // the child with another object might lead to error. Many of these fields
        // are ASTPtr -- this is redundant ownership, but hides the error if the
        // child field is replaced. Others can be ASTLiteral * or the like, which
        // leads to segfault if the pointed-to AST is replaced.
        // Replacing children is safe in case of ASTExpressionList. In a more
        // general case, we can change the value of ASTLiteral, which is what we
        // do here.
        if (fuzz_rand() % 11 == 0)
        {
            literal->value = fuzzField(literal->value);
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
void QueryFuzzer::collectFuzzInfoMain(ASTPtr ast)
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

void QueryFuzzer::addTableLike(ASTPtr ast)
{
    if (table_like_map.size() > 1000)
    {
        table_like_map.clear();
    }

    const auto name = ast->formatForErrorMessage();
    if (name.size() < 200)
    {
        table_like_map.insert({name, ast});
    }
}

void QueryFuzzer::addColumnLike(ASTPtr ast)
{
    if (column_like_map.size() > 1000)
    {
        column_like_map.clear();
    }

    const auto name = ast->formatForErrorMessage();
    if (name == "Null")
    {
        // The `Null` identifier from FORMAT Null clause. We don't quote it
        // properly when formatting the AST, and while the resulting query
        // technically works, it has non-standard case for Null (the standard
        // is NULL), so it breaks the query formatting idempotence check.
        // Just plug this particular case for now.
        return;
    }
    if (name.size() < 200)
    {
        column_like_map.insert({name, ast});
    }
}

void QueryFuzzer::collectFuzzInfoRecurse(ASTPtr ast)
{
    if (auto * impl = dynamic_cast<ASTWithAlias *>(ast.get()))
    {
        if (aliases_set.size() > 1000)
        {
            aliases_set.clear();
        }

        aliases_set.insert(impl->alias);
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
    current_ast_depth = 0;
    debug_visited_nodes.clear();
    debug_top_ast = &ast;

    collectFuzzInfoMain(ast);
    fuzz(ast);

    std::cout << std::endl;
    WriteBufferFromOStream ast_buf(std::cout, 4096);
    formatAST(*ast, ast_buf, false /*highlight*/);
    ast_buf.next();
    std::cout << std::endl << std::endl;
}

}
