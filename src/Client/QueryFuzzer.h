#pragma once

#include <unordered_set>
#include <unordered_map>
#include <vector>

#include <pcg-random/pcg_random.hpp>

#include <Common/randomSeed.h>
#include <Core/Field.h>
#include <Parsers/IAST.h>


namespace DB
{

class ASTExpressionList;
class ASTOrderByElement;
struct ASTWindowDefinition;

/*
 * This is an AST-based query fuzzer that makes random modifications to query
 * AST, changing numbers, list of columns, functions, etc. It remembers part of
 * queries it fuzzed previously, and can substitute these parts to new fuzzed
 * queries, so you want to feed it a lot of queries to get some interesting mix
 * of them. Normally we feed SQL regression tests to it.
 */
struct QueryFuzzer
{
    pcg64 fuzz_rand{randomSeed()};

    // We add elements to expression lists with fixed probability. Some elements
    // are so large, that the expected number of elements we add to them is
    // one or higher, hence this process might never finish. Put some limit on the
    // total depth of AST to prevent this.
    // This field is reset for each fuzzMain() call.
    size_t current_ast_depth = 0;

    // These arrays hold parts of queries that we can substitute into the query
    // we are currently fuzzing. We add some part from each new query we are asked
    // to fuzz, and keep this state between queries, so the fuzzing output becomes
    // more interesting over time, as the queries mix.
    std::unordered_set<std::string> aliases_set;
    std::vector<std::string> aliases;

    std::unordered_map<std::string, ASTPtr> column_like_map;
    std::vector<ASTPtr> column_like;

    std::unordered_map<std::string, ASTPtr> table_like_map;
    std::vector<ASTPtr> table_like;

    // Some debug fields for detecting problematic ASTs with loops.
    // These are reset for each fuzzMain call.
    std::unordered_set<const IAST *> debug_visited_nodes;
    ASTPtr * debug_top_ast = nullptr;


    // This is the only function you have to call -- it will modify the passed
    // ASTPtr to point to new AST with some random changes.
    void fuzzMain(ASTPtr & ast);

    // Various helper functions follow, normally you shouldn't have to call them.
    Field getRandomField(int type);
    Field fuzzField(Field field);
    ASTPtr getRandomColumnLike();
    void replaceWithColumnLike(ASTPtr & ast);
    void replaceWithTableLike(ASTPtr & ast);
    void fuzzOrderByElement(ASTOrderByElement * elem);
    void fuzzOrderByList(IAST * ast);
    void fuzzColumnLikeExpressionList(IAST * ast);
    void fuzzWindowFrame(ASTWindowDefinition & def);
    void fuzz(ASTs & asts);
    void fuzz(ASTPtr & ast);
    void collectFuzzInfoMain(ASTPtr ast);
    void addTableLike(ASTPtr ast);
    void addColumnLike(ASTPtr ast);
    void collectFuzzInfoRecurse(ASTPtr ast);
};

}
