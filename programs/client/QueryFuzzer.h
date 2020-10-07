#pragma once

#include <unordered_set>
#include <unordered_map>
#include <vector>

#include <Common/randomSeed.h>
#include <Common/Stopwatch.h>
#include <Core/Field.h>
#include <Parsers/IAST.h>

namespace DB
{

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

    // This is the only function you have to call -- it will modify the passed
    // ASTPtr to point to new AST with some random changes.
    void fuzzMain(ASTPtr & ast);

    // Variuos helper functions follow, normally you shouldn't have to call them.
    Field getRandomField(int type);
    Field fuzzField(Field field);
    ASTPtr getRandomColumnLike();
    void replaceWithColumnLike(ASTPtr & ast);
    void replaceWithTableLike(ASTPtr & ast);
    void fuzzColumnLikeExpressionList(ASTPtr ast);
    void fuzz(ASTs & asts);
    void fuzz(ASTPtr & ast);
    void collectFuzzInfoMain(const ASTPtr ast);
    void addTableLike(const ASTPtr ast);
    void addColumnLike(const ASTPtr ast);
    void collectFuzzInfoRecurse(const ASTPtr ast);
};

}
