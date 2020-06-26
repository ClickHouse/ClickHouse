#pragma once

#include <unordered_set>
#include <unordered_map>
#include <vector>

#include <Common/Stopwatch.h>
#include <Core/Field.h>
#include <Parsers/IAST.h>

namespace DB
{

struct QueryFuzzer
{
    //pcg64 fuzz_rand{static_cast<UInt64>(rand())};
    pcg64 fuzz_rand{clock_gettime_ns()};

    // Collection of asts with alias.
    /*
    std::unordered_map<std::string, ASTPtr> with_alias_map;
    std::vector<ASTPtr> with_alias;
    */

    std::unordered_set<std::string> aliases_set;
    std::vector<std::string> aliases;

    std::unordered_map<std::string, const ASTPtr> column_like_map;
    std::vector<const ASTPtr> column_like;

    std::unordered_map<std::string, const ASTPtr> table_like_map;
    std::vector<const ASTPtr> table_like;

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
    void fuzzMain(ASTPtr & ast);
};

}
