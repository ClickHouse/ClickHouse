#pragma once

#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <DataTypes/IDataType.h>

#include <pcg-random/pcg_random.hpp>

#include <Core/Field.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/NullsAction.h>
#include <Common/SettingsChanges.h>
#include <Common/randomSeed.h>


namespace DB
{

class ASTExpressionList;
class ASTOrderByElement;
class ASTCreateQuery;
class ASTInsertQuery;
class ASTColumnDeclaration;
class ASTDropQuery;
class ASTSetQuery;
struct ASTTableExpression;
struct ASTWindowDefinition;

/*
 * This is an AST-based query fuzzer that makes random modifications to query
 * AST, changing numbers, list of columns, functions, etc. It remembers part of
 * queries it fuzzed previously, and can substitute these parts to new fuzzed
 * queries, so you want to feed it a lot of queries to get some interesting mix
 * of them. Normally we feed SQL regression tests to it.
 */
class QueryFuzzer
{
public:
    explicit QueryFuzzer(pcg64 fuzz_rand_ = randomSeed(), std::ostream * out_stream_ = nullptr, std::ostream * debug_stream_ = nullptr)
        : seed(0)
        , fuzz_rand(fuzz_rand_)
        , out_stream(out_stream_)
        , debug_stream(debug_stream_)
    {
    }

    // This is the only function you have to call -- it will modify the passed
    // ASTPtr to point to new AST with some random changes.
    void fuzzMain(ASTPtr & ast);

    ASTs getInsertQueriesForFuzzedTables(const String & full_query);
    ASTs getOptimizeQueriesForFuzzedTables(const String & full_query);
    ASTs getDropQueriesForFuzzedTables(const ASTDropQuery & drop_query);
    void notifyQueryFailed(ASTPtr ast);

    static bool isSuitableForFuzzing(const ASTCreateQuery & create);

    UInt64 getSeed() const { return seed; }

    void setSeed(const UInt64 new_seed)
    {
        seed = new_seed;
        fuzz_rand = pcg64(seed);
    }

    /// When measuring performance, sometimes change settings
    void getRandomSettings(SettingsChanges & settings_changes);

private:
    UInt64 seed;
    pcg64 fuzz_rand;

    std::ostream * out_stream = nullptr;
    std::ostream * debug_stream = nullptr;

    // We add elements to expression lists with fixed probability. Some elements
    // are so large, that the expected number of elements we add to them is
    // one or higher, hence this process might never finish. Put some limit on the
    // total depth of AST to prevent this.
    // This field is reset for each fuzzMain() call.
    size_t current_ast_depth = 0;

    // Used to track added tables in join clauses
    uint32_t alias_counter = 0;

    // These arrays hold parts of queries that we can substitute into the query
    // we are currently fuzzing. We add some part from each new query we are asked
    // to fuzz, and keep this state between queries, so the fuzzing output becomes
    // more interesting over time, as the queries mix.
    // The hash tables are used for collection, and the vectors are used for random access.
    std::unordered_map<std::string, ASTPtr> column_like_map;
    std::vector<std::pair<std::string, ASTPtr>> column_like, colids;

    std::unordered_map<std::string, ASTPtr> table_like_map;
    std::vector<std::pair<std::string, ASTPtr>> table_like;

    // Some debug fields for detecting problematic ASTs with loops.
    // These are reset for each fuzzMain call.
    std::unordered_set<const IAST *> debug_visited_nodes;
    ASTPtr * debug_top_ast = nullptr;

    std::unordered_map<std::string, std::unordered_set<std::string>> original_table_name_to_fuzzed;
    std::unordered_map<std::string, size_t> index_of_fuzzed_table;
    std::set<IAST::Hash> created_tables_hashes;

    // Various helper functions follow, normally you shouldn't have to call them.
    Field getRandomField(int type);
    Field fuzzField(Field field);
    ASTPtr getRandomColumnLike();
    ASTPtr getRandomExpressionList(size_t nproj);
    DataTypePtr fuzzDataType(DataTypePtr type);
    DataTypePtr getRandomType();
    void fuzzJoinType(ASTTableJoin * table_join);
    void fuzzOrderByElement(ASTOrderByElement * elem);
    void fuzzOrderByList(IAST * ast, size_t nproj);
    void fuzzColumnLikeExpressionList(IAST * ast);
    void fuzzNullsAction(NullsAction & action);
    void fuzzWindowFrame(ASTWindowDefinition & def);
    void fuzzCreateQuery(ASTCreateQuery & create);
    void fuzzExplainQuery(ASTExplainQuery & explain);
    ASTExplainQuery::ExplainKind fuzzExplainKind(ASTExplainQuery::ExplainKind kind = ASTExplainQuery::ExplainKind::QueryPipeline);
    void fuzzExplainSettings(ASTSetQuery & settings_ast, ASTExplainQuery::ExplainKind kind);
    void fuzzColumnDeclaration(ASTColumnDeclaration & column);
    void fuzzTableName(ASTTableExpression & table);
    ASTPtr fuzzLiteralUnderExpressionList(ASTPtr child);
    ASTPtr reverseLiteralFuzzing(ASTPtr child);
    void fuzzExpressionList(ASTExpressionList & expr_list);
    ASTPtr tryNegateNextPredicate(const ASTPtr & pred, int prob);
    ASTPtr setIdentifierAliasOrNot(ASTPtr & exp);
    ASTPtr addJoinClause();
    ASTPtr addArrayJoinClause();
    ASTPtr generatePredicate();
    void addOrReplacePredicate(ASTSelectQuery * sel, ASTSelectQuery::Expression expr);
    void fuzz(ASTs & asts);
    void fuzz(ASTPtr & ast);
    void collectFuzzInfoMain(ASTPtr ast);
    void addTableLike(ASTPtr ast);
    void addColumnLike(ASTPtr ast);
    void collectFuzzInfoRecurse(ASTPtr ast);

    void extractPredicates(const ASTPtr & node, ASTs & predicates, const std::string & op, int negProb);
    ASTPtr permutePredicateClause(const ASTPtr & predicate, int negProb);

    template <typename T>
    const T & pickRandomlyFromVector(pcg64 & rand, const std::vector<T> & vals)
    {
        std::uniform_int_distribution<size_t> d{0, vals.size() - 1};
        return vals[d(rand)];
    }

    template <typename T>
    const T & pickRandomlyFromSet(pcg64 & rand, const std::unordered_set<T> & vals)
    {
        std::uniform_int_distribution<size_t> d{0, vals.size() - 1};
        auto it = vals.begin();
        std::advance(it, d(rand));
        return *it;
    }

    template <typename K, typename V>
    const K & pickKeyRandomlyFromMap(pcg64 & rand, const std::unordered_map<K, V> & vals)
    {
        std::uniform_int_distribution<size_t> d{0, vals.size() - 1};
        auto it = vals.begin();
        std::advance(it, d(rand));
        return it->first;
    }
};

}
