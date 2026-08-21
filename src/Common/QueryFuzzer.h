#pragma once

#include <DataTypes/IDataType.h>

#include <pcg-random/pcg_random.hpp>

#include <Core/Field.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/IASTHash.h>
#include <Parsers/NullsAction.h>
#include <Parsers/ParserInsertQuery.h>
#include <Parsers/parseQuery.h>
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

    ASTs getDropQueriesForFuzzedTables(const ASTDropQuery & drop_query);
    void notifyQueryFailed(ASTPtr ast);

    static bool isSuitableForFuzzing(const ASTCreateQuery & create);

    UInt64 getSeed() const { return seed; }

    /// Returns the total number of accumulated AST fragments (column-like + table-like).
    size_t getAccumulatedStateSize() const { return column_like.size() + table_like.size(); }

    void setSeed(const UInt64 new_seed)
    {
        seed = new_seed;
        fuzz_rand = pcg64(seed);
    }

    /// When measuring performance, sometimes change settings
    void getRandomSettings(SettingsChanges & settings_changes);

private:
    template <typename Parser>
    ASTPtr tryParseQueryForFuzzedTables(const std::string_view & full_query)
    {
        String message;
        const char * pos = full_query.data();
        const char * end = full_query.data() + full_query.size();

        if constexpr (std::is_same_v<Parser, ParserInsertQuery>)
        {
            Parser p(end, false);
            return tryParseQuery(
                p,
                pos,
                end,
                message,
                false,
                "",
                false,
                DBMS_DEFAULT_MAX_QUERY_SIZE,
                DBMS_DEFAULT_MAX_PARSER_DEPTH,
                DBMS_DEFAULT_MAX_PARSER_BACKTRACKS,
                true);
        }
        else
        {
            Parser p;
            return tryParseQuery(
                p,
                pos,
                end,
                message,
                false,
                "",
                false,
                DBMS_DEFAULT_MAX_QUERY_SIZE,
                DBMS_DEFAULT_MAX_PARSER_DEPTH,
                DBMS_DEFAULT_MAX_PARSER_BACKTRACKS,
                true);
        }
    }

public:
    template <typename ParsedAST, typename Parser>
    ASTs getQueriesForFuzzedTables(const std::string_view & full_query)
    {
        auto parsed_query = tryParseQueryForFuzzedTables<Parser>(full_query);
        if (!parsed_query)
            return {};

        const auto & query_ast = *parsed_query->template as<ParsedAST>();
        if (!query_ast.table)
            return {};

        auto table_name = query_ast.getTable();
        auto it = original_table_name_to_fuzzed.find(table_name);
        if (it == original_table_name_to_fuzzed.end())
            return {};

        ASTs queries;
        for (const auto & fuzzed_name : it->second)
        {
            /// Parse query from scratch for each table instead of clone,
            /// to store proper pointers to inlined data,
            /// which are not copied during clone.
            auto & query = queries.emplace_back(tryParseQueryForFuzzedTables<Parser>(full_query));
            query->template as<ParsedAST>()->setTable(fuzzed_name);
        }

        return queries;
    }

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

    // Similar to current_ast_depth, this is a limit on some measure of query size or number of
    // steps we take. Without it, with small probability, query size may explode even when depth is
    // limited. In particular, array lengths and depths in fuzzField() were seen to do that:
    // https://github.com/ClickHouse/ClickHouse/issues/77408
    //
    // I don't fully understand how this happens, but my impression is that recursive random
    // generators like this just generally tend to produce size distribution with heavy tail.
    // Maybe if the query size/depth/some-other-property reaches some critical mass, each recursive
    // call on average causes more than one additional recursive call (e.g. by copying a huge subtree
    // with some small-but-not-tiny probability), so the expected number of calls becomes infinite.
    // Despite infinite expected tree size, the p99 size may still be moderate
    // (see e.g. "St. Petersburg lottery"), so the failures can be rare in practice.
    //
    // (What does "infinite expected value" mean in practice? Suppose you keep generating more and
    //  more values and averaging them. If the expected value is finite, the average will be
    //  converging to it. If the expected value is infinite, the average will keep growing without
    //  bound.)
    size_t iteration_count = 0;
    static constexpr size_t iteration_limit = 500000;

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
    std::set<IASTHash> created_tables_hashes;

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
    NullsAction fuzzNullsAction(NullsAction action);
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
    void checkIterationLimit();

    void extractPredicates(const ASTPtr & node, ASTs & predicates, const std::string & op, int negProb);
    ASTPtr permutePredicateClause(const ASTPtr & predicate, int negProb);

    template <typename Container>
    const auto & pickRandomly(pcg64 & rand, const Container & container)
    {
        std::uniform_int_distribution<size_t> d{0, container.size() - 1};
        auto it = container.begin();
        std::advance(it, d(rand));

        if constexpr (requires { it->first; })
            return it->first;
        else
            return *it;
    }
};

}
