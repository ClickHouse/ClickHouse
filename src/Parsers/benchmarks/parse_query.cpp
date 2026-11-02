/// Parse-only benchmark of `ParserQuery` on statements that parse and on statements that fail.
///
/// `ParseFailing` parses every prefix of each statement that ends at a space, so most of its parses
/// fail somewhere inside a statement parser: that is where the failure exits are, which is what to
/// look at when they change. `ParseValid` parses the statements themselves.

#include <Core/Defines.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>

#include <benchmark/benchmark.h>

#include <string>
#include <vector>

using namespace DB;

namespace
{

/// Long statements are split into adjacent literals.
/// NOLINTBEGIN(bugprone-suspicious-missing-comma)
const std::vector<std::string> statements = {
    "SELECT a, b + 1 AS c, count() FROM db.t AS x LEFT JOIN u USING (a) PREWHERE b > 0 WHERE c IN (SELECT c FROM v) "
    "GROUP BY a, c WITH TOTALS HAVING count() > 1 ORDER BY a DESC NULLS LAST, c LIMIT 10 OFFSET 5 SETTINGS max_threads = 1",
    "WITH s AS (SELECT number AS n FROM numbers(10)) SELECT n, sum(n) OVER (PARTITION BY n % 2 ORDER BY n) FROM s "
    "WHERE n BETWEEN 1 AND 8 QUALIFY n > 2 LIMIT 1 BY n",
    "SELECT * FROM (SELECT a FROM t UNION ALL SELECT b FROM u) ARRAY JOIN arr AS e WHERE e LIKE 'x%' FORMAT JSONEachRow",
    "INSERT INTO db.t (a, b, c) SETTINGS async_insert = 1 SELECT a, b, c FROM src WHERE a > 0",
    "INSERT INTO FUNCTION file('out.csv', 'CSV') SELECT number FROM numbers(10)",
    "CREATE TABLE IF NOT EXISTS db.t ON CLUSTER c (a UInt64, b String DEFAULT 'x' CODEC(ZSTD(1)), c Nullable(Float64), "
    "INDEX i b TYPE bloom_filter GRANULARITY 4, PROJECTION p (SELECT a, count() GROUP BY a)) "
    "ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/t', '{replica}') PARTITION BY toYYYYMM(now()) "
    "ORDER BY (a, b) SAMPLE BY a TTL now() + INTERVAL 1 DAY SETTINGS index_granularity = 8192 COMMENT 'table'",
    "CREATE MATERIALIZED VIEW db.mv REFRESH EVERY 1 HOUR OFFSET 5 MINUTE TO db.dst (a UInt64, s UInt64) "
    "AS SELECT a, sum(b) AS s FROM db.src GROUP BY a",
    "CREATE VIEW v (a, b) DEFINER = u SQL SECURITY DEFINER AS SELECT a, b FROM t WHERE a > 1",
    "CREATE DICTIONARY db.d (k UInt64, v String DEFAULT '') PRIMARY KEY k "
    "SOURCE(CLICKHOUSE(TABLE 'src' DB 'db')) LAYOUT(HASHED()) LIFETIME(MIN 0 MAX 300) COMMENT 'dict'",
    "ALTER TABLE db.t ON CLUSTER c ADD COLUMN d UInt8 DEFAULT 0 AFTER c, MODIFY COLUMN b LowCardinality(String), "
    "DROP INDEX i, MODIFY TTL now() + INTERVAL 2 DAY, UPDATE a = a + 1 WHERE b = 'x'",
};
/// NOLINTEND(bugprone-suspicious-missing-comma)

std::vector<std::string> prefixes()
{
    std::vector<std::string> result;
    for (const auto & statement : statements)
        for (size_t i = 0; i < statement.size(); ++i)
            if (statement[i] == ' ')
                result.push_back(statement.substr(0, i));
    return result;
}

size_t parseAll(const std::vector<std::string> & queries)
{
    size_t parsed = 0;
    std::string error;
    for (const auto & query : queries)
    {
        const char * begin = query.data();
        const char * end = begin + query.size();
        ParserQuery parser(end);
        ASTPtr ast = tryParseQuery(
            parser, begin, end, error, /*hilite=*/ false, "", /*allow_multi_statements=*/ false, DBMS_DEFAULT_MAX_QUERY_SIZE,
            DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS, /*skip_insignificant=*/ true);
        parsed += ast != nullptr;
    }
    return parsed;
}

void run(benchmark::State & state, const std::vector<std::string> & queries)
{
    for (auto _ [[maybe_unused]] : state)
        benchmark::DoNotOptimize(parseAll(queries));
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * queries.size()));
}

}

static void ParseValid(benchmark::State & state)
{
    run(state, statements);
}

static void ParseFailing(benchmark::State & state)
{
    static const auto queries = prefixes();
    run(state, queries);
}

BENCHMARK(ParseValid);
BENCHMARK(ParseFailing);
