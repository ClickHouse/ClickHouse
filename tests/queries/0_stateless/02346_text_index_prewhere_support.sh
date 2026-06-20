#!/usr/bin/env bash
# Tags: no-parallel-replicas

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

MY_CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT --enable_analyzer 1 --use_skip_indexes_on_data_read 1"

$MY_CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS tab;

    CREATE TABLE tab
    (
        id Int64,
        a Int32,
        text1 String,
        text2 String,
        INDEX inv_idx1 text1 TYPE text(tokenizer = 'splitByNonAlpha'),
        INDEX inv_idx2 text2 TYPE text(tokenizer = 'splitByNonAlpha')
    )
    ENGINE = MergeTree
    ORDER BY id
    SETTINGS index_granularity = 8;

    INSERT INTO tab VALUES
    (1, 10, 'clickhouse rocks', 'fastio engine'),
    (2, 20, 'column store database', 'merge tree storage'),
    (3, 30, 'search query analytics', 'distributed fast query'),
    (4, 40, 'column store index', 'index optimize engine'),
    (5, 50, 'click clicker clicking', 'indexer indexing'),
    (6, 60, 'hello world example', 'no relation here'),
    (7, 70, 'abc', 'xyz'),
    (8, 80, '', ''),
    (9, 90, 'id123 token456', 'v100 gpu t4'),
    (10, 100, 'click-house,fast/io;merge.tree', 'index+optimize-query'),
    (11, 110, 'clickhouse clickhouse clickhouse', 'fastio fastio'),
    (12, 120, 'none here', 'login logout user'),
    (13, 130, 'user login event', 'user logout event'),
    (14, 140, 'analytics engine for column store text search', 'distributed warehouse lake'),
    (15, 150, 'debug mode enabled', 'internal test only'),
    (16, 160, 'column', 'store'),
    (17, 170, 'column', 'tree'),
    (18, 180, 'column store tree merge index', 'optimize index merge'),
    (19, 190, '搜索 clickhouse 数据库', '合并 tree 存储'),
    (20, 200, 'a b c clickhouse d e f analytics g h lake warehouse', 'info warn error active online prod'),
    (21, 1, 'token1 token2', 'token3'),
    (22, 2, 'token2 token3', 'token1 token4'),
    (23, 3, 'merge tree', 'merge'),
    (24, 4, 'merge', 'tree'),
    (25, 5, 'fast query engine', 'clickhouse analytics'),
    (26, 6, 'hot warm cold', 'dry wet'),
    (27, 7, 'mysql postgres oracle', 'ck ch clickhouse'),
    (28, 8, 'aa bb cc', 'dd ee ff'),
    (29, 9, 'user active online', 'offline inactive'),
    (30, 10, 'error warn info', 'fatal panic'),
    (31, 11, 'quick brown fox', 'jumped over lazy dog'),
    (32, 12, 'login login login', 'logout'),
    (33, 13, 'index index index', 'optimize optimize'),
    (34, 14, 'analytics clickhouse index', 'index merge tree'),
    (35, 15, 'engine storage table', 'column store db'),
    (36, 16, 'event timeline', 'span trace'),
    (37, 17, 'json parser', 'sql processor'),
    (38, 18, '你好 世界 clickhouse', '文本 处理'),
    (39, 19, 'lake house delta iceberg', 'warehouse table'),
    (40, 20, 'fast search engine', 'query optimizer'),
    (41, 21, 'token a1 a2', 'b1 b2'),
    (42, 22, 'token x y z', 'x1 y1 z1'),
    (43, 23, 'clickhouse integration test', 'ci cd pipeline'),
    (44, 24, 'benchmark suite', 'performance test'),
    (45, 25, 'very long sentence with many many tokens included for stress testing the tokenizer behavior', 'another long long sentence'),
    (46, 26, 'key1 key2 key3', 'key4 key5 key6'),
    (47, 27, 'metrics logs traces', 'prod staging dev'),
    (48, 28, 'aaa bbb ccc ddd', 'eee fff ggg hhh'),
    (49, 29, 'login user password', 'token secret'),
    (50, 30, 'error failed crashed', 'retry recover continue'),
    (51, 31, 'olap database query', 'oltp transaction'),
    (52, 32, 'columnar format optimized', 'shuffle map reduce'),
    (53, 33, 'semantic vector embedding', 'machine learning'),
    (54, 34, 'gpu cuda tensor', 'cpu thread'),
    (55, 35, 'merge join hash', 'distributed execution'),
    (56, 36, 'tree structure', 'balanced tree'),
    (57, 37, 'multi token test', 'single token'),
    (58, 38, 'foo bar baz', 'qux quux'),
    (59, 39, 'apple banana cherry', 'pear peach plum'),
    (60, 40, 'cat dog mouse', 'lion tiger panda'),
    (61, 41, 'network tcp udp', 'http https'),
    (62, 42, 'router switch firewall', 'gateway nat'),
    (63, 43, 'offset limit sample', 'order group'),
    (64, 44, 'table column row', 'disk memory cache'),
    (65, 45, 'log info debug error', 'trace span'),
    (66, 46, 'session user token', 'role permission'),
    (67, 47, 'schema table database', 'view materialized'),
    (68, 48, 'optimize finalize mutate', 'insert delete update'),
    (69, 49, 'big small tiny huge', 'micro nano pico'),
    (70, 50, 'fast slow medium', 'low high'),
    (71, 51, '山 河 大 地', '春 夏 秋 冬'),
    (72, 52, '搜索 引擎 测试', '算法 优化'),
    (73, 53, '分布式 系统 架构', '高 可用'),
    (74, 54, 'text search engine', 'query expansion'),
    (75, 55, 'token splitting test', 'punctuation,should;split'),
    (76, 56, 'has all tokens test', 'has any tokens test'),
    (77, 57, 'id id id id', 'value value value'),
    (78, 58, 'abc123 xyz456', 'aaa111 bbb222'),
    (79, 59, 'merge tree parts marks', 'granules rows'),
    (80, 60, 'skip index test', 'prewhere optimization'),
    (81, 61, 'vector search embedding', 'semantic query'),
    (82, 62, 'index skip logic', 'index read chain'),
    (83, 63, 'projection text index', 'inverted index'),
    (84, 64, 'token boundaries edge', 'case sensitivity test'),
    (85, 65, 'single token match', 'multiple token match'),
    (86, 66, 'query planner', 'expression analyzer'),
    (87, 67, 'cache miss hit', 'cache eviction'),
    (88, 68, 'io scheduler test', 'latency throughput'),
    (89, 69, 'memory usage tracking', 'oom killer test'),
    (90, 70, 'cpu profile flamegraph', 'perf event'),
    (91, 71, 'hello clickhouse', 'world fastio'),
    (92, 72, 'json document', 'xml yaml'),
    (93, 73, 'tree map list set', 'hash map vector'),
    (94, 74, 'vector db', 'embedding index'),
    (95, 75, 'has token', 'no token'),
    (96, 76, 'aaa aaa bbb', 'ccc ddd'),
    (97, 77, 'mixed english 中文 测试', '多 语言 text'),
    (98, 78, 'uu vv ww', 'xx yy zz'),
    (99, 79, 'login failed', 'retry ok'),
    (100, 80, 'merge tree reader', 'index reader'),
    (101, 81, 'optimizer rule test', 'logical rules'),
    (102, 82, 'fast merge pass', 'read rows adjust'),
    (103, 83, 'vectorized execution', 'columnar iteration'),
    (104, 84, 'parallel processing', 'thread pool'),
    (105, 85, 'table engine memory', 'table engine log'),
    (106, 86, 'zookeeper keeper', 'raft consensus'),
    (107, 87, 'tokenizer abc-def', 'ghi_jkl'),
    (108, 88, 'stress long long long input', 'another long long'),
    (109, 89, 'real world logs', 'debug info warn'),
    (110, 90, 'text indexing benchmark', 'performance stats'),
    (111, 91, 'semantic search', 'embedding vector'),
    (112, 92, 'clickhouse fast', 'cloud platform'),
    (113, 93, 'vector similarity', 'cosine distance'),
    (114, 94, 'event logs traces', 'profiling spans'),
    (115, 95, 'runtime error crash', 'fault safe'),
    (116, 96, 'data lake warehouse', 'delta iceberg'),
    (117, 97, 'token-a token=b token:c', 'x-y,z;w'),
    (118, 98, 'user permission role', 'policy rules'),
    (119, 99, 'cache line alignment', 'cpu cache test'),
    (120, 100, 'search index engine', 'query processing'),
    (121, 101, 'hash join build probe', 'shuffle exchange'),
    (122, 102, 'token and text index', 'prewhere filter'),
    (123, 103, 'query rewrite test', 'optimizer hints'),
    (124, 104, 'nested tokens test', 'deep token tree'),
    (125, 105, 'multi word match', 'single word'),
    (126, 106, 'abc def ghi', 'jkl mno pqr'),
    (127, 107, 'xyz uvw rst', 'aaa bbb ccc'),
    (128, 108, 'final test row', 'completed yes');
"

# Tests that text indexes can be used in PREWHERE clause.
function run()
{
    query="$1"
    echo "$query"
    $MY_CLICKHOUSE_CLIENT --use_skip_indexes 0 --query "$query"
    $MY_CLICKHOUSE_CLIENT --use_skip_indexes 1 --query "$query"

    $MY_CLICKHOUSE_CLIENT --query "
        SELECT trim(explain) FROM
        (
            EXPLAIN actions = 1, indexes = 1 $query SETTINGS use_skip_indexes_on_data_read = 1
        )
        WHERE explain ILIKE '%filter column%' OR explain ILIKE '%name: inv_idx%'
    "
}

# Basic: single text-index function on 'text1'
run "SELECT count() FROM tab PREWHERE hasToken(text1, 'clickhouse')"

# Basic: hasAnyTokens on 'text1'
run "SELECT count() FROM tab PREWHERE hasAnyTokens(text1, ['clickhouse', 'database'])"

# Basic: hasAllTokens on 'text1'
run "SELECT count() FROM tab PREWHERE hasAllTokens(text1, ['column', 'store'])"

# Basic: text-index functions on 'text2'
run "SELECT count() FROM tab PREWHERE hasToken(text2, 'fastio')"

run "SELECT count() FROM tab PREWHERE hasAnyTokens(text2, ['search', 'index'])"

run "SELECT count() FROM tab PREWHERE hasAllTokens(text2, ['merge', 'tree'])"

# Text condition + numeric condition (AND)
run "SELECT count() FROM tab PREWHERE hasToken(text1, 'clickhouse') AND a > 0"

run "SELECT count() FROM tab PREWHERE hasAnyTokens(text1, ['clickhouse', 'database']) AND id BETWEEN 10 AND 100"

run "SELECT count() FROM tab PREWHERE hasAllTokens(text1, ['column', 'store']) AND (a % 2) = 0"

# Text condition + numeric condition with OR and parentheses
run "SELECT count() FROM tab PREWHERE hasToken(text1, 'analytics') OR a < 0"

run "SELECT count() FROM tab PREWHERE (hasAnyTokens(text1, ['log', 'event']) OR hasToken(text2, 'error')) AND id > 0"

run "SELECT count() FROM tab PREWHERE (a > 100 AND hasToken(text1, 'hot')) OR (a <= 100 AND hasAnyTokens(text2, ['cold', 'warm']))"

# Multiple text functions on the same column
run "SELECT count() FROM tab PREWHERE hasToken(text1, 'clickhouse') AND hasAnyTokens(text1, ['clickhouse', 'database'])"

run "SELECT count() FROM tab PREWHERE hasAnyTokens(text1, ['clickhouse', 'database']) AND NOT hasToken(text1, 'mysql')"

run "SELECT count() FROM tab PREWHERE hasAllTokens(text1, ['column', 'store']) OR hasAnyTokens(text1, ['olap', 'analytics'])"

# Cross-column combinations (text1 + text2)
run "SELECT count() FROM tab PREWHERE hasToken(text1, 'clickhouse') AND hasToken(text2, 'fastio')"

run "SELECT count() FROM tab PREWHERE hasAnyTokens(text1, ['search', 'query']) OR hasAllTokens(text2, ['index', 'optimize'])"

run "SELECT count() FROM tab PREWHERE (hasToken(text1, 'user') AND hasAnyTokens(text2, ['login', 'logout'])) OR (a >= 0 AND a <= 10)"

# Combinations using NOT
run "SELECT count() FROM tab PREWHERE NOT hasToken(text1, 'debug') AND hasAnyTokens(text2, ['info', 'warn', 'error'])"

run "SELECT count() FROM tab PREWHERE NOT hasAllTokens(text1, ['internal', 'test']) AND a > 0 AND hasToken(text2, 'prod')"

run "SELECT count() FROM tab PREWHERE NOT (hasAnyTokens(text1, ['drop', 'truncate']) OR hasToken(text2, 'danger')) AND id IN (1, 2, 3, 4)"

# Text functions mixed into complex expressions
run "SELECT count() FROM tab PREWHERE (a > 10 AND hasToken(text1, 'clickhouse')) OR (a <= 10 AND hasAnyTokens(text2, ['mysql', 'postgres']))"

run "SELECT count() FROM tab PREWHERE (hasAllTokens(text1, ['column', 'store']) AND a BETWEEN 1 AND 100) OR (hasAnyTokens(text2, ['lake', 'warehouse']) AND id > 1000)"

run "SELECT count() FROM tab PREWHERE (NOT hasToken(text1, 'archived') AND a > 0) OR (hasAnyTokens(text2, ['active', 'online']) AND id % 5 = 0)"

#  Text index filter in PREWHERE, numeric filter in WHERE
run "SELECT count() FROM tab PREWHERE hasToken(text1, 'clickhouse') WHERE a > 0"

# PREWHERE uses hasAllTokens, WHERE excludes some special cases
run "SELECT count() FROM tab PREWHERE hasAllTokens(text1, ['column', 'store']) WHERE a NOT IN (150, 160) AND id != 15"

# Combinations with PREWHERE and WHERE
run "SELECT count() FROM tab PREWHERE hasToken(text1, 'clickhouse') WHERE hasToken(text2, 'fastio')"

run "SELECT count() FROM tab PREWHERE hasAllTokens(text1, ['column', 'store']) WHERE hasAnyTokens(text1, ['olap', 'analytics'])"
