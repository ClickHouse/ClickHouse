#include <Parsers/SPARQL/SparqlQueryParser.h>
#include <Parsers/SPARQL/SparqlToSqlTranslator.h>

#include <benchmark/benchmark.h>

using namespace DB::SPARQL;

static void BM_ParseBasicSelect(benchmark::State & state)
{
    const std::string sparql = "SELECT ?name WHERE { ?p <rdf:type> <:Person> . ?p <:name> ?name }";
    for (auto _ : state)
    {
        SparqlQueryParser parser;
        auto ast = parser.parse(sparql);
        benchmark::DoNotOptimize(ast);
    }
}
BENCHMARK(BM_ParseBasicSelect);

static void BM_TranslateBasicSelect(benchmark::State & state)
{
    const std::string sparql = "SELECT ?name WHERE { ?p <rdf:type> <:Person> . ?p <:name> ?name }";
    for (auto _ : state)
    {
        SparqlQueryParser parser;
        auto ast = parser.parse(sparql);
        SparqlToSqlTranslator translator;
        auto sql = translator.translate(*ast);
        benchmark::DoNotOptimize(sql);
    }
}
BENCHMARK(BM_TranslateBasicSelect);

static void BM_TranslateOptional(benchmark::State & state)
{
    const std::string sparql =
        "SELECT ?name ?email WHERE { "
        "?p <rdf:type> <:Person> . ?p <:name> ?name . "
        "OPTIONAL { ?p <:email> ?email } }";
    for (auto _ : state)
    {
        SparqlQueryParser parser;
        auto ast = parser.parse(sparql);
        SparqlToSqlTranslator translator;
        auto sql = translator.translate(*ast);
        benchmark::DoNotOptimize(sql);
    }
}
BENCHMARK(BM_TranslateOptional);

static void BM_TranslateUnion(benchmark::State & state)
{
    const std::string sparql =
        "SELECT ?contact WHERE { "
        "{ ?p <:email> ?contact } UNION { ?p <:phone> ?contact } }";
    for (auto _ : state)
    {
        SparqlQueryParser parser;
        auto ast = parser.parse(sparql);
        SparqlToSqlTranslator translator;
        auto sql = translator.translate(*ast);
        benchmark::DoNotOptimize(sql);
    }
}
BENCHMARK(BM_TranslateUnion);

static void BM_TranslateFilter(benchmark::State & state)
{
    const std::string sparql =
        "SELECT ?name WHERE { "
        "?p <rdf:type> <:Person> . ?p <:name> ?name . ?p <:age> ?age . "
        "FILTER (?age > 25) }";
    for (auto _ : state)
    {
        SparqlQueryParser parser;
        auto ast = parser.parse(sparql);
        SparqlToSqlTranslator translator;
        auto sql = translator.translate(*ast);
        benchmark::DoNotOptimize(sql);
    }
}
BENCHMARK(BM_TranslateFilter);

static void BM_TranslateThreeWayJoin(benchmark::State & state)
{
    const std::string sparql =
        "SELECT ?name ?age WHERE { "
        "?p <rdf:type> <:Person> . ?p <:name> ?name . ?p <:age> ?age }";
    for (auto _ : state)
    {
        SparqlQueryParser parser;
        auto ast = parser.parse(sparql);
        SparqlToSqlTranslator translator;
        auto sql = translator.translate(*ast);
        benchmark::DoNotOptimize(sql);
    }
}
BENCHMARK(BM_TranslateThreeWayJoin);

static void BM_TranslateComplexFilter(benchmark::State & state)
{
    const std::string sparql =
        "SELECT ?name ?age WHERE { "
        "?p <:name> ?name . ?p <:age> ?age . "
        "FILTER (?age > 20 && ?age < 30) }";
    for (auto _ : state)
    {
        SparqlQueryParser parser;
        auto ast = parser.parse(sparql);
        SparqlToSqlTranslator translator;
        auto sql = translator.translate(*ast);
        benchmark::DoNotOptimize(sql);
    }
}
BENCHMARK(BM_TranslateComplexFilter);

static void BM_TranslateMultipleOptionals(benchmark::State & state)
{
    const std::string sparql =
        "SELECT ?name ?email ?phone WHERE { "
        "?p <rdf:type> <:Person> . ?p <:name> ?name . "
        "OPTIONAL { ?p <:email> ?email } . "
        "OPTIONAL { ?p <:phone> ?phone } }";
    for (auto _ : state)
    {
        SparqlQueryParser parser;
        auto ast = parser.parse(sparql);
        SparqlToSqlTranslator translator;
        auto sql = translator.translate(*ast);
        benchmark::DoNotOptimize(sql);
    }
}
BENCHMARK(BM_TranslateMultipleOptionals);

static void BM_ParseOnly_vs_TripleCount(benchmark::State & state)
{
    std::string sparql = "SELECT ?x WHERE { ";
    int n = state.range(0);
    for (int i = 0; i < n; ++i)
    {
        if (i > 0) sparql += " . ";
        sparql += "?x <:pred" + std::to_string(i) + "> ?v" + std::to_string(i);
    }
    sparql += " }";

    for (auto _ : state)
    {
        SparqlQueryParser parser;
        auto ast = parser.parse(sparql);
        benchmark::DoNotOptimize(ast);
    }
    state.SetItemsProcessed(state.iterations() * n);
}
BENCHMARK(BM_ParseOnly_vs_TripleCount)->RangeMultiplier(2)->Range(1, 32);

static void BM_Translate_vs_TripleCount(benchmark::State & state)
{
    std::string sparql = "SELECT ?x WHERE { ";
    int n = state.range(0);
    for (int i = 0; i < n; ++i)
    {
        if (i > 0) sparql += " . ";
        sparql += "?x <:pred" + std::to_string(i) + "> ?v" + std::to_string(i);
    }
    sparql += " }";

    for (auto _ : state)
    {
        SparqlQueryParser parser;
        auto ast = parser.parse(sparql);
        SparqlToSqlTranslator translator;
        auto sql = translator.translate(*ast);
        benchmark::DoNotOptimize(sql);
    }
    state.SetItemsProcessed(state.iterations() * n);
}
BENCHMARK(BM_Translate_vs_TripleCount)->RangeMultiplier(2)->Range(1, 32);

static void BM_TranslateWithOrderByLimit(benchmark::State & state)
{
    std::string sparql = "SELECT ?name ?age WHERE { ?p <:name> ?name . ?p <:age> ?age } ORDER BY ?name LIMIT 100 OFFSET 10";
    for (auto _ : state)
    {
        SparqlQueryParser parser;
        auto ast = parser.parse(sparql);
        SparqlToSqlTranslator translator;
        auto sql = translator.translate(*ast);
        benchmark::DoNotOptimize(sql);
    }
}
BENCHMARK(BM_TranslateWithOrderByLimit);

static void BM_TranslateDistinct(benchmark::State & state)
{
    std::string sparql = "SELECT DISTINCT ?name WHERE { ?p <rdf:type> <:Person> . ?p <:name> ?name }";
    for (auto _ : state)
    {
        SparqlQueryParser parser;
        auto ast = parser.parse(sparql);
        SparqlToSqlTranslator translator;
        auto sql = translator.translate(*ast);
        benchmark::DoNotOptimize(sql);
    }
}
BENCHMARK(BM_TranslateDistinct);

static void BM_TranslateComplexQuery(benchmark::State & state)
{
    std::string sparql =
        "SELECT DISTINCT ?name ?email WHERE { "
        "?p <rdf:type> <:Person> . ?p <:name> ?name . "
        "OPTIONAL { ?p <:email> ?email } . "
        "?p <:age> ?age . FILTER(?age > 25) "
        "} ORDER BY ?name LIMIT 50";
    for (auto _ : state)
    {
        SparqlQueryParser parser;
        auto ast = parser.parse(sparql);
        SparqlToSqlTranslator translator;
        auto sql = translator.translate(*ast);
        benchmark::DoNotOptimize(sql);
    }
}
BENCHMARK(BM_TranslateComplexQuery);

BENCHMARK_MAIN();
