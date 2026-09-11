#include <Parsers/SPARQL/SparqlQueryParser.h>
#include <Parsers/SPARQL/SparqlToSqlTranslator.h>

#include <gtest/gtest.h>

using namespace DB::SPARQL;

static std::unique_ptr<SelectQuery> parseSparql(const std::string & sparql)
{
    SparqlQueryParser parser;
    return parser.parse(sparql);
}

static std::string translateSparql(const std::string & sparql)
{
    auto ast = parseSparql(sparql);
    SparqlToSqlTranslator translator;
    return translator.translate(*ast);
}


TEST(SparqlParser, BasicProjection)
{
    auto ast = parseSparql("SELECT ?name ?age WHERE { ?p <:name> ?name . ?p <:age> ?age }");
    ASSERT_EQ(ast->projection.size(), 2);
    EXPECT_EQ(ast->projection[0], "?name");
    EXPECT_EQ(ast->projection[1], "?age");
}

TEST(SparqlParser, TriplePatternCount)
{
    auto ast = parseSparql("SELECT ?x WHERE { ?x <rdf:type> <:Person> . ?x <:name> ?n . ?x <:age> ?a }");
    ASSERT_NE(ast->where_clause, nullptr);
    EXPECT_EQ(ast->where_clause->triples.size(), 3);
}

TEST(SparqlParser, IRITermKind)
{
    auto ast = parseSparql("SELECT ?x WHERE { ?x <rdf:type> <:Person> }");
    ASSERT_EQ(ast->where_clause->triples.size(), 1);
    const auto & tp = ast->where_clause->triples[0];
    EXPECT_EQ(tp.subject.kind, Term::Variable);
    EXPECT_EQ(tp.predicate.kind, Term::IRI);
    EXPECT_EQ(tp.predicate.value, "rdf:type");
    EXPECT_EQ(tp.object.kind, Term::IRI);
    EXPECT_EQ(tp.object.value, ":Person");
}

TEST(SparqlParser, OptionalStructure)
{
    auto ast = parseSparql(
        "SELECT ?n ?e WHERE { ?p <:name> ?n . OPTIONAL { ?p <:email> ?e } }");
    ASSERT_NE(ast->where_clause, nullptr);
    EXPECT_EQ(ast->where_clause->triples.size(), 1);
    EXPECT_EQ(ast->where_clause->optionals.size(), 1);
    ASSERT_NE(ast->where_clause->optionals[0].pattern, nullptr);
    EXPECT_EQ(ast->where_clause->optionals[0].pattern->triples.size(), 1);
}

TEST(SparqlParser, UnionStructure)
{
    auto ast = parseSparql(
        "SELECT ?c WHERE { { ?p <:email> ?c } UNION { ?p <:name> ?c } }");
    ASSERT_NE(ast->where_clause, nullptr);
    EXPECT_EQ(ast->where_clause->unions.size(), 1);
    EXPECT_EQ(ast->where_clause->unions[0].alternatives.size(), 2);
}

TEST(SparqlParser, FilterStructure)
{
    auto ast = parseSparql(
        "SELECT ?n WHERE { ?p <:name> ?n . ?p <:age> ?a . FILTER (?a > 25) }");
    ASSERT_NE(ast->where_clause, nullptr);
    EXPECT_EQ(ast->where_clause->filters.size(), 1);
    EXPECT_EQ(ast->where_clause->filters[0]->op, FilterExpr::Gt);
}

TEST(SparqlParser, SemicolonSyntax)
{
    auto ast = parseSparql("SELECT ?n ?a WHERE { ?p <:name> ?n ; <:age> ?a }");
    ASSERT_NE(ast->where_clause, nullptr);
    EXPECT_EQ(ast->where_clause->triples.size(), 2);
    EXPECT_EQ(ast->where_clause->triples[0].subject.value, "?p");
    EXPECT_EQ(ast->where_clause->triples[1].subject.value, "?p");
    EXPECT_EQ(ast->where_clause->triples[0].predicate.value, ":name");
    EXPECT_EQ(ast->where_clause->triples[1].predicate.value, ":age");
}

TEST(SparqlParser, PrefixExpansion)
{
    auto ast = parseSparql(
        "PREFIX foaf: <http://xmlns.com/foaf/0.1/> "
        "SELECT ?n WHERE { ?p foaf:name ?n }");
    ASSERT_EQ(ast->where_clause->triples.size(), 1);
    EXPECT_EQ(ast->where_clause->triples[0].predicate.value, "http://xmlns.com/foaf/0.1/name");
}


TEST(SparqlTranslator, BasicTriplePatternJoin)
{
    std::string sql = translateSparql(
        "SELECT ?name WHERE { ?p <rdf:type> <:Person> . ?p <:name> ?name }");

    EXPECT_NE(sql.find("SELECT t2.object AS name"), std::string::npos);
    EXPECT_NE(sql.find("FROM rdf_triples AS t1"), std::string::npos);
    EXPECT_NE(sql.find("JOIN rdf_triples AS t2 ON t2.subject = t1.subject"), std::string::npos);
    EXPECT_NE(sql.find("t1.predicate = 'rdf:type'"), std::string::npos);
    EXPECT_NE(sql.find("t1.object = ':Person'"), std::string::npos);
    EXPECT_NE(sql.find("t2.predicate = ':name'"), std::string::npos);
    EXPECT_EQ(sql.find("LEFT JOIN"), std::string::npos);
}

TEST(SparqlTranslator, SingleTriplePattern)
{
    std::string sql = translateSparql("SELECT ?s WHERE { ?s <rdf:type> <:Person> }");
    EXPECT_NE(sql.find("SELECT t1.subject AS s"), std::string::npos);
    EXPECT_NE(sql.find("FROM rdf_triples AS t1"), std::string::npos);
    EXPECT_NE(sql.find("t1.predicate = 'rdf:type'"), std::string::npos);
    EXPECT_EQ(sql.find("JOIN"), std::string::npos);
}

TEST(SparqlTranslator, OptionalPattern)
{
    std::string sql = translateSparql(
        "SELECT ?name ?email WHERE { "
        "?p <rdf:type> <:Person> . "
        "?p <:name> ?name . "
        "OPTIONAL { ?p <:email> ?email } }");

    EXPECT_NE(sql.find("SELECT t2.object AS name, t3.object AS email"), std::string::npos);
    EXPECT_NE(sql.find("LEFT JOIN rdf_triples AS t3 ON"), std::string::npos);
    EXPECT_NE(sql.find("t3.subject = t1.subject"), std::string::npos);
    EXPECT_NE(sql.find("t3.predicate = ':email'"), std::string::npos);
}

TEST(SparqlTranslator, OptionalConstantsInOnClause)
{
    std::string sql = translateSparql(
        "SELECT ?name ?email WHERE { "
        "?p <:name> ?name . "
        "OPTIONAL { ?p <:email> ?email } }");

    size_t left_join_pos = sql.find("LEFT JOIN");
    size_t where_pos = sql.find("WHERE");
    size_t email_pred_pos = sql.find("t2.predicate = ':email'");
    ASSERT_NE(left_join_pos, std::string::npos);
    ASSERT_NE(email_pred_pos, std::string::npos);
    EXPECT_LT(left_join_pos, email_pred_pos);
    if (where_pos != std::string::npos)
        EXPECT_LT(email_pred_pos, where_pos);
}

TEST(SparqlTranslator, UnionPattern)
{
    std::string sql = translateSparql(
        "SELECT ?contact WHERE { "
        "{ ?p <:email> ?contact } UNION { ?p <:name> ?contact } }");

    EXPECT_NE(sql.find("UNION ALL"), std::string::npos);
    EXPECT_NE(sql.find("t1.predicate = ':email'"), std::string::npos);
    EXPECT_NE(sql.find("t1.predicate = ':name'"), std::string::npos);
    EXPECT_NE(sql.find("t1.object AS contact"), std::string::npos);
}

TEST(SparqlTranslator, UnionAliasReset)
{
    std::string sql = translateSparql(
        "SELECT ?x WHERE { { ?x <:a> ?y } UNION { ?x <:b> ?y } }");

    size_t union_pos = sql.find("UNION ALL");
    ASSERT_NE(union_pos, std::string::npos);
    size_t first_t1 = sql.find("FROM rdf_triples AS t1");
    size_t second_t1 = sql.find("FROM rdf_triples AS t1", union_pos);
    EXPECT_NE(first_t1, std::string::npos);
    EXPECT_NE(second_t1, std::string::npos);
}

TEST(SparqlTranslator, FilterWithToInt64)
{
    std::string sql = translateSparql(
        "SELECT ?name WHERE { "
        "?p <rdf:type> <:Person> . "
        "?p <:name> ?name . "
        "?p <:age> ?age . "
        "FILTER (?age > 25) }");

    EXPECT_NE(sql.find("toInt64OrNull(t3.object) > 25"), std::string::npos);
    EXPECT_NE(sql.find("t3.predicate = ':age'"), std::string::npos);
    EXPECT_NE(sql.find("t2.object AS name"), std::string::npos);
}

TEST(SparqlTranslator, FilterLessThan)
{
    std::string sql = translateSparql(
        "SELECT ?n WHERE { ?p <:name> ?n . ?p <:age> ?a . FILTER (?a < 30) }");
    EXPECT_NE(sql.find("toInt64OrNull(t2.object) < 30"), std::string::npos);
}

TEST(SparqlTranslator, FilterEquality)
{
    std::string sql = translateSparql(
        "SELECT ?p WHERE { ?p <:name> ?n . FILTER (?n = \"Alice\") }");
    EXPECT_NE(sql.find("t1.object = 'Alice'"), std::string::npos);
}

TEST(SparqlTranslator, FilterLogicalAnd)
{
    std::string sql = translateSparql(
        "SELECT ?n WHERE { ?p <:name> ?n . ?p <:age> ?a . FILTER (?a > 20 && ?a < 30) }");
    EXPECT_NE(sql.find("toInt64OrNull(t2.object) > 20"), std::string::npos);
    EXPECT_NE(sql.find("toInt64OrNull(t2.object) < 30"), std::string::npos);
    EXPECT_NE(sql.find(" AND "), std::string::npos);
}

TEST(SparqlTranslator, VariableBindingFirstOccurrence)
{
    std::string sql = translateSparql(
        "SELECT ?name WHERE { ?p <:name> ?name . ?p <:age> ?age }");

    EXPECT_NE(sql.find("t1.object AS name"), std::string::npos);
    EXPECT_NE(sql.find("t2.subject = t1.subject"), std::string::npos);
}

TEST(SparqlTranslator, ThreeWayJoin)
{
    std::string sql = translateSparql(
        "SELECT ?n ?a WHERE { ?p <rdf:type> <:Person> . ?p <:name> ?n . ?p <:age> ?a }");
    EXPECT_NE(sql.find("FROM rdf_triples AS t1"), std::string::npos);
    EXPECT_NE(sql.find("JOIN rdf_triples AS t2 ON"), std::string::npos);
    EXPECT_NE(sql.find("JOIN rdf_triples AS t3 ON"), std::string::npos);
    EXPECT_NE(sql.find("t2.subject = t1.subject"), std::string::npos);
    EXPECT_NE(sql.find("t3.subject = t1.subject"), std::string::npos);
}

TEST(SparqlTranslator, PrewhereForFirstTable)
{
    auto sql = translateSparql("SELECT ?name WHERE { ?p <rdf:type> <:Person> . ?p <:name> ?name }");
    EXPECT_NE(sql.find("PREWHERE"), std::string::npos);
    EXPECT_NE(sql.find("t1.predicate = 'rdf:type'"), std::string::npos);
}

TEST(SparqlTranslator, PrewhereNotInWhere)
{
    auto sql = translateSparql("SELECT ?name WHERE { ?p <rdf:type> <:Person> . ?p <:name> ?name }");
    auto prewhere_pos = sql.find("PREWHERE");
    EXPECT_NE(prewhere_pos, std::string::npos);
    auto where_pos = sql.find("\nWHERE", prewhere_pos);
    EXPECT_NE(where_pos, std::string::npos);
    auto where_block = sql.substr(where_pos);
    EXPECT_EQ(where_block.find("t1.predicate"), std::string::npos);
}

TEST(SparqlTranslator, LimitClause)
{
    auto sql = translateSparql("SELECT ?s WHERE { ?s ?p ?o } LIMIT 10");
    EXPECT_NE(sql.find("LIMIT 10"), std::string::npos);
}

TEST(SparqlTranslator, OffsetClause)
{
    auto sql = translateSparql("SELECT ?s WHERE { ?s ?p ?o } LIMIT 10 OFFSET 5");
    EXPECT_NE(sql.find("LIMIT 10"), std::string::npos);
    EXPECT_NE(sql.find("OFFSET 5"), std::string::npos);
}

TEST(SparqlTranslator, OrderByVariable)
{
    auto sql = translateSparql("SELECT ?name WHERE { ?p <:name> ?name } ORDER BY ?name");
    EXPECT_NE(sql.find("ORDER BY"), std::string::npos);
    EXPECT_NE(sql.find("t1.object"), std::string::npos);
}

TEST(SparqlTranslator, OrderByDesc)
{
    auto sql = translateSparql("SELECT ?name WHERE { ?p <:name> ?name } ORDER BY DESC(?name)");
    EXPECT_NE(sql.find("ORDER BY"), std::string::npos);
    EXPECT_NE(sql.find("DESC"), std::string::npos);
}

TEST(SparqlTranslator, SelectDistinct)
{
    auto sql = translateSparql("SELECT DISTINCT ?name WHERE { ?p <:name> ?name }");
    EXPECT_NE(sql.find("SELECT DISTINCT"), std::string::npos);
}

TEST(SparqlTranslator, CombinedModifiers)
{
    auto sql = translateSparql("SELECT ?name WHERE { ?p <:name> ?name } ORDER BY ?name LIMIT 100 OFFSET 20");
    EXPECT_NE(sql.find("ORDER BY"), std::string::npos);
    EXPECT_NE(sql.find("LIMIT 100"), std::string::npos);
    EXPECT_NE(sql.find("OFFSET 20"), std::string::npos);
}

TEST(SparqlTranslator, SyntaxError)
{
    EXPECT_THROW(translateSparql("NOT SPARQL AT ALL"), std::runtime_error);
}

TEST(SparqlTranslator, OnlySelectSupported)
{
    EXPECT_THROW(translateSparql("CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"), std::runtime_error);
}

TEST(SparqlTranslator, UnboundVariableCleanName)
{
    auto sql = translateSparql("SELECT ?name ?email WHERE { ?p <:name> ?name }");
    EXPECT_NE(sql.find("NULL AS email"), std::string::npos);
    EXPECT_EQ(sql.find("NULL AS ?email"), std::string::npos);
}

TEST(SparqlTranslator, ChainJoinObjectToSubject)
{
    auto sql = translateSparql(
        "SELECT ?name ?dept WHERE { ?p <:name> ?name . ?p <:worksAt> ?d . ?d <:deptName> ?dept }");
    EXPECT_NE(sql.find("t3.subject = t2.object"), std::string::npos);
    EXPECT_NE(sql.find("t3.predicate = ':deptName'"), std::string::npos);
}

TEST(SparqlParser, PrefixMultiple)
{
    auto ast = parseSparql(
        "PREFIX ex: <http://example.org/> "
        "PREFIX foaf: <http://xmlns.com/foaf/0.1/> "
        "SELECT ?n WHERE { ?p foaf:name ?n . ?p ex:age ?a }");
    ASSERT_EQ(ast->where_clause->triples.size(), 2);
    EXPECT_EQ(ast->where_clause->triples[0].predicate.value, "http://xmlns.com/foaf/0.1/name");
    EXPECT_EQ(ast->where_clause->triples[1].predicate.value, "http://example.org/age");
}

TEST(SparqlTranslator, FilterInsideOptionalGoesToOn)
{
    auto sql = translateSparql(
        "SELECT ?name ?age WHERE { ?p <:name> ?name . OPTIONAL { ?p <:age> ?age . FILTER(?age > 25) } }");
    auto left_join_pos = sql.find("LEFT JOIN");
    auto on_pos = sql.find(" ON ", left_join_pos);
    ASSERT_NE(left_join_pos, std::string::npos);
    ASSERT_NE(on_pos, std::string::npos);
    auto on_clause = sql.substr(on_pos);
    auto where_pos = on_clause.find("\nWHERE");
    auto filter_pos = on_clause.find("toInt64OrNull");
    EXPECT_NE(filter_pos, std::string::npos);
    if (where_pos != std::string::npos)
        EXPECT_LT(filter_pos, where_pos);
}

TEST(SparqlTranslator, EmptyResultNoJoins)
{
    auto sql = translateSparql("SELECT ?s WHERE { ?s <rdf:type> <:Animal> }");
    EXPECT_NE(sql.find("FROM rdf_triples AS t1"), std::string::npos);
    EXPECT_EQ(sql.find("JOIN"), std::string::npos);
}

TEST(SparqlTranslator, PrewhereMultipleConditions)
{
    auto sql = translateSparql("SELECT ?o WHERE { ?s <rdf:type> <:Person> }");
    auto prewhere_pos = sql.find("PREWHERE");
    ASSERT_NE(prewhere_pos, std::string::npos);
    auto prewhere_block = sql.substr(prewhere_pos);
    EXPECT_NE(prewhere_block.find("t1.predicate = 'rdf:type'"), std::string::npos);
    EXPECT_NE(prewhere_block.find("t1.object = ':Person'"), std::string::npos);
}

TEST(SparqlTranslator, RegexFilter)
{
    auto sql = translateSparql(
        "SELECT ?name WHERE { ?p <:name> ?name . FILTER regex(?name, \"Alice\") }");
    EXPECT_NE(sql.find("match(t1.object, 'Alice')"), std::string::npos);
}

TEST(SparqlTranslator, FourWayJoin)
{
    auto sql = translateSparql(
        "SELECT ?name ?age ?email WHERE { "
        "?p <rdf:type> <:Person> . ?p <:name> ?name . ?p <:age> ?age . ?p <:email> ?email }");
    EXPECT_NE(sql.find("JOIN rdf_triples AS t4 ON"), std::string::npos);
    EXPECT_NE(sql.find("t4.subject = t1.subject"), std::string::npos);
}


TEST(SparqlTranslator, TripleReorderingMostSelectiveFirst)
{
    auto sql = translateSparql(
        "SELECT ?name WHERE { ?p <:name> ?name . ?p <rdf:type> <:Person> }");
    auto prewhere_pos = sql.find("PREWHERE");
    ASSERT_NE(prewhere_pos, std::string::npos);
    EXPECT_NE(sql.find("t1.predicate = 'rdf:type'"), std::string::npos);
    EXPECT_NE(sql.find("t1.object = ':Person'"), std::string::npos);
}

TEST(SparqlTranslator, UnusedOptionalEliminated)
{
    auto sql = translateSparql(
        "SELECT ?name WHERE { ?p <:name> ?name . OPTIONAL { ?p <:email> ?email } }");
    EXPECT_EQ(sql.find("LEFT JOIN"), std::string::npos);
}

TEST(SparqlTranslator, OptionalKeptWhenVarProjected)
{
    auto sql = translateSparql(
        "SELECT ?name ?email WHERE { ?p <:name> ?name . OPTIONAL { ?p <:email> ?email } }");
    EXPECT_NE(sql.find("LEFT JOIN"), std::string::npos);
}

TEST(SparqlTranslator, OptionalKeptByOuterFilter)
{
    auto sql = translateSparql(
        "SELECT ?name WHERE { ?p <:name> ?name . OPTIONAL { ?p <:age> ?age } . FILTER(?age > 25) }");
    EXPECT_NE(sql.find("LEFT JOIN"), std::string::npos);
}
