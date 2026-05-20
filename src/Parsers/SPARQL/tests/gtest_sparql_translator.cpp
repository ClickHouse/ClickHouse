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

TEST(SparqlTranslator, SyntaxError)
{
    EXPECT_THROW(translateSparql("NOT SPARQL AT ALL"), std::runtime_error);
}

TEST(SparqlTranslator, OnlySelectSupported)
{
    EXPECT_THROW(translateSparql("CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"), std::runtime_error);
}
