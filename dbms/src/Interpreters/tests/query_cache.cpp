#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/parseQuery.h>
#include <Parsers/formatAST.h>
#include <Interpreters/QueryCache.h>

using namespace DB;
using namespace std;

#ifdef ASSERT
#undef ASSERT
#endif
#define ASSERT(cond) \
do \
{ \
    if (!(cond)) \
    { \
        cout << __FILE__ << ":" << __LINE__ << ":" \
            << "Assertion " << #cond << " failed.\n"; \
        exit(1); \
    } \
} \
while (0)

using Ptr = QueryCache::MappedPtr;

Ptr toPtr(const QueryResult & res)
{
    return Ptr(make_shared<QueryResult>(res));
}

void test_basic_operations()
{
    const char * KEY = "query";

    QueryCache cache(1024*1024);

    ASSERT(!cache.get(KEY));
    cache.set(KEY, toPtr(QueryResult{}));
    ASSERT(cache.get(KEY));
}

void test_get_tables()
{
    ParserQueryWithOutput parser;

    {
        std::string input = " SELECT v FROM default.t1";

        ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0);
        std::set<DatabaseAndTableWithAlias> tables;
        getTables(*ast, tables, "curdb");

        ASSERT(tables.size() == 1);
        ASSERT(tables.find({"default", "t1", ""}) != tables.end());
    }

    {
        std::string input = "SELECT * FROM default.t1 LEFT JOIN default.t2 ON t1.v = t2.v";

        ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0);
        std::set<DatabaseAndTableWithAlias> tables;
        getTables(*ast, tables, "curdb");

        ASSERT(tables.size() == 2);
        ASSERT(tables.find({"default", "t1", ""}) != tables.end());
        ASSERT(tables.find({"default", "t2", ""}) != tables.end());
    }

    {
        std::string input =
            "select * from default.t1 LEFT JOIN default.t2 ON t1.v = t2.v "
            "WHERE t1.v in (SELECT v FROM default.t3)";

        ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0);
        std::set<DatabaseAndTableWithAlias> tables;
        getTables(*ast, tables, "curdb");

        ASSERT(tables.size() == 3);
        ASSERT(tables.find({"default", "t1", ""}) != tables.end());
        ASSERT(tables.find({"default", "t2", ""}) != tables.end());
        ASSERT(tables.find({"default", "t3", ""}) != tables.end());
    }

    {
        std::string input =
            "SELECT * FROM default.t4 LEFT JOIN default.t2 ON t4.v = t2.v "
            "UNION ALL SELECT * FROM default.t1 LEFT JOIN default.t2 ON t1.v = t2.v WHERE t1.v IN (SELECT v FROM default.t3)";

        ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0);
        std::set<DatabaseAndTableWithAlias> tables;
        getTables(*ast, tables, "curdb");

        ASSERT(tables.size() == 4);
        ASSERT(tables.find({"default", "t1", ""}) != tables.end());
        ASSERT(tables.find({"default", "t2", ""}) != tables.end());
        ASSERT(tables.find({"default", "t3", ""}) != tables.end());
        ASSERT(tables.find({"default", "t4", ""}) != tables.end());
    }

    {
        std::string input =
            "select * from default.t1 as _1 LEFT JOIN default.t2 ON t1.v = t2.v "
            "WHERE t1.v in (SELECT v FROM default.t3)";

        ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0);

        std::set<DatabaseAndTableWithAlias> tables;
        auto * select_union = ast->as<ASTSelectWithUnionQuery>();
        auto * select = select_union->list_of_selects->children[0]->as<ASTSelectQuery>();
        getTables(*select->where(), tables, "curdb");

        ASSERT(tables.size() == 1);
        ASSERT(tables.find({"default", "t3", ""}) != tables.end());
    }

    {
        std::string input =
            "SELECT * FROM default.t1 AS kkk LEFT JOIN default.t2 ON t1.v = t2.v "
            "WHERE t1.v IN (SELECT v FROM default.t3 LEFT JOIN default.t4 ON t3.v = t4.v)";

        ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0);

        std::set<DatabaseAndTableWithAlias> tables;
        auto * select_union = ast->as<ASTSelectWithUnionQuery>();
        auto * select = select_union->list_of_selects->children[0]->as<ASTSelectQuery>();
        getTables(*select->where(), tables, "curdb");

        ASSERT(tables.size() == 2);
        ASSERT(tables.find({"default", "t3", ""}) != tables.end());
        ASSERT(tables.find({"default", "t4", ""}) != tables.end());
    }

    {
        std::string input =
            "SELECT * FROM default.t1 AS kkk LEFT JOIN default.t2 ON t1.v = t2.v "
            "WHERE t1.v IN (SELECT v FROM t3 ARRAY JOIN ary LEFT JOIN t4 ON t3.v = t4.v)";

        ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0);

        std::set<DatabaseAndTableWithAlias> tables;
        auto * select_union = ast->as<ASTSelectWithUnionQuery>();
        auto * select = select_union->list_of_selects->children[0]->as<ASTSelectQuery>();
        getTables(*select->where(), tables, "curdb");

        ASSERT(tables.size() == 2);
        ASSERT(tables.find({"curdb", "t3", ""}) != tables.end());
        ASSERT(tables.find({"curdb", "t4", ""}) != tables.end());
    }
}

int main(int, char **)
{
    test_basic_operations();
    test_get_tables();

    return 0;
}

