#include <memory>

namespace DB
{

template <class Documentation, class Factory>
void registerNull()
{
    auto null_doc = std::make_shared<Documentation>("null", "TableFunctions");

    null_doc->addDescription(
        "Description",
        "Creates a temporary table of the specified structure with the "
        );
    null_doc->addReference("Null", "special null");
    null_doc->addDescription(
        "",  
        "table engine. According to the `Null`-engine properties, the table data is ignored and \
the table itself is immediately droped right after the query execution. The function is \
used for the convenience of test writing and demonstrations.");

    null_doc->addDescription(
        "Syntax",
        Documentation::createCodeSection("\tsql\n\tnull('structure')\n"));
    null_doc->addDescription("Parameter", "-`structure` — A list of columns and column types.");
    null_doc->addReference("String", "data_types string");
    null_doc->addDescription("Returned value", "A temporary `Null`-engine table with the specified structure.");
    
    /// Don't know how to parse this type of example. TODO
    null_doc->addDescription("Example", "Query with the `null` function:\n" +
    Documentation::createCodeSection("sql\nINSERT INTO function null('x UInt64') SELECT * FROM numbers_mt(1000000000);\n") +
    "can replace three queries:" +
    Documentation::createCodeSection("\tsql\n\t\
CREATE TABLE t (x UInt64) ENGINE = Null;\n\t\
INSERT INTO t SELECT * FROM numbers_mt(1000000000);\n\t\
DROP TABLE IF EXISTS t;\n\t")
);
    null_doc->addReferencesToDocs();

    Factory::instance().registerDocForFunction("null", null_doc);
}

}
