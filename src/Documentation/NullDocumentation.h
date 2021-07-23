#include <memory>
#include <Documentation/ConsoleDocumentation.h>
#include <Documentation/TableDocumentationFactory.h>

namespace DB
{

void registerNull()
{
    auto null_doc = std::make_shared<ConsoleDocumentation>("null", "TableFunctions");

    null_doc->addDescription(
        "Creates a temporary table of the specified structure with the [Null](../../engines/table-engines/special/null.md) table engine. According to the `Null`-engine properties, the table data is ignored and the table itself is immediately droped right after the query execution. The function is used for the convenience of test writing and demonstrations."
        );

    TableDocumentationFactory::instance().registerDocForFunction("null", null_doc);
}

}
