#include <Documentation/NullDocumentation.h>
#include <Documentation/ConsoleDocumentation.h>
#include <Documentation/TableDocumentationFactory.h>

namespace DB
{

void registerDocumentations()
{
    registerNull<ConsoleDocumentation, TableDocumentationFactory>();
}

}
