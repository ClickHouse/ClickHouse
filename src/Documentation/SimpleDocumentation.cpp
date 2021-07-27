#include <Documentation/SimpleDocumentation.h>

namespace DB
{

IDocumentationPtr makeSimpleDocumentation(const char * docs)
{
    SimpleDocumentation* documentation = new SimpleDocumentation(docs);
    return std::unique_ptr<SimpleDocumentation>(documentation);
}

SimpleDocumentation::SimpleDocumentation(const char* docs) : IDocumentation(docs) {}

std::string SimpleDocumentation::getDocumentation() const
{
    return IDocumentation::documentation;
}

}
