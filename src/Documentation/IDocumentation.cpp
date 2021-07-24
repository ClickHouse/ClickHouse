#include <Documentation/IDocumentation.h>
#include "Common/Exception.h"
#include "common/types.h"

namespace DB
{

IDocumentation::IDocumentation(const String& doc_name, const String& doc_group): name(doc_name), group(doc_group) {}

String IDocumentation::getDocumentation() const { return documentation; }

String IDocumentation::createCodeSection(const String &code) { return code; }

void IDocumentation::addExample(const String& input, const String& query, const String& result)
{
    inputs.push_back(input);
    queries.push_back(query);
    results.push_back(result);
    addHeader("Example");
    if (!input.empty())
        addDescription("Input", createCodeSection(input));
    addDescription("Query", createCodeSection(query));
    addDescription("Result", createCodeSection(result));
}

void IDocumentation::addReference(const String& ref_name, const String& source)
{
    references.push_back(ref_name);
    sources.push_back(source);
    documentation += createReference(ref_name, source);
}

void IDocumentation::addDescription(const String& header, const String& description)
{
    if (!header.empty())
        addHeader(header);
    documentation += description;
}

void IDocumentation::addReferencesToDocs()
{
    addHeader("See also");
    for (size_t i = 0; i < references.size(); ++i)
    {
        documentation += references[i] + ":" + sources[i] + "\n\t";
    }
}

void IDocumentation::addHeader(const String &header_name)
{
    documentation += createHeader(header_name);
}

}
