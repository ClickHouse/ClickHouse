#include <Documentation/IDocumentation.h>
#include "common/types.h"

namespace DB
{

IDocumentation::IDocumentation(const String& doc_name, const String& doc_group): name(doc_name), group(doc_group) {}

void IDocumentation::addDescription(const String& description)
{
    order.push_back("Description");
    descriptions.push_back(description);
}

void IDocumentation::addExample(const String& example)
{
    order.push_back("Example");
    examples.push_back(example);
}

void IDocumentation::addSetting(const String& setting)
{
    order.push_back("Setting");
    settings.push_back(setting);
}

void IDocumentation::addReference(const String& reference)
{
    order.push_back("See also");
    references.push_back(reference);
}

String IDocumentation::getDocumentation() const { return createDocumentation(); }

const std::vector<String>& IDocumentation::getOrder()        const { return order; } 
const std::vector<String>& IDocumentation::getReferences()   const { return references; } 
const std::vector<String>& IDocumentation::getDescriptions() const { return descriptions; } 
const std::vector<String>& IDocumentation::getExamples()     const { return examples; } 
const std::vector<String>& IDocumentation::getSettings()     const { return settings; } 
}
