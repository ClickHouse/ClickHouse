#pragma once

#include <Documentation/IDocumentation.h>
#include "common/types.h"

namespace DB
{

class ConsoleDocumentation final: public IDocumentation
{
public:
    ConsoleDocumentation(const String& doc_name, const String& doc_group) : IDocumentation(doc_name, doc_group) {}

    static String createCodeSection(const String &code) {return "\n" + code + "\n"; }
private:
    String createHeader(const String& header_name) const override
    {
        return "\n" + header_name + ":\n\t";
    }

    String createReference(const String& ref_name, const String& /*source*/) const override
    { 
        //TODO may be there is a better way to create references?
        return ref_name + " (see also)";
    }
};

using ConsoleDocumentationPtr = std::shared_ptr<ConsoleDocumentation>;
}
