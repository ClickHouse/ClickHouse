#pragma once

#include <Documentation/IDocumentation.h>
#include "common/types.h"

namespace DB
{

class MarkdownDocumentation final: public IDocumentation
{
public:
    MarkdownDocumentation(const String& doc_name, const String& doc_group) : IDocumentation(doc_name, doc_group) {}

    static String createCodeSection(const String &code) {return "\n'''" + code + "'''\n"; }
private:
    String createHeader(const String& header_name) const override
    {
        return "\n # " + header_name + ":\n\t";
    }

    String createReference(const String& ref_name, const String& source) const override
    { 
        return "[" + ref_name + "]" + "(" + source + ")";
    }
};

using ConsoleDocumentationPtr = std::shared_ptr<MarkdownDocumentation>;
}
