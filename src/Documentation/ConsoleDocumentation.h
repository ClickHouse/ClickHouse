#pragma once

#include <memory>
#include <Documentation/IDocumentation.h>
#include "Common/Exception.h"

namespace DB
{

class ConsoleDocumentation final: public IDocumentation
{
public:
    ConsoleDocumentation(const String& doc_name, const String& doc_group) : IDocumentation(doc_name, doc_group) {}

private:
    String createDocumentation() const override 
    {
        String documentation;
        size_t des_index = 0;
        size_t ref_index = 0;
        size_t set_index = 0;
        size_t exa_index = 0;
        for (const auto& option: getOrder()) {
            documentation += addHeader(option);
            if (option == "Description")
            {
                documentation += getDescriptions()[des_index];
                ++des_index;
            } else if (option == "Example") {
                documentation += getExamples()[exa_index];
                ++exa_index;
            } else if (option == "Setting") {
                documentation += getSettings()[set_index];
                ++set_index;
            } else if (option == "See also") {
                documentation += getReferences()[ref_index];
                ++ref_index;
            } else {
                //TODO error?
            }
        }

        return documentation;
        
    }

    String addHeader(const String& header_name) const override
    {
        return header_name + ":\n\t";
    }
};

using ConsoleDocumentationPtr = std::shared_ptr<ConsoleDocumentation>;

}
