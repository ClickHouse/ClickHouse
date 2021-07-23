#pragma once

#include <string>
#include <Common/IFactoryWithAliases.h>
#include "Documentation/IDocumentation.h"

namespace DB
{
/// Find documentation by name.
class TableDocumentationFactory : private boost::noncopyable,
                                  public IFactoryWithAliases<IDocumentationPtr>
{
public:
    static TableDocumentationFactory & instance();

    void registerDocForFunction(const std::string& name, Value documentation, CaseSensitiveness case_sensitiveness = CaseSensitive);

    std::string tryGet(const std::string& name_param);
private:
    using Docs = std::unordered_map<String, IDocumentationPtr>;

    Docs documentations;
    Docs case_insensitive_documentations;

    const Docs & getMap() const override { return documentations; }
    const Docs & getCaseInsensitiveMap() const override { return case_insensitive_documentations; }
    std::string getFactoryName() const override { return "TableFunctions"; }
    
};
                
}
