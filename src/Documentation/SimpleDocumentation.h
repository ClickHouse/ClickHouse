#pragma once

#include <memory>
#include <Documentation/IDocumentation.h>

namespace DB
{

class SimpleDocumentation : public IDocumentation
{
public:
    explicit SimpleDocumentation(const char* docs);

    std::string getDocumentation() const override;
};

IDocumentationPtr makeSimpleDocumentation(const char * docs);

}
