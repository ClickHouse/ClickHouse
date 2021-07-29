#pragma once

#include <Documentation/IMarkdownGenerator.h>

namespace DB
{

class SimpleGenerator : public IMarkdownGenerator
{
protected:
std::string makeDocumentation(const std::string& text) override
{
    return text;
}

};

}
