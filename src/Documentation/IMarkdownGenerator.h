#pragma once

#include <iostream>
#include <string>

namespace DB
{

/// This interface is made for different generators 
// (for example, if documentation in factory will have markdown syntax, we can generate easy,
/// otherwise we will need to transform it)
class IMarkdownGenerator
{
public:
void printDocumentation(const std::string& group, const std::string& name, const std::string& path="");
virtual ~IMarkdownGenerator() = default;

protected:
virtual std::string makeDocumentation(const std::string& text) = 0;

std::string findDocumentation(const std::string& group, const std::string& name);

};

}
