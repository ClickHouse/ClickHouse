#include <fstream>
#include <ostream>
#include <string>
#include <Documentation/IMarkdownGenerator.h>
#include "Common/Exception.h"
#include "Common/typeid_cast.h"
#include "AggregateFunctions/AggregateFunctionCombinatorFactory.h"
#include "TableFunctions/TableFunctionFactory.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void IMarkdownGenerator::printDocumentation(const std::string& group, const std::string& name, const std::string& path)
{
    if (path.empty())
        std::cout << findDocumentation(group, name);
    else
    {
        std::ofstream input(path);
        if (!input)
        {
            std::cout << "Failed to open file on path " + path + "." << std::endl;
            return;
        }

        input << findDocumentation(group, name);
    }
    
}

std::string IMarkdownGenerator::findDocumentation(const std::string& group, const std::string& name)
{
    if (group == "table_functions")
        return makeDocumentation(TableFunctionFactory::instance().getDocumentation(name));
    else if (group == "aggregate_function_combinators")
        return makeDocumentation(AggregateFunctionCombinatorFactory::instance().getDocumentation(name));
    else
        std::cout << "Group " + group + " not found." << std::endl;
    return "Failed to generate documentation";
}

}
