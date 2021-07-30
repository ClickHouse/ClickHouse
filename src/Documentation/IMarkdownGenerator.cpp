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

// print information in stdout or try to put it in file
void IMarkdownGenerator::printDocumentation(const std::string& group, const std::string& name, const std::string& path)
{
    if (path.empty())
        std::cout << findDocumentation(group, name) << std::endl;
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

template <typename Factory>
std::string IMarkdownGenerator::processName(const std::string& name)
{
    if (name.empty())
    {
        const auto & names = Factory::instance().getAllRegisteredNames();
        std::string output = "Choose one of names: ";
        for (const auto& fun_name: names)
            output += fun_name + ", ";
        return output;
    }
    return makeDocumentation(Factory::instance().getDocumentation(name));
}

/*
 * If only group is known, then show it's content.
 * If we know name and group, try to find it in appropriate factory.
 * Otherwise, show user that information was not found
 */
std::string IMarkdownGenerator::findDocumentation(const std::string& group, const std::string& name)
{
    if (group == "table_functions")
        return processName<TableFunctionFactory>(name);
    else if (group == "aggregate_function_combinators")
        return processName<AggregateFunctionCombinatorFactory>(name);
    else
        std::cout << "Group " + group + " not found." << std::endl;
    
    //TODO: maybe throw an error
    return "Failed to generate documentation";
}

}
