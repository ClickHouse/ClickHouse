#include <cstddef>
#include <ostream>
#include <string>
#include <Documentation/SimpleGenerator.h>
#include <Functions/registerFunctions.h>
#include <TableFunctions/registerTableFunctions.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include "common/types.h"
#include <unordered_set>

namespace DB
{

// Documentation generator have two modes:
//    * single query processing
//    * many query processing
class DocumentationGenerator
{
public:
    DocumentationGenerator() = default;
    explicit DocumentationGenerator(const std::string& category, const std::string& object="", const std::string& path_to_write=""): group(category), 
                                                                                                                                     name(object),
                                                                                                                                     path(path_to_write) {}

    void completeQuery();

    void processManyQueries();

private:
    std::unordered_set<std::string> quit_options{"exit", "quit", "logout", "учше", "йгше", "дщпщге", "exit;", "quit;", "учшеж",
                                                 "йгшеж", "q", "й", "\\q", "\\Q", "\\й", "\\Й", ":q", "Жй", "n", "n;", "т", "Т", 
                                                 "N", "N;", "т", " T;"};

    std::unordered_set<std::string> agree_options{"Yes", "YES", "yes", "Yes;", "YES;", "yes;", "Y", "y", "Y;", "y;",
                                                  "Нуы", "НУЫ", "нуы", "Нуы;", "НУЫ;", "нуы;", "Н", "н", "Н;", "н;"};

    DB::SimpleGenerator generator;

    bool wantToWatchDocumentation();

    void readQuery();

    std::string group;
    std::string name;
    std::string path;
    
};

void DocumentationGenerator::completeQuery()
{
    generator.printDocumentation(group, name, path);
}

bool DocumentationGenerator::wantToWatchDocumentation()
{
    std::string user_command;
    while (true)
    {
        std::cout << "Would you like to get documentation?[Y/n]" << std::endl;
        std::cin >> user_command;

        // To prevent reading '\n' in next function
        std::cin.get();

        if (agree_options.find(user_command) != agree_options.end())
            return true;
        if (quit_options.find(user_command) != quit_options.end())
            return false;
        std::cout << "Unknown command, try again." << std::endl;
    }
}

void DocumentationGenerator::readQuery()
{
    size_t mode = 0;

    char letter;
    
    while (std::cin.get(letter))
    {
        // if end of line, finish processing query
        if (letter == '\n' || letter == '\4')
            return;

        if (letter == ' ')
        {
            ++mode;
            if (mode > 2)
            {
                std::cout << "Wrong number of arguments. Expected not more than three: group name path_to_file." << std::endl;
                return;
            }
            continue;
        }

        switch (mode)
        {
        case 0:
            group += letter;
            break;
        case 1:
            name += letter;
            break;
        case 2:
            path += letter;
            break;
        default:
            std::cout << "Wrong mode " << mode << " communicate support, if you meet this." << std::endl;
            return;
        }
    }
}

void DocumentationGenerator::processManyQueries()
{
    while (true)
    {
        if (!wantToWatchDocumentation())
            break;

        // clear previous information
        group.clear();
        name.clear();
        path.clear();

        std::cout << "Enter group, name of object (or function) and path (optional)." << std::endl;
        readQuery();
        completeQuery();
    }

}

}

// Print documentation
// Format of input is: group name path
// where 
// - group means number of functions, types etc, with common part (for example table_functions)
// for some objects group means system database, which they belong to
// - name is a name of function, type etc.
// -path (optional) shows where documentation should be put (be default it is printed)
int entryDocumentationGenerator(int argc, char** argv)
{
    // need to 
    DB::registerFunctions();
    DB::registerAggregateFunctions();
    DB::registerTableFunctions();

    if (argc > 1)
    {
        if (argc == 3)
        {
            DB::DocumentationGenerator(argv[1], argv[2]).completeQuery();
            return 0;
        }
        else if (argc == 4)
        {
            DB::DocumentationGenerator(argv[1], argv[2], argv[3]).completeQuery();
            return 0;
        }
        else 
        {
            std::cout << "Wrong number of parameters" << std::endl;
            return -1;
        }
    }
    
    DB::DocumentationGenerator().processManyQueries();
    
    return 0;
}
