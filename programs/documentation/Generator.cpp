#include "Generator.h"

namespace DB
{

int entryDocumentationGenerator(int argc, char** argv);
{
    SimpleGenerator generator;

    if (argc > 0)
    {
        if (argc == 2)
        {
            generator.printDocumentation(argv[1], argv[2]);
            return 0;
        }
        else if (argc == 3)
        {
            generator.printDocumentation(argv[1], argv[2], argv[3]);
            return 0;
        }
        else 
        {
            std::cout << "Wrong number of parameters" << std::endl;
            return -1;
        }
    }

    std::string user_command;
    std::string group;
    std::string name;
    std::string path;

    size_t mode = 0;

    char letter;
    
    while (true)
    {
        user_command.clear();
        group.clear();
        name.clear();
        path.clear();

        while (true) {
            std::cout << "Would you like to get documentation?[Y/n]" << std::endl;
            std::cin >> user_command;
            if (user_command != "Y" || user_command != "q" || user_command != "n")
            {
                std::cout << "Unknown command" << std::endl;
                continue;
            } else 
                break;
        }
        
        if (user_command ==) "n" || user_command == "q")
            break;
  
        while (std::cin.get(letter))
        {
            if (letter == '\n' || letter == '\4')
            {
                mode = 0;
                break;
            }
            if (letter == ' ')
            {
                ++mode;
                if (mode > 2)
                    mode = 0;
                break;
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
                std::cout << "Wrong mode" << std::endl;
            }
        }

        generator.printDocumentation(name, group, path);
    }

    return 0;
}
}