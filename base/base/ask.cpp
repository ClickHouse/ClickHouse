#include <base/ask.h>

#include <iostream>


bool ask(std::string question)
{
    while (true)
    {
        std::string answer;
        std::cout << question;
        std::getline(std::cin, answer);
        if (!std::cin.good())
            return false;

        if (answer.empty() || answer == "n" || answer == "N")
            return false;
        if (answer == "y" || answer == "Y")
            return true;
    }
}
