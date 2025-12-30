#include <iostream>

#include <IO/Ask.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{

bool ask(std::string question, ReadBuffer & in, WriteBuffer & out)
{
    while (true)
    {
        std::string answer;
        writeText(question, out);
        out.next();
        readStringUntilNewlineInto(answer, in);
        skipToNextLineOrEOF(in);

        if (answer.empty() || answer == "n" || answer == "N")
            return false;
        if (answer == "y" || answer == "Y")
            return true;
    }
}

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

}
