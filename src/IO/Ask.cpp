#include <iostream>

#include <IO/Ask.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{

bool ask(std::string question, ReadBuffer & in, WriteBuffer & out, bool default_yes)
{
    while (true)
    {
        std::string answer;
        writeText(question, out);
        out.next();
        readStringUntilNewlineInto(answer, in);
        /// Checked before the newline itself is consumed below, so pressing Enter (an empty
        /// answer, but a completed line) is distinguishable from terminating the input.
        const bool input_ended = in.eof();
        skipToNextLineOrEOF(in);

        /// EOF (e.g. Ctrl+D) means the input was aborted, not answered: fail closed instead of
        /// acting on the default, like the `std::getline` overload below. Otherwise a prompt
        /// with `default_yes` would treat aborted input as an approval.
        if (answer.empty() && input_ended)
            return false;

        if (answer.empty())
            return default_yes;
        if (answer == "n" || answer == "N")
            return false;
        if (answer == "y" || answer == "Y")
            return true;
    }
}

bool ask(std::string question, bool default_yes)
{
    while (true)
    {
        std::string answer;
        std::cout << question;
        std::getline(std::cin, answer);
        if (!std::cin.good())
            return false;

        if (answer.empty())
            return default_yes;
        if (answer == "n" || answer == "N")
            return false;
        if (answer == "y" || answer == "Y")
            return true;
    }
}

}
