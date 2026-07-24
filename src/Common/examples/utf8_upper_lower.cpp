#include <iostream>
#include <unicode/unistr.h>

std::string utf8_to_lower(const std::string & input)
{
    icu::UnicodeString unicodeInput(input.c_str(), "UTF-8");
    unicodeInput.toLower();
    std::string output;
    unicodeInput.toUTF8String(output);
    return output;
}

std::string utf8_to_upper(const std::string & input)
{
    icu::UnicodeString unicodeInput(input.c_str(), "UTF-8");
    unicodeInput.toUpper();
    std::string output;
    unicodeInput.toUTF8String(output);
    return output;
}

int main()
{
    std::string input = "ır";
    std::cout << "upper:" << utf8_to_upper(input) << std::endl;
    return 0;
}
