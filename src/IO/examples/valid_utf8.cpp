#include <IO/WriteBufferValidUTF8.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <string>
#include <iostream>

int main(int, char **)
{
    try
    {
        std::string test1 = R"(kjhsgdfkjhg2378rtzgvxkz877%^&^*%&^*&*)";
        std::string test2 = R"({"asd" = "qw1","qwe24" = "3asd"})";
        test2[test2.find('1')] = char(127 + 64);
        test2[test2.find('2')] = char(127 + 64 + 32);
        test2[test2.find('3')] = char(127 + 64 + 32 + 16);
        test2[test2.find('4')] = char(127 + 64 + 32 + 16 + 8);

        std::string str;
        {
            DB::WriteBufferFromString str_buf(str);
            {
                DB::WriteBufferValidUTF8 utf_buf(str_buf, true, "-");
                DB::writeEscapedString(test1, utf_buf);
            }
        }
        std::cout << str << std::endl;

        str = "";
        {
            DB::WriteBufferFromString str_buf(str);
            {
                DB::WriteBufferValidUTF8 utf_buf(str_buf, true, "-");
                DB::writeEscapedString(test2, utf_buf);
            }
        }
        std::cout << str << std::endl;
    }
    catch (const DB::Exception & e)
    {
        std::cerr << e.what() << ", " << e.displayText() << std::endl;
        return 1;
    }

    return 0;
}
