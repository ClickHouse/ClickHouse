
#include <iostream>
#include <snappy.h>

// Basic snappy uncompression, used for testing snappy compression requests
//
int mainEntryClickHouseSimpleSnappy(int, char **)
{
    std::string line;
    std::getline(std::cin, line);

    std::string output;

    snappy::Uncompress(line.data(), line.size(), &output);

    char * out_data = output.data();
    for (size_t i = 0 ; i < output.size(); ++i)
    {
        /// Break when we reached the end of the uncompressed buffer, which is marked by the a 0 char
        if (out_data[i] == '\0')
        {
            std::cout << std::endl;
            break;
        }
        std::cout << out_data[i];
    }

    return 0;
}
