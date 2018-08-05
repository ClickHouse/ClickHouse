#include <iostream>
#include <Common/Exception.h>
#include <Dictionaries/validateODBCConnectionString.h>


using namespace DB;

int main(int argc, char ** argv)
try
{
    if (argc < 2)
    {
        std::cerr << "Usage: validate-odbc-connection-string 'ConnectionString'\n";
        return 1;
    }

    std::cout << validateODBCConnectionString(argv[1]) << '\n';
    return 0;
}
catch (...)
{
    std::cerr << getCurrentExceptionMessage(false) << "\n";
    return 2;
}
