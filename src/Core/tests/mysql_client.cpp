#include <iostream>
#include <Core/MySQLClient.h>


int main(int, char **)
{
    using namespace DB;

    UInt16 port = 9001;
    String host = "127.0.0.1", user = "default", password = "123";
    MySQLClient client(host, port, user, password, "");
    if (!client.connect())
    {
        std::cerr << "Connect Error: " << client.error() << std::endl;
    }
    return 0;
}
