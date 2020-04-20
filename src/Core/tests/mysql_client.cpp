#include <Core/MySQLClient.h>


int main(int, char **)
{
    using namespace DB;

    UInt16 port = 4407;
    String host = "127.0.0.1", user = "mock", password = "mock";
    MySQLClient client(host, port, user, password, "");
    client.connect();
    return 0;
}
