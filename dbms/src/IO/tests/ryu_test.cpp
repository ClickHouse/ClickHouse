#include <string>
#include <iostream>
#include <ryu/ryu.h>


int main(int argc, char ** argv)
{
    double x = argc > 1 ? std::stod(argv[1]) : 0;
    char buf[32];

    d2s_buffered(x, buf);
    std::cout << buf << "\n";

    return 0;
}
