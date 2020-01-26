/**
 *  Address.cpp
 * 
 *  Test program to parse address strings
 * 
 *  @author Emiel Bruijntjes <emiel.bruijntjes@copernica.com>
 *  @copyright 2017 Copernica BV
 */

/**
 *  Dependencies
 */
#include <amqpcpp.h>

/**
 *  Main procedure
 *  @param  argc
 *  @param  argv
 *  @return int
 */
/*
int main(int argc, const char *argv[])
{
    // iterate over the arguments
    for (int i = 1; i < argc; ++i)
    {
        // parse address
        AMQP::Address address(argv[i]);
        
        // output all properties
        std::cout << "user: " << address.login().user() << std::endl;
        std::cout << "password: " << address.login().password() << std::endl;
        std::cout << "hostname: " << address.hostname() << std::endl;
        std::cout << "port: " << address.port() << std::endl;
        std::cout << "vhost: " << address.vhost() << std::endl;
    }

    // done
    return 0;
}
*/