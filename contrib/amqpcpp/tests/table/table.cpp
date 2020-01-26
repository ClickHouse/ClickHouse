#include <iostream>
#include <amqpcpp.h>


int main()
{
    AMQP::Array x;
    
    x[0] = "abc";
    x[1] = "xyz";
    
    std::cout << x << std::endl;
    std::cout << x[0] << std::endl;
    std::cout << x[1] << std::endl;
    

    return 0;
}

