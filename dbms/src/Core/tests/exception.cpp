#include <iostream>

#include <Poco/Net/NetException.h>

#include <Common/Exception.h>


int main(int, char **)
{
    try
    {
        //throw Poco::Net::ConnectionRefusedException();
        throw DB::Exception(Poco::Net::ConnectionRefusedException());
    }
    catch (const DB::Exception & e)
    {
        std::cerr << e.displayText() << std::endl;
    }
    catch (const Poco::Exception & e)
    {
        std::cerr << e.displayText() << std::endl;
    }

    return 0;
}
