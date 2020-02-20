#include <iostream>
#include <common/DateLUT.h>
#include <Poco/Exception.h>

int main(int argc, char ** argv)
{
    try
    {
        const auto & date_lut = DateLUT::instance();
        std::cout << "Detected default timezone: `" << date_lut.getTimeZone() << "'" << std::endl;
        time_t now = time(NULL);
        std::cout << "Current time: " << date_lut.timeToString(now)
                  << ", UTC: " << DateLUT::instance("UTC").timeToString(now) << std::endl;
    }
    catch (const Poco::Exception & e)
    {
        std::cerr << e.displayText() << std::endl;
        return 1;
    }
    catch (std::exception & e)
    {
        std::cerr << "std::exception: " << e.what() << std::endl;
        return 2;
    }
    catch (...)
    {
        std::cerr << "Some exception" << std::endl;
        return 3;
    }
    return 0;
}
