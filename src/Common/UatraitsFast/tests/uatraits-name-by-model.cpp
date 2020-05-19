#include <iostream>

#include <Poco/String.h>

#include <metrika/core/libs/uatraits-fast/uatraits-fast.h>


int main(int /*argc*/, char ** argv)
{
    try
    {
        UATraits uatraits("browser.xml", "profiles.xml", "extra.xml");

        std::cerr << uatraits.getNameByModel(argv[1]) << std::endl;
    }
    catch (const Poco::Exception & e)
    {
        std::cerr << e.displayText() << std::endl;
    }

    return 0;
}
