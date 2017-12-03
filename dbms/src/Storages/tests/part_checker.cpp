#include <Poco/ConsoleChannel.h>
#include <Storages/MergeTree/checkDataPart.h>


int main(int argc, char ** argv)
{
    using namespace DB;

    Poco::AutoPtr<Poco::ConsoleChannel> channel = new Poco::ConsoleChannel(std::cerr);
    Logger::root().setChannel(channel);
    Logger::root().setLevel("trace");

    if (argc != 4)
    {
        std::cerr << "Usage: " << argv[0] << " path strict index_granularity" << std::endl;
        return 1;
    }

    try
    {
        checkDataPart(argv[1], parse<size_t>(argv[3]), parse<bool>(argv[2]), {});
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        throw;
    }

    return 0;
}
