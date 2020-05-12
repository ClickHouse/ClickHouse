#include <iostream>

#include <boost/program_options.hpp>

#include <Functions/UserDefinedFunctions/UDFManager.h>
#include <Common/TerminalSize.h>
#include <Common/Exception.h>


int mainEntryClickHouseUDFManager(int argc, char ** argv)
{
    try
    {
        using namespace DB;
        namespace po = boost::program_options;

        po::options_description description = createOptionsDescription("Options", getTerminalWidth());
        description.add_options()("help", "produce help message")(
                                  "uid", po::value<unsigned>(), "Linux user id");

        po::parsed_options parsed = po::command_line_parser(argc, argv).options(description).run();
        po::variables_map options;
        po::store(parsed, options);

        if (options.count("help"))
        {
            std::cout << "Usage: " << argv[0] << " [options]\n";
            return 0;
        }

        if (options.count("uid"))
        {
            setuid(options["uid"].as<unsigned>());
        }

        return UDFManager().run();
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(true) << "\n";
        auto code = DB::getCurrentExceptionCode();
        return code ? code : 1;
    }
}
