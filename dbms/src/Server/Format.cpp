#include <iostream>
#include <boost/program_options.hpp>

#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/formatAST.h>


int mainEntryClickHouseFormat(int argc, char ** argv)
{
    using namespace DB;

    boost::program_options::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message")
        ("hilite", "add syntax highlight with ANSI terminal escape sequences")
        ("oneline", "format in single line")
        ("quiet,q", "just check syntax, no output on success")
    ;

    boost::program_options::variables_map options;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), options);

    if (options.count("help"))
    {
        std::cout << "Usage: " << argv[0] << " [options] < query" << std::endl;
        std::cout << desc << std::endl;
        return 1;
    }

    try
    {
        bool hilite = options.count("hilite");
        bool oneline = options.count("oneline");
        bool quiet = options.count("quiet");

        if (quiet && (hilite || oneline))
        {
            std::cerr << "Options 'hilite' or 'oneline' have no sense in 'quiet' mode." << std::endl;
            return 2;
        }

        String query;
        ReadBufferFromFileDescriptor in(STDIN_FILENO);
        readStringUntilEOF(query, in);

        const char * pos = query.data();
        const char * end = pos + query.size();

        ParserQuery parser(end);
        ASTPtr res = parseQuery(parser, pos, end, "query", 0);

        if (!quiet)
        {
            formatAST(*res, std::cout, hilite, oneline);
            std::cout << std::endl;
        }
    }
    catch (...)
    {
        std::cerr << getCurrentExceptionMessage(true);
        return getCurrentExceptionCode();
    }

    return 0;
}
