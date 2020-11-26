#include <iostream>
#include <string_view>
#include <functional>
#include <boost/program_options.hpp>

#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromOStream.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/obfuscateQueries.h>
#include <Common/TerminalSize.h>

#include <Interpreters/Context.h>
#include <Functions/FunctionFactory.h>
#include <Functions/registerFunctions.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Storages/StorageFactory.h>
#include <Storages/registerStorages.h>
#include <DataTypes/DataTypeFactory.h>


#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wmissing-declarations"

int mainEntryClickHouseFormat(int argc, char ** argv)
{
    using namespace DB;

    boost::program_options::options_description desc = createOptionsDescription("Allowed options", getTerminalWidth());
    desc.add_options()
        ("help,h", "produce help message")
        ("hilite", "add syntax highlight with ANSI terminal escape sequences")
        ("oneline", "format in single line")
        ("quiet,q", "just check syntax, no output on success")
        ("multiquery,n", "allow multiple queries in the same file")
        ("obfuscate", "obfuscate instead of formatting")
        ("seed", po::value<std::string>(), "seed (arbitrary string) that determines the result of obfuscation")
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
        bool multiple = options.count("multiquery");
        bool obfuscate = options.count("obfuscate");

        if (quiet && (hilite || oneline || obfuscate))
        {
            std::cerr << "Options 'hilite' or 'oneline' or 'obfuscate' have no sense in 'quiet' mode." << std::endl;
            return 2;
        }

        if (obfuscate && (hilite || oneline || quiet))
        {
            std::cerr << "Options 'hilite' or 'oneline' or 'quiet' have no sense in 'obfuscate' mode." << std::endl;
            return 2;
        }

        String query;
        ReadBufferFromFileDescriptor in(STDIN_FILENO);
        readStringUntilEOF(query, in);

        if (obfuscate)
        {
            WordMap obfuscated_words_map;
            WordSet used_nouns;
            SipHash hash_func;

            if (options.count("seed"))
            {
                std::string seed;
                hash_func.update(options["seed"].as<std::string>());
            }

            SharedContextHolder shared_context = Context::createShared();
            Context context = Context::createGlobal(shared_context.get());
            context.makeGlobalContext();

            registerFunctions();
            registerAggregateFunctions();
            registerTableFunctions();
            registerStorages();

            std::unordered_set<std::string> additional_names;

            auto all_known_storage_names = StorageFactory::instance().getAllRegisteredNames();
            auto all_known_data_type_names = DataTypeFactory::instance().getAllRegisteredNames();

            additional_names.insert(all_known_storage_names.begin(), all_known_storage_names.end());
            additional_names.insert(all_known_data_type_names.begin(), all_known_data_type_names.end());

            KnownIdentifierFunc is_known_identifier = [&](std::string_view name)
            {
                std::string what(name);

                return FunctionFactory::instance().tryGet(what, context) != nullptr
                    || AggregateFunctionFactory::instance().isAggregateFunctionName(what)
                    || TableFunctionFactory::instance().isTableFunctionName(what)
                    || additional_names.count(what);
            };

            WriteBufferFromFileDescriptor out(STDOUT_FILENO);
            obfuscateQueries(query, out, obfuscated_words_map, used_nouns, hash_func, is_known_identifier);
        }
        else
        {
            const char * pos = query.data();
            const char * end = pos + query.size();

            ParserQuery parser(end);
            do
            {
                ASTPtr res = parseQueryAndMovePosition(parser, pos, end, "query", multiple, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
                if (!quiet)
                {
                    WriteBufferFromOStream res_buf(std::cout, 4096);
                    formatAST(*res, res_buf, hilite, oneline);
                    res_buf.next();
                    if (multiple)
                        std::cout << "\n;\n";
                    std::cout << std::endl;
                }
            } while (multiple && pos != end);
        }
    }
    catch (...)
    {
        std::cerr << getCurrentExceptionMessage(true);
        return getCurrentExceptionCode();
    }

    return 0;
}
