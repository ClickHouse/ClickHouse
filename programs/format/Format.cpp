#include <functional>
#include <iostream>
#include <string_view>
#include <boost/program_options.hpp>

#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromOStream.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/obfuscateQueries.h>
#include <Parsers/parseQuery.h>
#include <Common/ErrorCodes.h>
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
#include <Formats/FormatFactory.h>
#include <Formats/registerFormats.h>


#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wmissing-declarations"

namespace DB
{
namespace ErrorCodes
{
extern const int INVALID_FORMAT_INSERT_QUERY_WITH_DATA;
}
}

int mainEntryClickHouseFormat(int argc, char ** argv)
{
    using namespace DB;

    try
    {
        boost::program_options::options_description desc = createOptionsDescription("Allowed options", getTerminalWidth());
        desc.add_options()
            ("query", po::value<std::string>(), "query to format")
            ("help,h", "produce help message")
            ("hilite", "add syntax highlight with ANSI terminal escape sequences")
            ("oneline", "format in single line")
            ("quiet,q", "just check syntax, no output on success")
            ("multiquery,n", "allow multiple queries in the same file")
            ("obfuscate", "obfuscate instead of formatting")
            ("backslash", "add a backslash at the end of each line of the formatted query")
            ("allow_settings_after_format_in_insert", "Allow SETTINGS after FORMAT, but note, that this is not always safe")
            ("seed", po::value<std::string>(), "seed (arbitrary string) that determines the result of obfuscation")
        ;

        Settings cmd_settings;
        for (const auto & field : cmd_settings.all())
        {
            if (field.getName() == "max_parser_depth" || field.getName() == "max_query_size")
                cmd_settings.addProgramOption(desc, field);
        }

        boost::program_options::variables_map options;
        boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), options);
        po::notify(options);

        if (options.count("help"))
        {
            std::cout << "Usage: " << argv[0] << " [options] < query" << std::endl;
            std::cout << desc << std::endl;
            return 1;
        }

        bool hilite = options.count("hilite");
        bool oneline = options.count("oneline");
        bool quiet = options.count("quiet");
        bool multiple = options.count("multiquery");
        bool obfuscate = options.count("obfuscate");
        bool backslash = options.count("backslash");
        bool allow_settings_after_format_in_insert = options.count("allow_settings_after_format_in_insert");

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

        if (options.count("query"))
        {
            query = options["query"].as<std::string>();
        }
        else
        {
            ReadBufferFromFileDescriptor in(STDIN_FILENO);
            readStringUntilEOF(query, in);
        }

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

            registerFunctions();
            registerAggregateFunctions();
            registerTableFunctions();
            registerStorages();
            registerFormats();

            std::unordered_set<std::string> additional_names;

            auto all_known_storage_names = StorageFactory::instance().getAllRegisteredNames();
            auto all_known_data_type_names = DataTypeFactory::instance().getAllRegisteredNames();

            additional_names.insert(all_known_storage_names.begin(), all_known_storage_names.end());
            additional_names.insert(all_known_data_type_names.begin(), all_known_data_type_names.end());

            KnownIdentifierFunc is_known_identifier = [&](std::string_view name)
            {
                std::string what(name);

                return FunctionFactory::instance().has(what)
                    || AggregateFunctionFactory::instance().isAggregateFunctionName(what)
                    || TableFunctionFactory::instance().isTableFunctionName(what)
                    || FormatFactory::instance().isOutputFormat(what)
                    || FormatFactory::instance().isInputFormat(what)
                    || additional_names.contains(what);
            };

            WriteBufferFromFileDescriptor out(STDOUT_FILENO);
            obfuscateQueries(query, out, obfuscated_words_map, used_nouns, hash_func, is_known_identifier);
            out.finalize();
        }
        else
        {
            const char * pos = query.data();
            const char * end = pos + query.size();

            ParserQuery parser(end, allow_settings_after_format_in_insert);
            do
            {
                ASTPtr res = parseQueryAndMovePosition(
                    parser, pos, end, "query", multiple, cmd_settings.max_query_size, cmd_settings.max_parser_depth);
                /// For insert query with data(INSERT INTO ... VALUES ...), will lead to format fail,
                /// should throw exception early and make exception message more readable.
                if (const auto * insert_query = res->as<ASTInsertQuery>(); insert_query && insert_query->data)
                {
                    throw Exception(
                        "Can't format ASTInsertQuery with data, since data will be lost",
                        DB::ErrorCodes::INVALID_FORMAT_INSERT_QUERY_WITH_DATA);
                }
                if (!quiet)
                {
                    if (!backslash)
                    {
                        WriteBufferFromOStream res_buf(std::cout, 4096);
                        formatAST(*res, res_buf, hilite, oneline);
                        res_buf.finalize();
                        if (multiple)
                            std::cout << "\n;\n";
                        std::cout << std::endl;
                    }
                    /// add additional '\' at the end of each line;
                    else
                    {
                        WriteBufferFromOwnString str_buf;
                        formatAST(*res, str_buf, hilite, oneline);

                        auto res_string = str_buf.str();
                        WriteBufferFromOStream res_cout(std::cout, 4096);

                        const char * s_pos= res_string.data();
                        const char * s_end = s_pos + res_string.size();

                        while (s_pos != s_end)
                        {
                            if (*s_pos == '\n')
                                res_cout.write(" \\", 2);
                            res_cout.write(*s_pos++);
                        }

                        res_cout.finalize();
                        if (multiple)
                            std::cout << " \\\n;\n";
                        std::cout << std::endl;
                    }
                }

                do
                {
                    /// skip spaces to avoid throw exception after last query
                    while (pos != end && std::isspace(*pos))
                        ++pos;

                    /// for skip comment after the last query and to not throw exception
                    if (end - pos > 2 && *pos == '-' && *(pos + 1) == '-')
                    {
                        pos += 2;
                        /// skip until the end of the line
                        while (pos != end && *pos != '\n')
                            ++pos;
                    }
                    /// need to parse next sql
                    else
                        break;
                } while (pos != end);

            } while (multiple && pos != end);
        }
    }
    catch (...)
    {
        std::cerr << getCurrentExceptionMessage(true) << '\n';
        return getCurrentExceptionCode();
    }
    return 0;
}
