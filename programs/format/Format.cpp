#include <functional>
#include <iostream>
#include <string_view>
#include <Client/ClientBaseHelpers.h>
#include <boost/program_options.hpp>

#include <Core/Settings.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromOStream.h>
#include <IO/copyData.h>
#include <Interpreters/registerInterpreters.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/obfuscateQueries.h>
#include <Parsers/parseQuery.h>
#include <Common/ErrorCodes.h>
#include <Common/StringUtils.h>
#include <Common/TerminalSize.h>

#include <Interpreters/Context.h>
#include <Functions/FunctionFactory.h>
#include <Databases/registerDatabases.h>
#include <Functions/registerFunctions.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Storages/StorageFactory.h>
#include <Storages/registerStorages.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <DataTypes/DataTypeFactory.h>
#include <Formats/FormatFactory.h>
#include <Formats/registerFormats.h>
#include <Processors/Transforms/getSourceFromASTInsertQuery.h>

#include <boost/algorithm/string/split.hpp>

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_query_size;
}
}

namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace
{
}

#pragma clang diagnostic ignored "-Wunused-function"
#pragma clang diagnostic ignored "-Wmissing-declarations"

extern const char * auto_time_zones[];

int mainEntryClickHouseFormat(int argc, char ** argv)
{
    using namespace DB;

    try
    {
        boost::program_options::options_description desc = createOptionsDescription("Allowed options", getTerminalWidth());
        desc.add_options()
            ("query", po::value<std::string>(), "query to format")
            ("help,h", "produce help message")
            ("comments", "keep comments in the output")
            ("hilite,highlight", "add syntax highlight with ANSI terminal escape sequences (can also use --highlight)")
            ("oneline", "format in single line")
            ("max_line_length", po::value<size_t>()->default_value(0), "format in single line queries with length less than specified")
            ("quiet,q", "just check syntax, no output on success")
            ("multiquery,n", "allow multiple queries in the same file")
            ("obfuscate", "obfuscate instead of formatting")
            ("backslash", "add a backslash at the end of each line of the formatted query")
            ("allow_settings_after_format_in_insert", "allow SETTINGS after FORMAT, but note, that this is not always safe")
            ("seed", po::value<std::string>(), "seed (arbitrary string) that determines the result of obfuscation")
            ("show_secrets", po::bool_switch()->default_value(false), "show secret values like passwords, API keys, etc.")
            ("semicolons_inline", "In multiquery mode put semicolon on last line of query instead of a new line")
        ;

        Settings cmd_settings;
        cmd_settings.addToProgramOptions("max_parser_depth", desc);
        cmd_settings.addToProgramOptions("max_query_size", desc);

        boost::program_options::variables_map options;
        boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), options);
        po::notify(options);

        if (options.contains("help"))
        {
            std::cout << "Usage: " << argv[0] << " [options] < query" << std::endl;
            std::cout << desc << std::endl;
            return 1;
        }

        bool hilite = options.contains("hilite");
        bool oneline = options.contains("oneline");
        bool quiet = options.contains("quiet");
        bool multiple = options.contains("multiquery");
        size_t max_line_length = options["max_line_length"].as<size_t>();
        bool obfuscate = options.contains("obfuscate");
        bool backslash = options.contains("backslash");
        bool allow_settings_after_format_in_insert = options.contains("allow_settings_after_format_in_insert");
        bool show_secrets = options["show_secrets"].as<bool>();
        bool semicolon_inline = options.contains("semicolons_inline");

        std::function<void(std::string_view)> comments_callback;
        if (options.contains("comments"))
            comments_callback = [](const std::string_view comment) { std::cout << comment << '\n'; };

        SharedContextHolder shared_context = Context::createShared();
        auto context = Context::createGlobal(shared_context.get());
        auto context_const = WithContext(context).getContext();
        context->makeGlobalContext();

#if !USE_REPLXX
        if (hilite)
        {
            std::cerr << "Option 'hilite' is only available if ClickHouse is built with replxx library." << std::endl;
            return 2;
        }
#endif

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

        if (oneline && max_line_length)
        {
            std::cerr << "Options 'oneline' and 'max_line_length' are mutually exclusive." << std::endl;
            return 2;
        }

        if (max_line_length > 255)
        {
            std::cerr << "Option 'max_line_length' must be less than 256." << std::endl;
            return 2;
        }

        String query;

        if (options.contains("query"))
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

            if (options.contains("seed"))
            {
                hash_func.update(options["seed"].as<std::string>());
            }

            registerInterpreters();
            registerFunctions();
            registerAggregateFunctions();
            registerTableFunctions();
            registerDatabases();
            registerStorages();
            registerFormats();

            std::unordered_set<std::string> additional_names;

            auto all_known_storage_names = StorageFactory::instance().getAllRegisteredNames();
            auto all_known_data_type_names = DataTypeFactory::instance().getAllRegisteredNames();
            auto all_known_settings = Settings().getAllRegisteredNames();
            auto all_known_merge_tree_settings = MergeTreeSettings().getAllRegisteredNames();

            additional_names.insert(all_known_storage_names.begin(), all_known_storage_names.end());
            additional_names.insert(all_known_data_type_names.begin(), all_known_data_type_names.end());
            additional_names.insert(all_known_settings.begin(), all_known_settings.end());
            additional_names.insert(all_known_merge_tree_settings.begin(), all_known_merge_tree_settings.end());

            for (auto * it = auto_time_zones; *it; ++it)
            {
                String time_zone_name = *it;

                /// Example: Europe/Amsterdam
                Strings split;
                boost::split(split, time_zone_name, [](char c){ return c == '/'; });
                for (const auto & word : split)
                    if (!word.empty())
                        additional_names.insert(word);
            }

            /// Add lowercased versions of all additional names for case-insensitive matching.
            std::unordered_set<std::string> additional_names_lowercase;
            for (const auto & name : additional_names)
                additional_names_lowercase.insert(Poco::toLower(name));

            KnownIdentifierFunc is_known_identifier = [&](std::string_view name)
            {
                std::string what(name);

                if (FunctionFactory::instance().has(what)
                    || AggregateFunctionFactory::instance().isAggregateFunctionName(what)
                    || TableFunctionFactory::instance().isTableFunctionName(what)
                    || FormatFactory::instance().isOutputFormat(what)
                    || FormatFactory::instance().isInputFormat(what)
                    || additional_names.contains(what))
                    return true;

                /// Case-insensitive fallback for additional names (storage names, data types, settings, etc.)
                return additional_names_lowercase.contains(Poco::toLower(what));
            };

            WriteBufferFromFileDescriptor out(STDOUT_FILENO);
            obfuscateQueries(query, out, obfuscated_words_map, used_nouns, hash_func, is_known_identifier);
            out.finalize();
        }
        else
        {
            const char * pos = query.data();
            const char * end = pos + query.size();
            skipSpacesAndComments(pos, end, comments_callback);

            ParserQuery parser(end, allow_settings_after_format_in_insert);
            while (pos != end)
            {
                size_t approx_query_length = multiple ? find_first_symbols<';'>(pos, end) - pos : end - pos;

                ASTPtr res = parseQueryAndMovePosition(
                    parser,
                    pos,
                    end,
                    "query",
                    multiple,
                    cmd_settings[Setting::max_query_size],
                    cmd_settings[Setting::max_parser_depth],
                    cmd_settings[Setting::max_parser_backtracks]);

                std::unique_ptr<ReadBuffer> insert_query_payload;
                /// If the query is INSERT ... VALUES, then we will try to parse the data.
                if (auto * insert_query = res->as<ASTInsertQuery>(); insert_query && insert_query->data)
                {
                    if ("Values" != insert_query->format)
                        throw Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Can't format INSERT query with data format '{}'", insert_query->format);

                    /// Reset format to default to have `INSERT INTO table VALUES` instead of `INSERT INTO table VALUES FORMAT Values`
                    insert_query->format = {};

                    /// We assume that data ends with a newline character (same as client does)
                    const char * this_query_end = find_first_symbols<'\n'>(insert_query->data, end);
                    insert_query->end = this_query_end;
                    pos = this_query_end;
                    insert_query_payload = getReadBufferFromASTInsertQuery(res);
                }

                if (!quiet)
                {
                    if (!backslash)
                    {
                        WriteBufferFromOwnString str_buf;

                        WriteBufferFromOwnString query_buf;
                        bool oneline_current_query = oneline || approx_query_length < max_line_length;
                        IAST::FormatSettings settings(oneline_current_query);
                        settings.show_secrets = show_secrets;
                        settings.print_pretty_type_names = !oneline_current_query;
                        res->format(query_buf, settings);
                        String formatted_query = query_buf.str();
#if USE_REPLXX
                        if (hilite)
                            formatted_query = highlighted(formatted_query, *context);
#endif
                        str_buf.write(formatted_query.data(), formatted_query.size());

                        if (insert_query_payload)
                        {
                            str_buf.write(' ');
                            copyData(*insert_query_payload, str_buf);
                        }

                        String res_string = str_buf.str();
                        const char * s_pos = res_string.data();
                        const char * s_end = s_pos + res_string.size();
                        /// remove trailing spaces
                        while (s_end > s_pos && isWhitespaceASCIIOneLine(*(s_end - 1)))
                            --s_end;
                        WriteBufferFromOStream res_cout(std::cout, 4096);
                        /// For multiline queries we print ';' at new line,
                        /// but for single line queries we print ';' at the same line
                        bool has_multiple_lines = false;
                        while (s_pos != s_end)
                        {
                            if (*s_pos == '\n')
                                has_multiple_lines = true;
                            res_cout.write(*s_pos++);
                        }
                        res_cout.finalize();

                        if (multiple && !insert_query_payload)
                        {
                            if (oneline || !has_multiple_lines || semicolon_inline)
                                std::cout << ";\n";
                            else
                                std::cout << "\n;\n";
                        }
                        else if (multiple && insert_query_payload)
                            /// Do not need to add ; because it's already in the insert_query_payload
                            std::cout << "\n";

                        std::cout << std::endl;
                    }
                    /// add additional '\' at the end of each line;
                    else
                    {
                        WriteBufferFromOwnString str_buf;
                        bool oneline_current_query = oneline || approx_query_length < max_line_length;
                        IAST::FormatSettings settings(oneline_current_query);
                        settings.show_secrets = show_secrets;
                        settings.print_pretty_type_names = !oneline_current_query;
                        res->format(str_buf, settings);

                        String formatted_query = str_buf.str();
#if USE_REPLXX
                        if (hilite)
                            formatted_query = highlighted(formatted_query, *context);
#endif
                        WriteBufferFromOStream res_cout(std::cout, 4096);

                        const char * s_pos = formatted_query.data();
                        const char * s_end = s_pos + formatted_query.size();

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
                skipSpacesAndComments(pos, end, comments_callback);
                if (!multiple)
                    break;
            }
        }
    }
    catch (...)
    {
        std::cerr << getCurrentExceptionMessage(true) << '\n';
        return getCurrentExceptionCode();
    }
    return 0;
}
