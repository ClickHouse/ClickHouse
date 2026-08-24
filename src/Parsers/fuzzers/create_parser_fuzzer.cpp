#include <iostream>
#include <string>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>

/// This fuzzer supports its own arguments:
/// - `-max_parser_depth=N` sets the maximum parser depth (default: 150 for debug/sanitizer, 300 otherwise)
/// - `-max_parser_backtracks=N` sets the maximum parser backtracks (default: DBMS_DEFAULT_MAX_PARSER_BACKTRACKS)
/// - `-max_ast_depth=N` sets the maximum depth of the AST tree (default: 1000)
/// - `-max_ast_elements=N` sets the maximum number of elements in AST tree (default: 50000)
/// These arguments must be passed after `-ignore_remaining_args=1` to avoid interference with libFuzzer options.
using namespace DB;


#if defined(SANITIZER) || !defined(NDEBUG)
    size_t max_parser_depth = 150;
#else
    size_t max_parser_depth = 300;
#endif
size_t max_parser_backtracks = DBMS_DEFAULT_MAX_PARSER_BACKTRACKS;
size_t max_ast_depth = 1000;
size_t max_ast_elements = 50000;


// Helper function to check if this is a merge run
bool isMerge(int argc, char ** argv)
{
    for (int i = 1; i < argc; ++i)
    {
        std::string_view arg{argv[i]};
        if (std::string_view{arg.begin(), std::ranges::find(arg, '=')} == "-ignore_remaining_args")
            break;
        if (std::string_view{arg.begin(), std::ranges::find(arg, '=')} == "-merge")
            return true;
    }
    return false;
}

// Helper function to parse settings from command line arguments
std::unordered_map<std::string, std::string> parseSettingsFromArgs(int argc, char ** argv) // STYLE_CHECK_ALLOW_STD_CONTAINERS
{
    std::unordered_map<std::string, std::string> settings; // STYLE_CHECK_ALLOW_STD_CONTAINERS
    bool ignore_remaining = false;

    for (int i = 1; i < argc; ++i)
    {
        std::string arg{argv[i]};

        if (!ignore_remaining)
        {
            // Check for -ignore_remaining_args
            if (arg.starts_with("-ignore_remaining_args"))
            {
                ignore_remaining = true;
                continue;
            }
        }
        else
        {
            // Parse settings after -ignore_remaining_args
            size_t eq_pos = arg.find('=');
            if (eq_pos != std::string::npos)
            {
                // Skip leading dashes to get the setting name
                size_t key_start = 0;
                while (key_start < arg.length() && arg[key_start] == '-')
                    ++key_start;

                std::string key = arg.substr(key_start, eq_pos - key_start);
                std::string value = arg.substr(eq_pos + 1);
                settings[key] = value;
            }
        }
    }

    return settings;
}

extern "C" int LLVMFuzzerInitialize(const int *argc, char ***argv)
{
    // Check if this is a merge run and skip initialization if so
    if (isMerge(*argc, *argv))
        return 0;

    auto settings = parseSettingsFromArgs(*argc, *argv);
    auto parse_setting = [&settings](const std::string & key, size_t & out)
    {
        if (auto it = settings.find(key); it != settings.end())
        {
            try
            {
                size_t pos = 0;
                uint64_t val = std::stoul(it->second, &pos);
                if (pos != it->second.size())
                {
                    std::cerr << "Invalid value for " << key << ": " << it->second << std::endl;
                    exit(1);
                }
                out = static_cast<size_t>(val);
            }
            catch (const std::exception & e)
            {
                std::cerr << "Invalid value for " << key << ": " << it->second << " (" << e.what() << ")" << std::endl;
                exit(1);
            }
        }
    };

    parse_setting("max_parser_depth", max_parser_depth);
    parse_setting("max_parser_backtracks", max_parser_backtracks);
    parse_setting("max_ast_depth", max_ast_depth);
    parse_setting("max_ast_elements", max_ast_elements);

    return 0;
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
{
    try
    {
        std::string input = std::string(reinterpret_cast<const char*>(data), size);

        DB::ParserCreateQuery parser;

        DB::ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, max_parser_depth, max_parser_backtracks);

        ast->checkDepth(max_ast_depth);
        ast->checkSize(max_ast_elements);

        std::cerr << ast->formatWithSecretsOneLine() << std::endl;
    }
    catch (...)
    {
        // Ok
    }

    return 0;
}
