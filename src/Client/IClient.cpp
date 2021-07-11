#include <Client/IClient.h>

#include <iostream>
#include <iomanip>

#if !defined(ARCADIA_BUILD)
#    include <Common/config_version.h>
#endif
#include <common/DateLUT.h>
#include <common/LocalDate.h>
#include <Parsers/Lexer.h>
#include <Common/UTF8Helpers.h>
#include <Common/TerminalSize.h>
#include <common/argsToConfig.h>
#include <Common/clearPasswordFromCommandLine.h>
#include <Common/filesystemHelpers.h>
#include <common/LineReader.h>
#include <Common/Config/configReadClient.h>
#include <filesystem>

namespace fs = std::filesystem;


namespace DB
{

namespace ErrorCodes
{
    extern const int UNRECOGNIZED_ARGUMENTS;
}


/// Should we celebrate a bit?
bool IClient::isNewYearMode()
{
    time_t current_time = time(nullptr);

    /// It's bad to be intrusive.
    if (current_time % 3 != 0)
        return false;

    LocalDate now(current_time);
    return (now.month() == 12 && now.day() >= 20) || (now.month() == 1 && now.day() <= 5);
}


bool IClient::isChineseNewYearMode(const String & local_tz)
{
    /// Days of Dec. 20 in Chinese calendar starting from year 2019 to year 2105
    static constexpr UInt16 chineseNewYearIndicators[]
        = {18275, 18659, 19014, 19368, 19752, 20107, 20491, 20845, 21199, 21583, 21937, 22292, 22676, 23030, 23414, 23768, 24122, 24506,
            24860, 25215, 25599, 25954, 26308, 26692, 27046, 27430, 27784, 28138, 28522, 28877, 29232, 29616, 29970, 30354, 30708, 31062,
            31446, 31800, 32155, 32539, 32894, 33248, 33632, 33986, 34369, 34724, 35078, 35462, 35817, 36171, 36555, 36909, 37293, 37647,
            38002, 38386, 38740, 39095, 39479, 39833, 40187, 40571, 40925, 41309, 41664, 42018, 42402, 42757, 43111, 43495, 43849, 44233,
            44587, 44942, 45326, 45680, 46035, 46418, 46772, 47126, 47510, 47865, 48249, 48604, 48958, 49342};

    /// All time zone names are acquired from https://www.iana.org/time-zones
    static constexpr const char * chineseNewYearTimeZoneIndicators[] = {
        /// Time zones celebrating Chinese new year.
        "Asia/Shanghai",
        "Asia/Chongqing",
        "Asia/Harbin",
        "Asia/Urumqi",
        "Asia/Hong_Kong",
        "Asia/Chungking",
        "Asia/Macao",
        "Asia/Macau",
        "Asia/Taipei",
        "Asia/Singapore",

        /// Time zones celebrating Chinese new year but with different festival names. Let's not print the message for now.
        // "Asia/Brunei",
        // "Asia/Ho_Chi_Minh",
        // "Asia/Hovd",
        // "Asia/Jakarta",
        // "Asia/Jayapura",
        // "Asia/Kashgar",
        // "Asia/Kuala_Lumpur",
        // "Asia/Kuching",
        // "Asia/Makassar",
        // "Asia/Pontianak",
        // "Asia/Pyongyang",
        // "Asia/Saigon",
        // "Asia/Seoul",
        // "Asia/Ujung_Pandang",
        // "Asia/Ulaanbaatar",
        // "Asia/Ulan_Bator",
    };
    static constexpr size_t M = sizeof(chineseNewYearTimeZoneIndicators) / sizeof(chineseNewYearTimeZoneIndicators[0]);

    time_t current_time = time(nullptr);

    if (chineseNewYearTimeZoneIndicators + M
        == std::find_if(chineseNewYearTimeZoneIndicators, chineseNewYearTimeZoneIndicators + M, [&local_tz](const char * tz)
                        {
                            return tz == local_tz;
                        }))
        return false;

    /// It's bad to be intrusive.
    if (current_time % 3 != 0)
        return false;

    auto days = DateLUT::instance().toDayNum(current_time).toUnderType();
    for (auto d : chineseNewYearIndicators)
    {
        /// Let's celebrate until Lantern Festival
        if (d <= days && d + 25 >= days)
            return true;
        else if (d > days)
            return false;
    }
    return false;
}


#if USE_REPLXX
void IClient::highlight(const String & query, std::vector<replxx::Replxx::Color> & colors)
{
    using namespace replxx;

    static const std::unordered_map<TokenType, Replxx::Color> token_to_color
        = {{TokenType::Whitespace, Replxx::Color::DEFAULT},
            {TokenType::Comment, Replxx::Color::GRAY},
            {TokenType::BareWord, Replxx::Color::DEFAULT},
            {TokenType::Number, Replxx::Color::GREEN},
            {TokenType::StringLiteral, Replxx::Color::CYAN},
            {TokenType::QuotedIdentifier, Replxx::Color::MAGENTA},
            {TokenType::OpeningRoundBracket, Replxx::Color::BROWN},
            {TokenType::ClosingRoundBracket, Replxx::Color::BROWN},
            {TokenType::OpeningSquareBracket, Replxx::Color::BROWN},
            {TokenType::ClosingSquareBracket, Replxx::Color::BROWN},
            {TokenType::DoubleColon, Replxx::Color::BROWN},
            {TokenType::OpeningCurlyBrace, Replxx::Color::INTENSE},
            {TokenType::ClosingCurlyBrace, Replxx::Color::INTENSE},

            {TokenType::Comma, Replxx::Color::INTENSE},
            {TokenType::Semicolon, Replxx::Color::INTENSE},
            {TokenType::Dot, Replxx::Color::INTENSE},
            {TokenType::Asterisk, Replxx::Color::INTENSE},
            {TokenType::Plus, Replxx::Color::INTENSE},
            {TokenType::Minus, Replxx::Color::INTENSE},
            {TokenType::Slash, Replxx::Color::INTENSE},
            {TokenType::Percent, Replxx::Color::INTENSE},
            {TokenType::Arrow, Replxx::Color::INTENSE},
            {TokenType::QuestionMark, Replxx::Color::INTENSE},
            {TokenType::Colon, Replxx::Color::INTENSE},
            {TokenType::Equals, Replxx::Color::INTENSE},
            {TokenType::NotEquals, Replxx::Color::INTENSE},
            {TokenType::Less, Replxx::Color::INTENSE},
            {TokenType::Greater, Replxx::Color::INTENSE},
            {TokenType::LessOrEquals, Replxx::Color::INTENSE},
            {TokenType::GreaterOrEquals, Replxx::Color::INTENSE},
            {TokenType::Concatenation, Replxx::Color::INTENSE},
            {TokenType::At, Replxx::Color::INTENSE},
            {TokenType::DoubleAt, Replxx::Color::MAGENTA},

            {TokenType::EndOfStream, Replxx::Color::DEFAULT},

            {TokenType::Error, Replxx::Color::RED},
            {TokenType::ErrorMultilineCommentIsNotClosed, Replxx::Color::RED},
            {TokenType::ErrorSingleQuoteIsNotClosed, Replxx::Color::RED},
            {TokenType::ErrorDoubleQuoteIsNotClosed, Replxx::Color::RED},
            {TokenType::ErrorSinglePipeMark, Replxx::Color::RED},
            {TokenType::ErrorWrongNumber, Replxx::Color::RED},
            { TokenType::ErrorMaxQuerySizeExceeded,
                Replxx::Color::RED }};

    const Replxx::Color unknown_token_color = Replxx::Color::RED;

    Lexer lexer(query.data(), query.data() + query.size());
    size_t pos = 0;

    for (Token token = lexer.nextToken(); !token.isEnd(); token = lexer.nextToken())
    {
        size_t utf8_len = UTF8::countCodePoints(reinterpret_cast<const UInt8 *>(token.begin), token.size());
        for (size_t code_point_index = 0; code_point_index < utf8_len; ++code_point_index)
        {
            if (token_to_color.find(token.type) != token_to_color.end())
                colors[pos + code_point_index] = token_to_color.at(token.type);
            else
                colors[pos + code_point_index] = unknown_token_color;
        }

        pos += utf8_len;
    }
}
#endif


void IClient::clearTerminal()
{
    /// Clear from cursor until end of screen.
    /// It is needed if garbage is left in terminal.
    /// Show cursor. It can be left hidden by invocation of previous programs.
    /// A test for this feature: perl -e 'print "x"x100000'; echo -ne '\033[0;0H\033[?25l'; clickhouse-client
    std::cout << "\033[0J"
                    "\033[?25h";
}


static void showClientVersion()
{
    std::cout << DBMS_NAME << " client version " << VERSION_STRING << VERSION_OFFICIAL << "." << std::endl;
}


void IClient::runInteractive()
{
    if (config().has("query_id"))
        throw Exception("query_id could be specified only in non-interactive mode", ErrorCodes::BAD_ARGUMENTS);
    if (print_time_to_stderr)
        throw Exception("time option could be specified only in non-interactive mode", ErrorCodes::BAD_ARGUMENTS);

    /// Initialize DateLUT here to avoid counting time spent here as query execution time.
    const auto local_tz = DateLUT::instance().getTimeZone();

    suggest.emplace();
    loadSuggestionDataIfPossible();

    if (home_path.empty())
    {
        const char * home_path_cstr = getenv("HOME");
        if (home_path_cstr)
            home_path = home_path_cstr;

        configReadClient(config(), home_path);
    }

    /// Load command history if present.
    if (config().has("history_file"))
        history_file = config().getString("history_file");
    else
    {
        auto * history_file_from_env = getenv("CLICKHOUSE_HISTORY_FILE");
        if (history_file_from_env)
            history_file = history_file_from_env;
        else if (!home_path.empty())
            history_file = home_path + "/.clickhouse-client-history";
    }

    if (!history_file.empty() && !fs::exists(history_file))
    {
        /// Avoid TOCTOU issue.
        try
        {
            FS::createFile(history_file);
        }
        catch (const ErrnoException & e)
        {
            if (e.getErrno() != EEXIST)
                throw;
        }
    }

    LineReader::Patterns query_extenders = {"\\"};
    LineReader::Patterns query_delimiters = {";", "\\G"};

#if USE_REPLXX
    replxx::Replxx::highlighter_callback_t highlight_callback{};
    if (config().getBool("highlight", true))
        highlight_callback = highlight;

    ReplxxLineReader lr(*suggest, history_file, config().has("multiline"), query_extenders, query_delimiters, highlight_callback);

#elif defined(USE_READLINE) && USE_READLINE
    ReadlineLineReader lr(*suggest, history_file, config().has("multiline"), query_extenders, query_delimiters);
#else
    LineReader lr(history_file, config().has("multiline"), query_extenders, query_delimiters);
#endif

    /// Enable bracketed-paste-mode only when multiquery is enabled and multiline is
    ///  disabled, so that we are able to paste and execute multiline queries in a whole
    ///  instead of erroring out, while be less intrusive.
    if (config().has("multiquery") && !config().has("multiline"))
        lr.enableBracketedPaste();

    do
    {
        auto input = lr.readLine(prompt(), ":-] ");
        if (input.empty())
            break;

        has_vertical_output_suffix = false;
        if (input.ends_with("\\G"))
        {
            input.resize(input.size() - 2);
            has_vertical_output_suffix = true;
        }

        if (!processQueryFromInteractive(input))
            break;
    }
    while (true);

    if (isNewYearMode())
        std::cout << "Happy new year." << std::endl;
    else if (isChineseNewYearMode(local_tz))
        std::cout << "Happy Chinese new year. 春节快乐!" << std::endl;
    else
        std::cout << "Bye." << std::endl;
}


int IClient::mainImpl()
{
    if (isInteractive())
        is_interactive = true;

    if (config().has("query") && !queries_files.empty())
        throw Exception("Specify either `query` or `queries-file` option", ErrorCodes::BAD_ARGUMENTS);

    std::cout << std::fixed << std::setprecision(3);
    std::cerr << std::fixed << std::setprecision(3);

    if (is_interactive)
    {

        clearTerminal();
        showClientVersion();
    }
    else
    {
        need_render_progress = config().getBool("progress", false);
        echo_queries = config().getBool("echo", false);
        ignore_error = config().getBool("ignore-error", false);
    }

    return childMainImpl();
}


int IClient::main(const std::vector<std::string> & /*args*/)
{
    try
    {
        return mainImpl();
    }
    catch (const Exception & e)
    {
        processMainImplException(e);

        /// If exception code isn't zero, we should return non-zero return code anyway.
        return e.code() ? e.code() : -1;
    }
    catch (...)
    {
        std::cerr << getCurrentExceptionMessage(false) << std::endl;
        return getCurrentExceptionCode();
    }
}


void IClient::init(int argc, char ** argv)
{
    namespace po = boost::program_options;

    /// Don't parse options with Poco library, we prefer neat boost::program_options.
    stopOptionsProcessing();

    Arguments common_arguments{""}; /// 0th argument is ignored.
    std::vector<Arguments> external_tables_arguments;
    readArguments(argc, argv, common_arguments, external_tables_arguments);

    stdin_is_a_tty = isatty(STDIN_FILENO);
    stdout_is_a_tty = isatty(STDOUT_FILENO);
    if (stdin_is_a_tty)
        terminal_width = getTerminalWidth();

    OptionsDescription options_description;
    addOptions(options_description);

    cmd_settings.addProgramOptions(options_description.main_description.value());

    /// Parse main commandline options.
    po::parsed_options parsed = po::command_line_parser(common_arguments).options(options_description.main_description.value()).run();

    auto unrecognized_options = po::collect_unrecognized(parsed.options, po::collect_unrecognized_mode::include_positional);
    /// unrecognized_options[0] is "", I don't understand why we need "" as the first argument which unused
    if (unrecognized_options.size() > 1)
        throw Exception(ErrorCodes::UNRECOGNIZED_ARGUMENTS, "Unrecognized option '{}'", unrecognized_options[1]);

    po::variables_map options;
    po::store(parsed, options);
    po::notify(options);

    if (options.count("version") || options.count("V"))
    {
        showClientVersion();
        exit(0);
    }

    if (options.count("version-clean"))
    {
        std::cout << VERSION_STRING;
        exit(0);
    }

    /// Output of help message.
    if (options.count("help")
        || (options.count("host") && options["host"].as<std::string>() == "elp")) /// If user writes -help instead of --help.
    {
        printHelpMessage(options_description);
        exit(0);
    }

    if (options.count("log-level"))
        Poco::Logger::root().setLevel(options["log-level"].as<std::string>());

    processOptions(options_description, options, external_tables_arguments);

    argsToConfig(common_arguments, config(), 100);
    if (supportPasswordOption())
        clearPasswordFromCommandLine(argc, argv);
}

}
