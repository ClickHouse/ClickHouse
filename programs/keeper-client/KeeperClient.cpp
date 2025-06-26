#include "KeeperClient.h"
#include "Commands.h"
#include <Client/ReplxxLineReader.h>
#include <Client/ClientBase.h>
#include <Common/VersionNumber.h>
#include <Common/Config/ConfigProcessor.h>
#include <Client/ClientApplicationBase.h>
#include <Common/EventNotifier.h>
#include <Common/filesystemHelpers.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Parsers/parseQuery.h>
#include <Poco/Util/HelpFormatter.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

String KeeperClient::executeFourLetterCommand(const String & command)
{
    /// We need to create a new socket every time because ZooKeeper forcefully shuts down the connection after a four-letter-word command.
    Poco::Net::StreamSocket socket;
    socket.connect(Poco::Net::SocketAddress{zk_args.hosts[0]}, zk_args.connection_timeout_ms * 1000);

    socket.setReceiveTimeout(zk_args.operation_timeout_ms * 1000);
    socket.setSendTimeout(zk_args.operation_timeout_ms * 1000);
    socket.setNoDelay(true);

    ReadBufferFromPocoSocket in(socket);
    WriteBufferFromPocoSocket out(socket);

    out.write(command.data(), command.size());
    out.next();
    out.finalize();

    String result;
    readStringUntilEOF(result, in);
    in.next();
    return result;
}

std::vector<String> KeeperClient::getCompletions(const String & prefix) const
{
    Tokens tokens(prefix.data(), prefix.data() + prefix.size(), 0, false);
    IParser::Pos pos(tokens, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

    if (pos->type != TokenType::BareWord)
        return registered_commands_and_four_letter_words;

    ++pos;
    if (pos->isEnd())
        return registered_commands_and_four_letter_words;

    ParserToken{TokenType::Whitespace}.ignore(pos);

    std::vector<String> result;
    String string_path;
    Expected expected;
    if (!parseKeeperPath(pos, expected, string_path))
        string_path = cwd;

    if (!pos->isEnd())
        return result;

    fs::path path = string_path;
    String parent_path;
    if (string_path.ends_with("/"))
        parent_path = getAbsolutePath(string_path);
    else
        parent_path = getAbsolutePath(path.parent_path());

    try
    {
        for (const auto & child : zookeeper->getChildren(parent_path))
            result.push_back(child);
    }
    catch (Coordination::Exception &) {} // NOLINT(bugprone-empty-catch)

    std::sort(result.begin(), result.end());

    return result;
}

void KeeperClient::askConfirmation(const String & prompt, std::function<void()> && callback)
{
    if (!ask_confirmation)
    {
        callback();
        return;
    }

    std::cout << prompt << " Continue?\n";
    waiting_confirmation = true;
    confirmation_callback = callback;
}

fs::path KeeperClient::getAbsolutePath(const String & relative) const
{
    String result;
    if (relative.starts_with('/'))
        result = fs::weakly_canonical(relative);
    else
        result = fs::weakly_canonical(cwd / relative);

    if (result.ends_with('/') && result.size() > 1)
        result.pop_back();

    return result;
}

void KeeperClient::loadCommands(std::vector<Command> && new_commands)
{
    for (const auto & command : new_commands)
    {
        String name = command->getName();
        commands.insert({name, command});
        registered_commands_and_four_letter_words.push_back(std::move(name));
    }

    for (const auto & command : four_letter_word_commands)
        registered_commands_and_four_letter_words.push_back(command);

    std::sort(registered_commands_and_four_letter_words.begin(), registered_commands_and_four_letter_words.end());
}

void KeeperClient::defineOptions(Poco::Util::OptionSet & options)
{
    Poco::Util::Application::defineOptions(options);

    options.addOption(
        Poco::Util::Option("help", "", "show help and exit")
            .binding("help"));

    options.addOption(
        Poco::Util::Option("host", "h", "server hostname. default `localhost`")
            .argument("<host>")
            .binding("host"));

    options.addOption(
        Poco::Util::Option("port", "p", "server port. default `9181`")
            .argument("<port>")
            .binding("port"));

    options.addOption(
        Poco::Util::Option("password", "", "password to connect to keeper server")
            .argument("<password>")
            .binding("password"));

    options.addOption(
        Poco::Util::Option("query", "q", "will execute given query, then exit.")
            .argument("<query>")
            .binding("query"));

    options.addOption(
        Poco::Util::Option("connection-timeout", "", "set connection timeout in seconds. default 10s.")
            .argument("<seconds>")
            .binding("connection-timeout"));

    options.addOption(
        Poco::Util::Option("session-timeout", "", "set session timeout in seconds. default 10s.")
            .argument("<seconds>")
            .binding("session-timeout"));

    options.addOption(
        Poco::Util::Option("operation-timeout", "", "set operation timeout in seconds. default 10s.")
            .argument("<seconds>")
            .binding("operation-timeout"));

    options.addOption(
        Poco::Util::Option("use-xid-64", "", "use 64-bit XID. default false.")
            .binding("use-xid-64"));

    options.addOption(
        Poco::Util::Option("config-file", "c", "if set, will try to get a connection string from clickhouse config. default `config.xml`")
            .argument("<file>")
            .binding("config-file"));

    options.addOption(
        Poco::Util::Option("history-file", "", "set path of history file. default `~/.keeper-client-history`")
            .argument("<file>")
            .binding("history-file"));

    options.addOption(
        Poco::Util::Option("log-level", "", "set log level")
            .argument("<level>")
            .binding("log-level"));

    options.addOption(
        Poco::Util::Option("no-confirmation", "", "if set, will not require a confirmation on several commands. default false for interactive and true for query")
            .binding("no-confirmation"));

    options.addOption(
        Poco::Util::Option("tests-mode", "", "run keeper-client in a special mode for tests. all commands output are separated by special symbols. default false")
            .binding("tests-mode"));

    options.addOption(
        Poco::Util::Option("identity", "", "connect to Keeper using authentication with specified identity. default no identity")
            .argument("<identity>")
            .binding("identity"));
}

void KeeperClient::initialize(Poco::Util::Application & /* self */)
{
    suggest.setCompletionsCallback(
        [&](const String & prefix, size_t /* prefix_length */) { return getCompletions(prefix); });

    loadCommands({
        std::make_shared<LSCommand>(),
        std::make_shared<CDCommand>(),
        std::make_shared<SetCommand>(),
        std::make_shared<CreateCommand>(),
        std::make_shared<TouchCommand>(),
        std::make_shared<GetCommand>(),
        std::make_shared<ExistsCommand>(),
        std::make_shared<GetStatCommand>(),
        std::make_shared<FindSuperNodes>(),
        std::make_shared<DeleteStaleBackups>(),
        std::make_shared<FindBigFamily>(),
        std::make_shared<RMCommand>(),
        std::make_shared<RMRCommand>(),
        std::make_shared<ReconfigCommand>(),
        std::make_shared<SyncCommand>(),
        std::make_shared<HelpCommand>(),
        std::make_shared<FourLetterWordCommand>(),
        std::make_shared<GetDirectChildrenNumberCommand>(),
        std::make_shared<GetAllChildrenNumberCommand>(),
        std::make_shared<CPCommand>(),
        std::make_shared<MVCommand>(),
    });

    String home_path;
    const char * home_path_cstr = getenv("HOME"); // NOLINT(concurrency-mt-unsafe)
    if (home_path_cstr)
        home_path = home_path_cstr;

    if (config().has("history-file"))
        history_file = config().getString("history-file");
    else
        history_file = home_path + "/.keeper-client-history";

    if (!history_file.empty() && !fs::exists(history_file))
    {
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

    history_max_entries = config().getUInt("history-max-entries", 1000000);

    String default_log_level;
    if (config().has("query"))
        /// We don't want to see any information log in query mode, unless it was set explicitly
        default_log_level = "error";
    else
        default_log_level = "information";

    Poco::Logger::root().setLevel(config().getString("log-level", default_log_level));

    EventNotifier::init();
}

bool KeeperClient::processQueryText(const String & text)
{
    if (exit_strings.find(text) != exit_strings.end())
        return false;

    try
    {
        if (waiting_confirmation)
        {
            waiting_confirmation = false;
            if (text.size() == 1 && (text == "y" || text == "Y"))
                confirmation_callback();
            return true;
        }

        KeeperParser parser;
        const char * begin = text.data();
        const char * end = begin + text.size();

        while (begin < end)
        {
            String message;
            ASTPtr res = tryParseQuery(
                parser,
                begin,
                end,
                /* out_error_message = */ message,
                /* hilite = */ true,
                /* description = */ "",
                /* allow_multi_statements = */ true,
                /* max_query_size = */ 0,
                /* max_parser_depth = */ 0,
                /* max_parser_backtracks = */ 0,
                /* skip_insignificant = */ false);

            if (!res)
            {
                std::cerr << message << "\n";
                return true;
            }

            auto * query = res->as<ASTKeeperQuery>();

            auto command = KeeperClient::commands.find(query->command);
            command->second->execute(query, this);
        }
    }
    catch (Coordination::Exception & err)
    {
        std::cerr << err.message() << "\n";
    }
    return true;
}

void KeeperClient::runInteractiveReplxx()
{

    LineReader::Patterns query_extenders = {"\\"};
    LineReader::Patterns query_delimiters = {};
    char word_break_characters[] = " \t\v\f\a\b\r\n/";

    auto reader_options = ReplxxLineReader::Options
    {
        .suggest = suggest,
        .history_file_path = history_file,
        .history_max_entries = history_max_entries,
        .multiline = false,
        .ignore_shell_suspend = false,
        .extenders = query_extenders,
        .delimiters = query_delimiters,
        .word_break_characters = word_break_characters,
        .highlighter = {},
    };
    ReplxxLineReader lr(std::move(reader_options));
    lr.enableBracketedPaste();

    while (true)
    {
        String prompt;
        if (waiting_confirmation)
            prompt = "[y/n] ";
        else
            prompt = cwd.string() + " :) ";

        auto input = lr.readLine(prompt, ":-] ");
        if (input.empty())
            break;

        if (!processQueryText(input))
            break;
    }

    std::cout << std::endl;
}

void KeeperClient::runInteractiveInputStream()
{
    for (String input; std::getline(std::cin, input);)
    {
        if (!processQueryText(input))
            break;

        std::cout << "\a\a\a\a" << std::endl;
        std::cerr << std::flush;
    }
}

void KeeperClient::runInteractive()
{
    if (config().hasOption("tests-mode"))
        runInteractiveInputStream();
    else
        runInteractiveReplxx();
}

int KeeperClient::main(const std::vector<String> & /* args */)
{
    if (config().hasOption("help"))
    {
        Poco::Util::HelpFormatter help_formatter(KeeperClient::options());
        auto header_str = fmt::format("{} [OPTION]\n", commandName());
        help_formatter.setHeader(header_str);
        help_formatter.format(std::cout);
        return 0;
    }

    ConfigProcessor config_processor(config().getString("config-file", "config.xml"));

    /// This will handle a situation when clickhouse is running on the embedded config, but config.d folder is also present.
    ConfigProcessor::registerEmbeddedConfig("config.xml", "<clickhouse/>");
    auto clickhouse_config = config_processor.loadConfig();

    Poco::Util::AbstractConfiguration::Keys keys;
    clickhouse_config.configuration->keys("zookeeper", keys);

    if (!config().has("host") && !config().has("port") && !keys.empty())
    {
        LOG_INFO(getLogger("KeeperClient"), "Found keeper node in the config.xml, will use it for connection");

        for (const auto & key : keys)
        {
            if (key != "node")
                continue;

            String prefix = "zookeeper." + key;
            String host = clickhouse_config.configuration->getString(prefix + ".host");
            String port = clickhouse_config.configuration->getString(prefix + ".port");

            if (clickhouse_config.configuration->has(prefix + ".secure"))
                host = "secure://" + host;

            zk_args.hosts.push_back(host + ":" + port);
        }
    }
    else
    {
        String host = config().getString("host", "localhost");
        String port = config().getString("port", "9181");

        zk_args.hosts.push_back(host + ":" + port);
    }

    zk_args.availability_zones.resize(zk_args.hosts.size());
    zk_args.connection_timeout_ms = config().getInt("connection-timeout", 10) * 1000;
    zk_args.session_timeout_ms = config().getInt("session-timeout", 10) * 1000;
    zk_args.operation_timeout_ms = config().getInt("operation-timeout", 10) * 1000;
    zk_args.use_xid_64 = config().hasOption("use-xid-64");
    zk_args.password = config().getString("password", "");
    zk_args.identity = config().getString("identity", "");
    if (!zk_args.identity.empty())
        zk_args.auth_scheme = "digest";
    zookeeper = zkutil::ZooKeeper::createWithoutKillingPreviousSessions(zk_args);

    if (config().has("no-confirmation") || config().has("query"))
        ask_confirmation = false;

    if (config().has("query"))
    {
        processQueryText(config().getString("query"));
    }
    else
        runInteractive();

    /// Suppress "Finalizing session {}" message.
    getLogger("ZooKeeperClient")->setLevel("error");
    zookeeper.reset();

    return 0;
}

}


int mainEntryClickHouseKeeperClient(int argc, char ** argv)
{
    try
    {
        DB::KeeperClient client;
        client.init(argc, argv);
        return client.run();
    }
    catch (DB::Exception & e)
    {
        std::cerr << DB::getExceptionMessageForLogging(e, false) << std::endl;
        auto code = DB::getCurrentExceptionCode();
        return static_cast<UInt8>(code) ? code : 1;
    }
    catch (const boost::program_options::error & e)
    {
        std::cerr << "Bad arguments: " << e.what() << std::endl;
        return DB::ErrorCodes::BAD_ARGUMENTS;
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(true) << std::endl;
        auto code = DB::getCurrentExceptionCode();
        return static_cast<UInt8>(code) ? code : 1;
    }
}
