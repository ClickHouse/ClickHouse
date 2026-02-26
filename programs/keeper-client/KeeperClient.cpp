#include <KeeperClient.h>
#include <Client/ReplxxLineReader.h>
#include <Client/ClientBase.h>
#include <Common/VersionNumber.h>
#include <Common/Config/ConfigProcessor.h>
#include <Client/ClientApplicationBase.h>
#include <Common/EventNotifier.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/ZooKeeperArgs.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/filesystemHelpers.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ErrnoException.h>
#include <Parsers/parseQuery.h>
#include <Poco/Util/HelpFormatter.h>

#if USE_SSL
#include <Poco/Net/Context.h>
#include <Poco/Net/SSLManager.h>
#include <Poco/Net/AcceptCertificateHandler.h>
#include <Poco/Net/RejectCertificateHandler.h>
#endif


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

    options.addOption(
        Poco::Util::Option("secure", "s", "use secure connection (adds secure:// prefix to host). default false")
            .binding("secure"));

    options.addOption(
        Poco::Util::Option("tls-cert-file", "", "path to TLS certificate file for secure connection")
            .argument("<file>")
            .binding("tls-cert-file"));

    options.addOption(
        Poco::Util::Option("tls-key-file", "", "path to TLS private key file for secure connection")
            .argument("<file>")
            .binding("tls-key-file"));

    options.addOption(
        Poco::Util::Option("tls-ca-file", "", "path to TLS CA certificate file for secure connection")
            .argument("<file>")
            .binding("tls-ca-file"));

    options.addOption(
        Poco::Util::Option("accept-invalid-certificate", "", "accept invalid TLS certificates (bypasses verification). default false")
            .binding("accept-invalid-certificate"));
}

void KeeperClient::initialize(Poco::Util::Application & /* self */)
{
    suggest.setCompletionsCallback(
        [&](const String & prefix, size_t /* prefix_length */) { return getCompletions(prefix); });

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

bool KeeperClient::processQueryText(const String & text, bool is_interactive)
{
    if (exit_strings.contains(text))
        return false;

    static constexpr size_t total_retries = 10;
    std::chrono::milliseconds current_sleep{100};
    size_t i = 0;

    auto component_guard = Coordination::setCurrentComponent("KeeperClient::processQueryText");
    while (true)
    {
        try
        {
            if (i > 0)
                connectToKeeper();

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

            break;
        }
        catch (Coordination::Exception & err)
        {
            std::cerr << err.message() << "\n";

            if (!is_interactive || !Coordination::isHardwareError(err.code))
                break;

            if (i == total_retries)
            {
                std::cerr << "Failed to connect to Keeper" << std::endl;
                break;
            }

            ++i;
            current_sleep = std::min<std::chrono::milliseconds>(current_sleep * 2, std::chrono::milliseconds{5000});
            std::cerr << fmt::format("Will try to reconnect after {}ms ({}/{})", current_sleep.count(), i, total_retries) << std::endl;
            std::this_thread::sleep_for(current_sleep);
        }
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

        if (!processQueryText(input, /*is_interactive=*/true))
            break;
    }

    cout << std::endl;
}

/// In tests-mode, commands are read line by line from stdin.
/// After each command, a separator (four BEL characters + newline) is written
/// to stdout so the test harness can detect where one command's output ends.
/// Errors from failed commands go to stderr.
/// We must flush stderr before writing the separator to stdout, otherwise
/// the test harness may see the separator first and miss the error.
void KeeperClient::runInteractiveInputStream()
{
    for (String input; std::getline(std::cin, input);)
    {
        if (!processQueryText(input, /*is_interactive=*/true))
            break;

        cerr << std::flush;
        cout << "\a\a\a\a" << std::endl;
    }
}

void KeeperClient::runInteractive()
{
    if (config().hasOption("tests-mode"))
        runInteractiveInputStream();
    else
        runInteractiveReplxx();
}

void KeeperClient::connectToKeeper()
{
#if USE_SSL
    /// Configure SSL context if TLS options are provided
    if (config().has("tls-cert-file") || config().has("tls-key-file") || config().has("tls-ca-file") || config().has("accept-invalid-certificate"))
    {
        Poco::Net::Context::VerificationMode verification_mode = Poco::Net::Context::VERIFY_RELAXED;

        if (config().has("accept-invalid-certificate"))
        {
            verification_mode = Poco::Net::Context::VERIFY_NONE;
        }

        auto context = Poco::Net::Context::Ptr(new Poco::Net::Context(
            Poco::Net::Context::TLSV1_2_CLIENT_USE,
            config().getString("tls-key-file", ""),
            config().getString("tls-cert-file", ""),
            config().getString("tls-ca-file", ""),
            verification_mode,
            9,
            true,
            "ALL:!ADH:!LOW:!EXP:!MD5:@STRENGTH"
        ));

        Poco::Net::SSLManager::InvalidCertificateHandlerPtr certificate_handler;
        if (config().has("accept-invalid-certificate"))
        {
            certificate_handler = new Poco::Net::AcceptCertificateHandler(false);
        }
        else
        {
            certificate_handler = new Poco::Net::RejectCertificateHandler(false);
        }

        Poco::Net::SSLManager::instance().initializeClient(nullptr, certificate_handler, context);
    }
#endif

    ConfigProcessor config_processor(config().getString("config-file", "config.xml"));

    /// This will handle a situation when clickhouse is running on the embedded config, but config.d folder is also present.
    ConfigProcessor::registerEmbeddedConfig("config.xml", "<clickhouse/>");
    auto clickhouse_config = config_processor.loadConfig();

    Poco::Util::AbstractConfiguration::Keys keys;
    clickhouse_config.configuration->keys("zookeeper", keys);

    zkutil::ZooKeeperArgs new_zk_args;

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

            if (clickhouse_config.configuration->has(prefix + ".secure") || config().has("secure"))
                host = "secure://" + host;

            new_zk_args.hosts.push_back(host + ":" + port);
        }
    }
    else
    {
        String host = config().getString("host", "localhost");
        String port = config().getString("port", "9181");

        if (config().has("secure"))
            host = "secure://" + host;

        new_zk_args.hosts.push_back(host + ":" + port);
    }

    new_zk_args.availability_zones.resize(new_zk_args.hosts.size());
    new_zk_args.connection_timeout_ms = config().getInt("connection-timeout", 10) * 1000;
    new_zk_args.session_timeout_ms = config().getInt("session-timeout", 10) * 1000;
    new_zk_args.operation_timeout_ms = config().getInt("operation-timeout", 10) * 1000;
    new_zk_args.use_xid_64 = config().hasOption("use-xid-64");
    new_zk_args.password = config().getString("password", "");
    new_zk_args.identity = config().getString("identity", "");
    if (!new_zk_args.identity.empty())
        new_zk_args.auth_scheme = "digest";
    zk_args = new_zk_args;
    auto component_guard = Coordination::setCurrentComponent("KeeperClient::connectToKeeper");
    zookeeper = zkutil::ZooKeeper::createWithoutKillingPreviousSessions(std::move(new_zk_args));
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

    connectToKeeper();

    if (config().has("no-confirmation") || config().has("query"))
        ask_confirmation = false;

    if (config().has("query"))
    {
        processQueryText(config().getString("query"), /*is_interactive=*/false);
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
