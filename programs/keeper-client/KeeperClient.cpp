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
#include <Parsers/TokenIterator.h>
#include <Poco/Util/HelpFormatter.h>

#if USE_SSL
#include <Poco/Net/Context.h>
#include <Poco/Net/SSLManager.h>
#include <Poco/Net/AcceptCertificateHandler.h>
#include <Poco/Net/RejectCertificateHandler.h>
#endif

#include <algorithm>
#include <string_view>


namespace
{

char WORD_BREAK_CHARACTERS[] = " \t\v\f\a\b\r\n";

/// Unescape a bare (unquoted) path that may contain backslash escaping
/// and inline quoted segments, mirroring what parseKeeperArg accepts:
///   /foo\ bar        →  /foo bar       (backslash-escaped space)
///   /foo\\bar        →  /foo\bar       (escaped backslash)
///   /'has"quote'/d   →  /has"quote/d   (inline single-quoted segment)
///   /'it\'s'/d       →  /it's/d        (escaped quote inside single quotes)
String unescapePath(const String & s)
{
    String result;
    result.reserve(s.size());
    char in_quote = 0;
    for (size_t i = 0; i < s.size(); ++i)
    {
        char c = s[i];
        if (in_quote)
        {
            if (c == '\\' && i + 1 < s.size()
                && (s[i + 1] == in_quote || s[i + 1] == '\\'))
            {
                result += s[i + 1];
                ++i;
            }
            else if (c == in_quote)
            {
                in_quote = 0;
            }
            else
            {
                result += c;
            }
        }
        else if (c == '\'' || c == '"')
        {
            in_quote = c;
        }
        else if (c == '\\' && i + 1 < s.size()
            && (s[i + 1] == ' ' || s[i + 1] == '\\'))
        {
            result += s[i + 1];
            ++i;
        }
        else
        {
            result += c;
        }
    }
    return result;
}

}

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


/// `prefix` is the full input line up to the cursor position, as provided by replxx.
/// For example, if the user typed "ls /foo/b" and pressed Tab, prefix = "ls /foo/b".
std::vector<String> KeeperClient::getCompletions(const String & prefix) const
{
    /// Determine whether we are completing a command name or a path argument
    /// by looking for the first whitespace (end of command word).
    std::string_view word_breaks(WORD_BREAK_CHARACTERS);
    auto cmd_end = prefix.find_first_of(word_breaks);

    /// No whitespace → still typing the command name.
    if (cmd_end == String::npos || cmd_end == 0)
        return registered_commands_and_four_letter_words;

    /// Find where the path argument starts (first non-whitespace char after command).
    auto path_start = prefix.find_first_not_of(word_breaks, cmd_end);
    if (path_start == String::npos)
        path_start = prefix.size();

    /// Detect quoted mode by scanning forward from the arguments, tracking
    /// open/close quotes. If the cursor is inside an unclosed quote, we
    /// complete in quoted mode (no backslash escaping, closing quote on leaf).
    /// This correctly handles any argument position, e.g. 'create /path "val'.
    ///
    /// arg_start tracks the last unescaped word break (argument boundary).
    char quote_char = 0;
    size_t arg_start = path_start;
    for (size_t i = path_start; i < prefix.size(); ++i)
    {
        char c = prefix[i];
        if (quote_char)
        {
            if (c == quote_char)
            {
                /// Check if this quote is escaped by an odd number of preceding backslashes.
                /// e.g. \" is escaped (stays in quoted mode), \\" is not (closes the quote).
                size_t backslashes = 0;
                while (backslashes < (i - path_start) && prefix[i - 1 - backslashes] == '\\')
                    ++backslashes;

                if (backslashes % 2 == 0)
                    quote_char = 0; /// closing quote
            }
        }
        else if (c == '\'' || c == '"')
        {
            quote_char = c;
            /// Just track that we're inside a quote; arg_start stays at the word break.
        }
        else if (word_breaks.contains(c))
        {
            /// Count consecutive backslashes before this character.
            /// Odd count means this space is escaped (e.g. `\ `);
            /// even count means the backslash itself is escaped (e.g. `\\ `)
            /// and the space is a genuine word break.
            size_t backslashes = 0;
            while (backslashes < (i - path_start) && prefix[i - 1 - backslashes] == '\\')
                ++backslashes;

            if (backslashes % 2 == 0)
            {
                /// Unescaped word break — next argument starts after this.
                arg_start = i + 1;
            }
        }
    }

    /// Full argument text from arg_start, used for splitting at the last '/'.
    /// This includes any bare prefix before an opening quote (e.g. "/path/" in
    /// "/path/'child"), so parent path resolution sees the complete path.
    String full_arg = prefix.substr(arg_start);

    /// Compute the offset of replxx's "last_word" within full_completion.
    /// replxx splits on word-break characters (including space), so for quoted
    /// paths like 'ls "foo b', last_word is just 'b' even though the argument
    /// is '"foo b'. We compute last_word_offset so that our completions return
    /// only the suffix that replxx expects to match against last_word.
    auto last_word_pos = prefix.find_last_of(word_breaks);
    size_t last_word_start = (last_word_pos == String::npos) ? 0 : last_word_pos + 1;
    size_t last_word_offset = (last_word_start <= arg_start) ? 0 : (last_word_start - arg_start);

    /// Split the full argument at the last '/' into parent and child portions.
    auto last_slash = full_arg.rfind('/');
    String typed_parent_str;
    String typed_child_str;
    if (last_slash != String::npos)
    {
        typed_parent_str = full_arg.substr(0, last_slash + 1);
        typed_child_str = full_arg.substr(last_slash + 1);
    }
    else
    {
        typed_child_str = full_arg;
    }

    /// Unescape using unescapePath which handles both bare escaping (\ ) and
    /// inline quoted segments (/'dir"name'/) in a single pass.
    String unescaped_parent = unescapePath(typed_parent_str);
    String unescaped_child_prefix = unescapePath(typed_child_str);

    String parent_path;
    if (unescaped_parent.empty())
        parent_path = cwd;
    else
        parent_path = getAbsolutePath(unescaped_parent);

    Strings children;
    try
    {
        children = zookeeper->getChildren(parent_path);
    }
    catch (Coordination::Exception &) {} // NOLINT(bugprone-empty-catch)

    std::vector<String> result;

    for (const auto & child : children)
    {
        if (!unescaped_child_prefix.empty()
            && !child.starts_with(unescaped_child_prefix))
            continue;

        /// Build the full completion text for this child.
        /// In quoted mode: preserve typed_parent_str (including any bare prefix
        /// and/or opening quote), then append the child name inside the quote.
        /// The opening quote may be in typed_parent_str (e.g. "'/path/" from
        /// ls '/path/child) or in typed_child_str (e.g. "'child" from ls /path/'child).
        /// In unquoted mode: use formatKeeperNodeName which returns bare or single-quoted
        /// form — always round-trippable through the parser.
        String full_completion;
        if (quote_char)
        {
            /// If the opening quote landed after the last '/', it's in the child
            /// portion and needs to be re-added. Otherwise it's already in typed_parent_str.
            bool quote_in_child = !typed_child_str.empty() && typed_child_str[0] == quote_char;
            full_completion = typed_parent_str;
            if (quote_in_child)
                full_completion += quote_char;

            /// Escape the child name for the active quoting context: the active
            /// quote character and backslashes must be backslash-escaped so the
            /// completion round-trips through parseIdentifierOrStringLiteral.
            for (char c : child)
            {
                if (c == quote_char || c == '\\')
                    full_completion += '\\';
                full_completion += c;
            }
        }
        else
            full_completion = typed_parent_str + formatKeeperNodeName(child);

        /// Check if this node has children to decide the suffix:
        ///   - has children  → append '/' so the user can Tab-complete the next segment
        ///   - leaf node     → in quoted mode append closing quote, otherwise no suffix
        String full_path = parent_path;
        if (!full_path.ends_with('/'))
            full_path += '/';
        full_path += child;

        bool has_children = false;
        try
        {
            Strings sub = zookeeper->getChildren(full_path);
            has_children = !sub.empty();
        }
        catch (Coordination::Exception &) {} // NOLINT(bugprone-empty-catch)

        if (has_children)
            full_completion += '/';
        else if (quote_char)
            full_completion += quote_char;

        /// The completion text is the suffix starting from where replxx's
        /// last_word begins, so that prefix matching works.
        if (last_word_offset > full_completion.size())
            continue;

        String completion = full_completion.substr(last_word_offset);
        result.push_back(std::move(completion));
    }

    std::sort(result.begin(), result.end());
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

            const char * begin = text.data();
            const char * end = begin + text.size();

            /// Tokenize once for the entire input. We use skip_insignificant=false
            /// so that whitespace tokens are explicit — needed for `\ ` (escaped space)
            /// handling in parseKeeperArg. We avoid tryParseQuery because it rejects
            /// Error tokens (like `\`) during its lookahead scan.
            Tokens tokens(begin, end, 0, false);
            IParser::Pos pos(tokens, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

            while (!pos->isEnd())
            {
                /// Skip leading whitespace and semicolons (to support multi statements).
                while (!pos->isEnd() && (pos->type == TokenType::Whitespace || pos->type == TokenType::Semicolon))
                    ++pos;

                if (pos->isEnd())
                    break;

                KeeperParser parser;
                ASTPtr res;
                Expected expected;
                bool parsed = parser.parse(pos, res, expected);

                if (!parsed || !res)
                {
                    std::cerr << "Syntax error at position " << (pos->begin - begin) << "\n";
                    return true;
                }

                /// Skip trailing whitespace after the parsed command.
                while (!pos->isEnd() && pos->type == TokenType::Whitespace)
                    ++pos;

                /// After a command, the next token must be a semicolon (statement
                /// separator) or end-of-stream. Anything else is trailing garbage
                /// (e.g. "create /x v garbage") — report without executing.
                if (!pos->isEnd() && pos->type != TokenType::Semicolon)
                {
                    std::cerr << "Syntax error: unexpected trailing input at position "
                        << (pos->begin - begin) << "\n";
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

    auto reader_options = ReplxxLineReader::Options
    {
        .suggest = suggest,
        .history_file_path = history_file,
        .history_max_entries = history_max_entries,
        .multiline = false,
        .ignore_shell_suspend = false,
        .extenders = query_extenders,
        .delimiters = query_delimiters,
        .word_break_characters = WORD_BREAK_CHARACTERS,
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
