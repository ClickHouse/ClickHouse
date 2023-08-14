#include "KeeperClient.h"
#include "Commands.h"
#include <Client/ReplxxLineReader.h>
#include <Client/ClientBase.h>
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

    String result;
    readStringUntilEOF(result, in);
    in.next();
    return result;
}

std::vector<String> KeeperClient::getCompletions(const String & prefix) const
{
    Tokens tokens(prefix.data(), prefix.data() + prefix.size(), 0, false);
    IParser::Pos pos(tokens, 0);

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
    catch (Coordination::Exception &) {}

    std::sort(result.begin(), result.end());

    return result;
}

void KeeperClient::askConfirmation(const String & prompt, std::function<void()> && callback)
{
    std::cout << prompt << " Continue?\n";
    need_confirmation = true;
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
        Poco::Util::Option("port", "p", "server port. default `2181`")
            .argument("<port>")
            .binding("port"));

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
        Poco::Util::Option("history-file", "", "set path of history file. default `~/.keeper-client-history`")
            .argument("<file>")
            .binding("history-file"));

    options.addOption(
        Poco::Util::Option("log-level", "", "set log level")
            .argument("<level>")
            .binding("log-level"));
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
        std::make_shared<GetCommand>(),
        std::make_shared<RMCommand>(),
        std::make_shared<RMRCommand>(),
        std::make_shared<HelpCommand>(),
        std::make_shared<FourLetterWordCommand>(),
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

    Poco::Logger::root().setLevel(config().getString("log-level", "error"));

    EventNotifier::init();
}

void KeeperClient::executeQuery(const String & query)
{
    std::vector<String> queries;
    boost::algorithm::split(queries, query, boost::is_any_of(";"));

    for (const auto & query_text : queries)
    {
        if (!query_text.empty())
            processQueryText(query_text);
    }
}

bool KeeperClient::processQueryText(const String & text)
{
    if (exit_strings.find(text) != exit_strings.end())
        return false;

    try
    {
        if (need_confirmation)
        {
            need_confirmation = false;
            if (text.size() == 1 && (text == "y" || text == "Y"))
                confirmation_callback();
            return true;
        }

        KeeperParser parser;
        String message;
        const char * begin = text.data();
        ASTPtr res = tryParseQuery(parser, begin, begin + text.size(), message, true, "", false, 0, 0, false);

        if (!res)
        {
            std::cerr << message << "\n";
            return true;
        }

        auto * query = res->as<ASTKeeperQuery>();

        auto command = KeeperClient::commands.find(query->command);
        command->second->execute(query, this);
    }
    catch (Coordination::Exception & err)
    {
        std::cerr << err.message() << "\n";
    }
    return true;
}

void KeeperClient::runInteractive()
{

    LineReader::Patterns query_extenders = {"\\"};
    LineReader::Patterns query_delimiters = {};

    ReplxxLineReader lr(suggest, history_file, false, query_extenders, query_delimiters, {});
    lr.enableBracketedPaste();

    while (true)
    {
        String prompt;
        if (need_confirmation)
            prompt = "[y/n] ";
        else
            prompt = cwd.string() + " :) ";

        auto input = lr.readLine(prompt, ":-] ");
        if (input.empty())
            break;

        if (!processQueryText(input))
            break;
    }
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

    auto host = config().getString("host", "localhost");
    auto port = config().getString("port", "2181");
    zk_args.hosts = {host + ":" + port};
    zk_args.connection_timeout_ms = config().getInt("connection-timeout", 10) * 1000;
    zk_args.session_timeout_ms = config().getInt("session-timeout", 10) * 1000;
    zk_args.operation_timeout_ms = config().getInt("operation-timeout", 10) * 1000;
    zookeeper = std::make_unique<zkutil::ZooKeeper>(zk_args);

    if (config().has("query"))
        executeQuery(config().getString("query"));
    else
        runInteractive();

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
    catch (const DB::Exception & e)
    {
        std::cerr << DB::getExceptionMessage(e, false) << std::endl;
        return 1;
    }
    catch (const boost::program_options::error & e)
    {
        std::cerr << "Bad arguments: " << e.what() << std::endl;
        return DB::ErrorCodes::BAD_ARGUMENTS;
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(true) << std::endl;
        return 1;
    }
}
