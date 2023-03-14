#include "KeeperClient.h"
#include <Client/ReplxxLineReader.h>
#include <Client/ClientBase.h>
#include <Common/EventNotifier.h>
#include <Common/filesystemHelpers.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <boost/program_options.hpp>


namespace po = boost::program_options;
namespace fs = std::filesystem;

namespace DB
{

static const NameSet four_letter_word_commands
{
    "ruok", "mntr", "srvr", "stat", "srst", "conf",
    "cons", "crst", "envi", "dirs", "isro", "wchs",
    "wchc", "wchp", "dump", "csnp", "lgif", "rqld",
};

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

void KeeperClient::askConfirmation(const String & prompt, std::function<void()> && callback)
{
    std::cout << prompt << " Continue?\n";
    need_confirmation = true;
    confirmation_callback = callback;
}

String KeeperClient::getAbsolutePath(const String & relative)
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

void KeeperClient::loadCommands(std::vector<std::tuple<String, size_t, Callback>> && new_commands)
{
    for (const auto & [name, args_count, callback] : new_commands)
    {
        commands.insert({{name, args_count}, callback});
        suggest.addWords({name});
    }

    for (const auto & command : four_letter_word_commands) {
        suggest.addWords({command});
    }
}

void KeeperClient::defineOptions(Poco::Util::OptionSet & options)
{
    Poco::Util::Application::defineOptions(options);

    options.addOption(
        Poco::Util::Option("help", "h", "show help and exit")
            .binding("help"));

    options.addOption(
        Poco::Util::Option("query", "q", "will execute given query, then exit.")
            .argument("query")
            .binding("query"));

    options.addOption(
        Poco::Util::Option("connection-timeout", "", "set connection timeout in seconds. default 10s.")
            .argument("connection-timeout")
            .binding("connection-timeout"));

    options.addOption(
        Poco::Util::Option("session-timeout", "", "set session timeout in seconds. default 10s.")
            .argument("session-timeout")
            .binding("session-timeout"));

    options.addOption(
        Poco::Util::Option("operation-timeout", "", "set operation timeout in seconds. default 10s.")
            .argument("operation-timeout")
            .binding("operation-timeout"));

    options.addOption(
        Poco::Util::Option("history-file", "", "set path of history file. default `~/.keeper-client-history`")
            .argument("history-file")
            .binding("history-file"));

    options.addOption(
        Poco::Util::Option("log-level", "", "set log level")
            .argument("log-level")
            .binding("log-level"));
}

void KeeperClient::initialize(Poco::Util::Application & /* self */)
{
    loadCommands({
        {"set", 2, [](KeeperClient * client, const std::vector<String> & args)
         {
             client->zookeeper->set(client->getAbsolutePath(args[1]), args[2]);
         }},

        {"create", 2, [](KeeperClient * client, const std::vector<String> & args)
         {
             client->zookeeper->create(client->getAbsolutePath(args[1]), args[2], zkutil::CreateMode::Persistent);
         }},

        {"get", 1, [](KeeperClient * client, const std::vector<String> & args)
         {
             std::cout << client->zookeeper->get(client->getAbsolutePath(args[1])) << "\n";
         }},

        {"ls", 0, [](KeeperClient * client, const std::vector<String> & /* args */)
         {
             auto children = client->zookeeper->getChildren(client->cwd);
             for (auto & child : children)
                 std::cout << child << " ";
             std::cout << "\n";
         }},

        {"ls", 1, [](KeeperClient * client, const std::vector<String> & args)
         {
             auto children = client->zookeeper->getChildren(client->getAbsolutePath(args[1]));
             for (auto & child : children)
                 std::cout << child << " ";
             std::cout << "\n";
         }},

        {"cd", 0, [](KeeperClient * /* client */, const std::vector<String> & /* args */)
         {
         }},

        {"cd", 1, [](KeeperClient * client, const std::vector<String> & args)
         {
             auto new_path = client->getAbsolutePath(args[1]);
             if (!client->zookeeper->exists(new_path))
                 std::cerr << "Path " << new_path << " does not exists\n";
             else
                client->cwd = new_path;
         }},

        {"rm", 1, [](KeeperClient * client, const std::vector<String> & args)
         {
             client->zookeeper->remove(client->getAbsolutePath(args[1]));
         }},

        {"rmr", 1, [](KeeperClient * client, const std::vector<String> & args)
         {
             String path = client->getAbsolutePath(args[1]);
             client->askConfirmation("You are going to recursively delete path " + path,
                                     [client, path]{ client->zookeeper->removeRecursive(path); });
         }},
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

    std::vector<String> tokens;
    boost::algorithm::split(tokens, text, boost::is_any_of(" "));

    try
    {
        if (need_confirmation)
        {
            need_confirmation = false;
            if (tokens.size() == 1 && (tokens[0] == "y" || tokens[0] == "Y"))
                confirmation_callback();
        }
        else if (tokens.size() == 1 && tokens[0].size() == 4 && four_letter_word_commands.find(tokens[0]) != four_letter_word_commands.end())
            std::cout << executeFourLetterCommand(tokens[0]) << "\n";
        else
        {
            auto callback = commands.find({tokens[0], tokens.size() - 1});
            if (callback == commands.end())
            {
                if (tokens[0].size() == 4 && tokens.size() == 1)  /// Treat it like unrecognized four-letter command
                    std::cout << executeFourLetterCommand(tokens[0]) << "\n";
                else
                    std::cerr << "No command found with name " << tokens[0] << " and args count " << tokens.size() - 1 << "\n";
            }
            else
                callback->second(this, tokens);
        }
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

int KeeperClient::main(const std::vector<String> & args)
{
    if (args.empty())
        zk_args.hosts = {"localhost:2181"};
    else
        zk_args.hosts = {args[0]};

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
