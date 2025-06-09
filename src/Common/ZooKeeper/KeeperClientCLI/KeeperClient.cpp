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
#include <Common/ZooKeeper/KeeperClientCLI/KeeperClient.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

String KeeperClientBase::executeFourLetterCommand(const String & command)
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

std::vector<String> KeeperClientBase::getCompletions(const String & prefix) const
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

void KeeperClientBase::askConfirmation(const String & prompt, std::function<void()> && callback)
{
    if (!ask_confirmation)
    {
        callback();
        return;
    }

    cout << prompt << " Continue?\n";
    waiting_confirmation = true;
    confirmation_callback = callback;
}

fs::path KeeperClientBase::getAbsolutePath(const String & relative) const
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

void KeeperClientBase::loadCommands(std::vector<Command> && new_commands)
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

bool KeeperClientBase::processQueryText(const String & text)
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
                cerr << message << "\n";
                return true;
            }

            auto * query = res->as<ASTKeeperQuery>();

            auto command = KeeperClientBase::commands.find(query->command);
            command->second->execute(query, this);
        }
    }
    catch (Coordination::Exception & err)
    {
        cerr << err.message() << "\n";
    }
    return true;
}

KeeperClientBase::KeeperClientBase(std::ostream & sout_, std::ostream & serr_)
    : cout(sout_), cerr(serr_)
{
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
}

}
