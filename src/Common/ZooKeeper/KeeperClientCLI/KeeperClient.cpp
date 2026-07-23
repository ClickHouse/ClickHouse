#include <Common/StringUtils.h>
#include <Common/filesystemHelpers.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Parsers/parseQuery.h>
#include <Common/ZooKeeper/KeeperClientCLI/KeeperClient.h>
#include <Common/ZooKeeper/KeeperClientCLI/Commands.h>
#include <algorithm>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace
{

KeeperClientBase::CommandsMap createCommandsMap()
{
    KeeperClientBase::CommandsMap commands;
    std::vector<Command> list = {
        std::make_shared<LSCommand>(),
        std::make_shared<LSRCommand>(),
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
        std::make_shared<CPRCommand>(),
        std::make_shared<MVCommand>(),
        std::make_shared<MVRCommand>(),
        std::make_shared<GetAclCommand>(),
        std::make_shared<WaitWatchCommand>(),
    };

    for (auto & command : list)
        commands.emplace(command->getName(), std::move(command));

    return commands;
}

std::vector<String> createRegisteredCommandNames(const KeeperClientBase::CommandsMap & commands)
{
    std::vector<String> names;
    names.reserve(commands.size() + four_letter_word_commands.size());

    for (const auto & [name, _] : commands)
        names.push_back(name);

    for (const auto & command : four_letter_word_commands)
        names.push_back(command);

    std::sort(names.begin(), names.end());
    return names;
}

}

/// Format a ZooKeeper node name for display and round-tripping through the parser.
/// Returns the name bare when it contains no special characters, or wrapped in
/// single quotes with \' and \\ escaping otherwise. The result is always parseable
/// by parseKeeperArg (either as a bare token or as an inline quoted segment).
/// Used by both `ls` output and tab completion.
String formatKeeperNodeName(const String & name)
{
    /// parseKeeperArg in bare mode only consumes BareWord, Slash, Dot, Number,
    /// Minus tokens. Any other character (semicolons, parens, quotes, operators,
    /// control chars, non-ASCII, etc.) would stop the parser. We check whether
    /// each byte is safe for bare output; spaces and backslashes are safe with
    /// escaping; everything else that the tokenizer treats specially needs quoting.
    bool needs_escaping = false; /// spaces/backslashes — can use backslash escaping
    bool needs_quoting = false;  /// anything else that isn't bare-safe
    for (unsigned char c : name)
    {
        if (c == ' ' || c == '\\')
            needs_escaping = true;
        else if (!isWordCharASCII(c) && c != '/' && c != '.' && c != '-')
            needs_quoting = true;
    }

    if (!needs_escaping && !needs_quoting)
        return name;

    /// If the name only has spaces and backslashes, use backslash escaping.
    /// This preserves prefix matching for tab completion (the user types bare
    /// characters, and the escaped form shares the same prefix).
    if (!needs_quoting)
    {
        String result;
        result.reserve(name.size() + 4);
        for (unsigned char c : name)
        {
            if (c == ' ' || c == '\\')
                result += '\\';
            result += static_cast<char>(c);
        }
        return result;
    }

    /// Name contains characters not safe for bare output (semicolons, parens,
    /// quotes, non-ASCII, etc.) — must use single-quoted form with \' and \\
    /// escaping. Prefix matching may not work for partially-typed names, but
    /// completing right after '/' still works.
    String result = "'";
    for (char c : name)
    {
        if (c == '\'' || c == '\\')
            result += '\\';
        result += c;
    }
    result += '\'';
    return result;
}

const KeeperClientBase::CommandsMap & KeeperClientBase::getCommands()
{
    static const CommandsMap commands = createCommandsMap();
    return commands;
}

const std::vector<String> & KeeperClientBase::getRegisteredCommandNames()
{
    static const std::vector<String> names = createRegisteredCommandNames(getCommands());
    return names;
}

String KeeperClientBase::executeFourLetterCommand(const String & /* command */)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "4lwc is not implemented");
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

void KeeperClientBase::processQueryText(const String & text)
{
    try
    {
        if (waiting_confirmation)
        {
            waiting_confirmation = false;
            if (text.size() == 1 && (text == "y" || text == "Y"))
                confirmation_callback();
            return;
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
                return;
            }

            auto * query = res->as<ASTKeeperQuery>();

            auto command = getCommands().find(query->command);
            command->second->execute(query, this);
        }
    }
    catch (Coordination::Exception & err)
    {
        cerr << err.message() << "\n";
    }
}

KeeperClientBase::KeeperClientBase(std::ostream & cout_, std::ostream & cerr_)
    : cout(cout_), cerr(cerr_)
{
}

}
