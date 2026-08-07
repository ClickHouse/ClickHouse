#include <Common/StringUtils.h>
#include <Common/filesystemHelpers.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Parsers/parseQuery.h>
#include <Common/ZooKeeper/KeeperClientCLI/KeeperClient.h>
#include <Common/ZooKeeper/KeeperClientCLI/Commands.h>
#include <algorithm>
#include <string>
#include <string_view>
#include <vector>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace
{

/// Word breaks used by replxx / clickhouse-keeper-client line editing.
constexpr std::string_view WORD_BREAK_CHARACTERS = " \t\v\f\a\b\r\n";

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

KeeperCompletionResult KeeperClientBase::completeQueryPrefix(const String & prefix) const
{
    KeeperCompletionResult result;

    /// Skip leading indentation so "  cre" still completes command names and
    /// " ls /…" still reaches path completion. replace_start points at the first
    /// non-word-break character so insertions keep any leading spaces.
    auto cmd_start = prefix.find_first_not_of(WORD_BREAK_CHARACTERS);
    if (cmd_start == String::npos)
    {
        result.replace_start = prefix.size();
        for (const auto & name : getRegisteredCommandNames())
            result.completions.push_back(name);
        return result;
    }

    /// End of the command word (first whitespace after the command name).
    auto cmd_end = prefix.find_first_of(WORD_BREAK_CHARACTERS, cmd_start);

    /// No whitespace after the command → still typing the command name.
    if (cmd_end == String::npos)
    {
        const String cmd_prefix = prefix.substr(cmd_start);
        result.replace_start = cmd_start;
        for (const auto & name : getRegisteredCommandNames())
        {
            if (cmd_prefix.empty() || name.starts_with(cmd_prefix))
                result.completions.push_back(name);
        }
        return result;
    }

    /// Find where the path argument starts (first non-whitespace char after command).
    auto path_start = prefix.find_first_not_of(WORD_BREAK_CHARACTERS, cmd_end);
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
        else if (WORD_BREAK_CHARACTERS.contains(c))
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
    auto last_word_pos = prefix.find_last_of(WORD_BREAK_CHARACTERS);
    size_t last_word_start = (last_word_pos == String::npos) ? 0 : last_word_pos + 1;
    size_t last_word_offset = (last_word_start <= arg_start) ? 0 : (last_word_start - arg_start);
    result.replace_start = last_word_start;

    if (!zookeeper)
        return result;

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
    catch (Coordination::Exception &) // NOLINT(bugprone-empty-catch) Ok: completion is best-effort if the parent path is missing.
    {
    }

    struct CompletionCandidate
    {
        String full_completion;
        String full_path;
    };

    std::vector<CompletionCandidate> candidates;
    candidates.reserve(children.size());

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

        String full_path = parent_path;
        if (!full_path.ends_with('/'))
            full_path += '/';
        full_path += child;

        candidates.push_back(CompletionCandidate{std::move(full_completion), std::move(full_path)});
    }

    /// Decide '/' vs closing-quote suffix from Stat.numChildren via batched exists.
    /// Avoids per-candidate getChildren (1 + N full list reads on every Tab).
    std::vector<char> has_children(candidates.size(), 0);
    if (!candidates.empty())
    {
        std::vector<std::string> paths_to_check;
        paths_to_check.reserve(candidates.size());
        for (const auto & candidate : candidates)
            paths_to_check.push_back(candidate.full_path);

        try
        {
            auto responses = zookeeper->exists(paths_to_check);
            for (size_t i = 0; i < candidates.size(); ++i)
            {
                if (responses[i].error == Coordination::Error::ZOK)
                    has_children[i] = responses[i].stat.numChildren > 0;
            }
        }
        catch (Coordination::Exception &) // NOLINT(bugprone-empty-catch) Ok: treat exists() failure as unknown child counts.
        {
        }
    }

    for (size_t i = 0; i < candidates.size(); ++i)
    {
        String & full_completion = candidates[i].full_completion;

        ///   - has children  → append '/' so the user can Tab-complete the next segment
        ///   - leaf node     → in quoted mode append closing quote, otherwise no suffix
        if (has_children[i])
            full_completion += '/';
        else if (quote_char)
            full_completion += quote_char;

        /// The completion text is the suffix starting from where replxx's
        /// last_word begins, so that prefix matching works.
        if (last_word_offset > full_completion.size())
            continue;

        result.completions.push_back(full_completion.substr(last_word_offset));
    }

    std::sort(result.completions.begin(), result.completions.end());
    return result;
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
