#include <Client/ClientBase.h>

#include <iostream>
#include <iomanip>
#include <filesystem>

#include <common/argsToConfig.h>
#include <common/DateLUT.h>
#include <common/LocalDate.h>
#include <common/LineReader.h>
#include <common/scope_guard_safe.h>

#if !defined(ARCADIA_BUILD)
#    include <Common/config_version.h>
#endif
#include <Common/UTF8Helpers.h>
#include <Common/TerminalSize.h>
#include <Common/clearPasswordFromCommandLine.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/filesystemHelpers.h>
#include <Common/Config/configReadClient.h>

#include <Client/ClientBaseHelpers.h>

#include <Parsers/parseQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTUseQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>

#include <IO/WriteBufferFromOStream.h>
#include <DataStreams/NullBlockOutputStream.h>
#include <IO/UseSSL.h>

namespace fs = std::filesystem;


namespace DB
{

static const NameSet exit_strings{"exit", "quit", "logout", "учше", "йгше", "дщпщге", "exit;", "quit;", "logout;", "учшеж", "йгшеж", "дщпщгеж", "q", "й", "\\q", "\\Q", "\\й", "\\Й", ":q", "Жй"};

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

}

namespace DB
{

void ClientBase::onProgress(const Progress & value)
{
    if (!progress_indication.updateProgress(value))
    {
        // Just a keep-alive update.
        return;
    }

    if (block_out_stream)
        block_out_stream->onProgress(value);

    if (need_render_progress)
        progress_indication.writeProgress();
}


ASTPtr ClientBase::parseQuery(const char *& pos, const char * end, bool allow_multi_statements) const
{
    ParserQuery parser(end);
    ASTPtr res;

    const auto & settings = global_context->getSettingsRef();
    size_t max_length = 0;

    if (!allow_multi_statements)
        max_length = settings.max_query_size;

    if (is_interactive || ignore_error)
    {
        String message;
        res = tryParseQuery(parser, pos, end, message, true, "", allow_multi_statements, max_length, settings.max_parser_depth);

        if (!res)
        {
            std::cerr << std::endl << message << std::endl << std::endl;
            return nullptr;
        }
    }
    else
    {
        res = parseQueryAndMovePosition(parser, pos, end, "", allow_multi_statements, max_length, settings.max_parser_depth);
    }

    if (is_interactive)
    {
        std::cout << std::endl;
        WriteBufferFromOStream res_buf(std::cout, 4096);
        formatAST(*res, res_buf);
        res_buf.next();
        std::cout << std::endl << std::endl;
    }

    return res;
}


// Consumes trailing semicolons and tries to consume the same-line trailing
// comment.
static void adjustQueryEnd(const char *& this_query_end, const char * all_queries_end, int max_parser_depth)
{
    // We have to skip the trailing semicolon that might be left
    // after VALUES parsing or just after a normal semicolon-terminated query.
    Tokens after_query_tokens(this_query_end, all_queries_end);
    IParser::Pos after_query_iterator(after_query_tokens, max_parser_depth);
    while (after_query_iterator.isValid() && after_query_iterator->type == TokenType::Semicolon)
    {
        this_query_end = after_query_iterator->end;
        ++after_query_iterator;
    }

    // Now we have to do some extra work to add the trailing
    // same-line comment to the query, but preserve the leading
    // comments of the next query. The trailing comment is important
    // because the test hints are usually written this way, e.g.:
    // select nonexistent_column; -- { serverError 12345 }.
    // The token iterator skips comments and whitespace, so we have
    // to find the newline in the string manually. If it's earlier
    // than the next significant token, it means that the text before
    // newline is some trailing whitespace or comment, and we should
    // add it to our query. There are also several special cases
    // that are described below.
    const auto * newline = find_first_symbols<'\n'>(this_query_end, all_queries_end);
    const char * next_query_begin = after_query_iterator->begin;

    // We include the entire line if the next query starts after
    // it. This is a generic case of trailing in-line comment.
    // The "equals" condition is for case of end of input (they both equal
    // all_queries_end);
    if (newline <= next_query_begin)
    {
        assert(newline >= this_query_end);
        this_query_end = newline;
    }
    else
    {
        // Many queries on one line, can't do anything. By the way, this
        // syntax is probably going to work as expected:
        // select nonexistent /* { serverError 12345 } */; select 1
    }
}


/// Flush all buffers.
void ClientBase::resetOutput()
{
    block_out_stream.reset();
    logs_out_stream.reset();

    if (pager_cmd)
    {
        pager_cmd->in.close();
        pager_cmd->wait();
    }
    pager_cmd = nullptr;

    if (out_file_buf)
    {
        out_file_buf->next();
        out_file_buf.reset();
    }

    if (out_logs_buf)
    {
        out_logs_buf->next();
        out_logs_buf.reset();
    }

    std_out.next();
}


void ClientBase::processSingleQueryImpl(const String & full_query, const String & query_to_execute, ASTPtr parsed_query, std::optional<bool> echo_query_, bool report_error)
{
    resetOutput();
    have_error = false;

    if (echo_query_ && *echo_query_)
    {
        writeString(full_query, std_out);
        writeChar('\n', std_out);
        std_out.next();
    }

    if (is_interactive)
    {
        // Generate a new query_id
        global_context->setCurrentQueryId("");
        for (const auto & query_id_format : query_id_formats)
        {
            writeString(query_id_format.first, std_out);
            writeString(fmt::format(query_id_format.second, fmt::arg("query_id", global_context->getCurrentQueryId())), std_out);
            writeChar('\n', std_out);
            std_out.next();
        }
    }

    processed_rows = 0;
    written_first_block = false;
    progress_indication.resetProgress();

    executeSingleQuery(query_to_execute, parsed_query);

    if (is_interactive)
    {
        std::cout << std::endl << processed_rows << " rows in set. Elapsed: " << progress_indication.elapsedSeconds() << " sec. ";
        progress_indication.writeFinalProgress();
        std::cout << std::endl << std::endl;
    }
    else if (print_time_to_stderr)
    {
        std::cerr << progress_indication.elapsedSeconds() << "\n";
    }

    if (have_error && report_error)
        reportQueryError(full_query);
}


bool ClientBase::processMultiQueryImpl(const String & all_queries_text,
                                       std::function<void(const String &, const String &, ASTPtr)> execute_single_query,
                                       std::function<void(const String &, Exception &)> process_parse_query_error)
{

    /// Several queries separated by ';'.
    /// INSERT data is ended by the end of line, not ';'.
    /// An exception is VALUES format where we also support semicolon in
    /// addition to end of line.
    const char * this_query_begin = all_queries_text.data();
    const char * all_queries_end = all_queries_text.data() + all_queries_text.size();

    String full_query; // full_query is the query + inline INSERT data + trailing comments (the latter is our best guess for now).
    String query_to_execute;
    ASTPtr parsed_query;

    while (this_query_begin < all_queries_end)
    {
        // Remove leading empty newlines and other whitespace, because they
        // are annoying to filter in query log. This is mostly relevant for
        // the tests.
        while (this_query_begin < all_queries_end && isWhitespaceASCII(*this_query_begin))
            ++this_query_begin;

        if (this_query_begin >= all_queries_end)
            break;

        // If there are only comments left until the end of file, we just
        // stop. The parser can't handle this situation because it always
        // expects that there is some query that it can parse.
        // We can get into this situation because the parser also doesn't
        // skip the trailing comments after parsing a query. This is because
        // they may as well be the leading comments for the next query,
        // and it makes more sense to treat them as such.
        {
            Tokens tokens(this_query_begin, all_queries_end);
            IParser::Pos token_iterator(tokens, global_context->getSettingsRef().max_parser_depth);
            if (!token_iterator.isValid())
            {
                break;
            }
        }

        const char * this_query_end = this_query_begin;
        try
        {
            parsed_query = parseQuery(this_query_end, all_queries_end, true);
        }
        catch (Exception & e)
        {
            this_query_end = find_first_symbols<'\n'>(this_query_end, all_queries_end);
            process_parse_query_error(String(this_query_begin, this_query_end - this_query_begin), e);
            /// It's expected syntax error, skip the line
            this_query_begin = this_query_end;
            continue;
        }

        if (!parsed_query)
        {
            if (ignore_error)
            {
                Tokens tokens(this_query_begin, all_queries_end);
                IParser::Pos token_iterator(tokens, global_context->getSettingsRef().max_parser_depth);
                while (token_iterator->type != TokenType::Semicolon && token_iterator.isValid())
                    ++token_iterator;
                this_query_begin = token_iterator->end;
                continue;
            }

            return true;
        }

        // INSERT queries may have the inserted data in the query text
        // that follow the query itself, e.g. "insert into t format CSV 1;2".
        // They need special handling. First of all, here we find where the
        // inserted data ends. In multy-query mode, it is delimited by a
        // newline.
        // The VALUES format needs even more handling -- we also allow the
        // data to be delimited by semicolon. This case is handled later by
        // the format parser itself.
        // We can't do multiline INSERTs with inline data, because most
        // row input formats (e.g. TSV) can't tell when the input stops,
        // unlike VALUES.
        auto * insert_ast = parsed_query->as<ASTInsertQuery>();
        if (insert_ast && insert_ast->data)
        {
            this_query_end = find_first_symbols<'\n'>(insert_ast->data, all_queries_end);
            insert_ast->end = this_query_end;
            query_to_execute = all_queries_text.substr(this_query_begin - all_queries_text.data(), insert_ast->data - this_query_begin);
        }
        else
        {
            query_to_execute = all_queries_text.substr(this_query_begin - all_queries_text.data(), this_query_end - this_query_begin);
        }

        // Try to include the trailing comment with test hints. It is just
        // a guess for now, because we don't yet know where the query ends
        // if it is an INSERT query with inline data. We will do it again
        // after we have processed the query. But even this guess is
        // beneficial so that we see proper trailing comments in "echo" and
        // server log.
        adjustQueryEnd(this_query_end, all_queries_end, global_context->getSettingsRef().max_parser_depth);

        full_query = all_queries_text.substr(this_query_begin - all_queries_text.data(), this_query_end - this_query_begin);
        if (query_fuzzer_runs)
        {
            if (!processWithFuzzing(full_query))
                return false;
            this_query_begin = this_query_end;
            continue;
        }

        execute_single_query(String(this_query_begin, this_query_end - this_query_begin), query_to_execute, parsed_query);

        // For INSERTs with inline data: use the end of inline data as
        // reported by the format parser (it is saved in sendData()).
        // This allows us to handle queries like:
        //   insert into t values (1); select 1
        // , where the inline data is delimited by semicolon and not by a
        // newline.
        if (insert_ast && insert_ast->data)
        {
            this_query_end = insert_ast->end;
            adjustQueryEnd(this_query_end, all_queries_end, global_context->getSettingsRef().max_parser_depth);
        }

        // Report error.
        if (have_error)
            reportQueryError(full_query);

        // Stop processing queries if needed.
        if (have_error && !ignore_error)
        {
            if (is_interactive)
                break;
            else
                return false;
        }

        this_query_begin = this_query_end;
    }
    return true;
}


bool ClientBase::processQueryText(const String & text)
{
    if (exit_strings.end() != exit_strings.find(trim(text, [](char c) { return isWhitespaceASCII(c) || c == ';'; })))
        return false;

    if (!is_multiquery)
    {
        assert(!query_fuzzer_runs);
        processSingleQuery(text);

        return true;
    }

    if (query_fuzzer_runs)
    {
        processWithFuzzing(text);
        return true;
    }

    return processMultiQuery(text);
}


void ClientBase::runInteractive(std::function<bool(std::function<bool()>)> try_process_query_text)
{
    if (config().has("query_id"))
        throw Exception("query_id could be specified only in non-interactive mode", ErrorCodes::BAD_ARGUMENTS);
    if (print_time_to_stderr)
        throw Exception("time option could be specified only in non-interactive mode", ErrorCodes::BAD_ARGUMENTS);

    /// Initialize DateLUT here to avoid counting time spent here as query execution time.
    const auto local_tz = DateLUT::instance().getTimeZone();

    std::optional<Suggest> suggest;
    suggest.emplace();
    loadSuggestionData(*suggest);

    if (home_path.empty())
    {
        const char * home_path_cstr = getenv("HOME");
        if (home_path_cstr)
            home_path = home_path_cstr;
    }

    /// Initialize query_id_formats if any
    if (config().has("query_id_formats"))
    {
        Poco::Util::AbstractConfiguration::Keys keys;
        config().keys("query_id_formats", keys);
        for (const auto & name : keys)
            query_id_formats.emplace_back(name + ":", config().getString("query_id_formats." + name));
    }

    if (query_id_formats.empty())
        query_id_formats.emplace_back("Query id:", " {query_id}\n");

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

        if (!try_process_query_text([&]() -> bool { return processQueryText(input); }))
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


void ClientBase::runNonInteractive()
{
    if (!queries_files.empty())
    {
        auto process_multi_query_from_file = [&](const String & file)
        {
            auto text = getQueryTextPrefix();
            String queries_from_file;

            ReadBufferFromFile in(file);
            readStringUntilEOF(queries_from_file, in);

            text += queries_from_file;
            return processMultiQuery(text);
        };

        /// Read all queries into `text`.
        for (const auto & queries_file : queries_files)
        {
            for (const auto & interleave_file : interleave_queries_files)
                if (!process_multi_query_from_file(interleave_file))
                    return;

            if (!process_multi_query_from_file(queries_file))
                return;
        }

        return;
    }

    String text;
    if (is_multiquery)
        text = getQueryTextPrefix();

    if (config().has("query"))
    {
        text += config().getRawString("query"); /// Poco configuration should not process substitutions in form of ${...} inside query.
    }
    else
    {
        /// If 'query' parameter is not set, read a query from stdin.
        /// The query is read entirely into memory (streaming is disabled).
        ReadBufferFromFileDescriptor in(STDIN_FILENO);
        readStringUntilEOF(text, in);
    }

    if (query_fuzzer_runs)
        processWithFuzzing(text);
    else
        processQueryText(text);
}


static void clearTerminal()
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


int ClientBase::main(const std::vector<std::string> & /*args*/)
{
    UseSSL use_ssl;

    std::cout << std::fixed << std::setprecision(3);
    std::cerr << std::fixed << std::setprecision(3);

    if (is_interactive)
    {
        clearTerminal();
        showClientVersion();
    }

    return mainImpl();
}


void ClientBase::init(int argc, char ** argv)
{
    namespace po = boost::program_options;

    /// Don't parse options with Poco library, we prefer neat boost::program_options.
    stopOptionsProcessing();

    stdin_is_a_tty = isatty(STDIN_FILENO);
    stdout_is_a_tty = isatty(STDOUT_FILENO);
    terminal_width = getTerminalWidth();

    Arguments common_arguments{""}; /// 0th argument is ignored.
    std::vector<Arguments> external_tables_arguments;

    readArguments(argc, argv, common_arguments, external_tables_arguments);

    po::variables_map options;
    OptionsDescription options_description;
    addAndCheckOptions(options_description, options, common_arguments);
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
    clearPasswordFromCommandLine(argc, argv);
}

}
