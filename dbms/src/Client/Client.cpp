#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <signal.h>

#include <iostream>
#include <fstream>
#include <iomanip>
#include <experimental/optional>

#include <unordered_set>
#include <algorithm>

#include <boost/program_options.hpp>

#include <Poco/File.h>
#include <Poco/Util/Application.h>

#include <common/ClickHouseRevision.h>

#include <DB/Common/Stopwatch.h>

#include <DB/Common/Exception.h>
#include <DB/Common/ShellCommand.h>
#include <DB/Core/Types.h>
#include <DB/Core/QueryProcessingStage.h>

#include <DB/IO/ReadBufferFromFileDescriptor.h>
#include <DB/IO/WriteBufferFromFileDescriptor.h>
#include <DB/IO/WriteBufferFromFile.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/ReadBufferFromMemory.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>

#include <DB/DataStreams/AsynchronousBlockInputStream.h>
#include <DB/DataStreams/TabSeparatedRowInputStream.h>

#include <DB/Parsers/ParserQuery.h>
#include <DB/Parsers/ASTSetQuery.h>
#include <DB/Parsers/ASTUseQuery.h>
#include <DB/Parsers/ASTInsertQuery.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTQueryWithOutput.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/formatAST.h>
#include <DB/Parsers/parseQuery.h>

#include <DB/Interpreters/Context.h>

#include <DB/Client/Connection.h>

#include "InterruptListener.h"

#include <DB/Common/ExternalTable.h>
#include <DB/Common/UnicodeBar.h>
#include <DB/Common/formatReadable.h>
#include <DB/Columns/ColumnString.h>

#include <DB/Common/NetException.h>

#include <common/config_common.h>
#include <common/readline_use.h>


/// http://en.wikipedia.org/wiki/ANSI_escape_code
#define SAVE_CURSOR_POSITION "\033[s"
#define RESTORE_CURSOR_POSITION "\033[u"
#define CLEAR_TO_END_OF_LINE "\033[K"
/// This codes are possibly not supported everywhere.
#define DISABLE_LINE_WRAPPING "\033[?7l"
#define ENABLE_LINE_WRAPPING "\033[?7h"


namespace DB
{

namespace ErrorCodes
{
	extern const int POCO_EXCEPTION;
	extern const int STD_EXCEPTION;
	extern const int UNKNOWN_EXCEPTION;
	extern const int NETWORK_ERROR;
	extern const int NO_DATA_TO_INSERT;
	extern const int BAD_ARGUMENTS;
	extern const int CANNOT_READ_HISTORY;
	extern const int CANNOT_APPEND_HISTORY;
	extern const int UNKNOWN_PACKET_FROM_SERVER;
	extern const int UNEXPECTED_PACKET_FROM_SERVER;
	extern const int CLIENT_OUTPUT_FORMAT_SPECIFIED;
}


class Client : public Poco::Util::Application
{
public:
	Client() {}

private:
	using StringSet = std::unordered_set<String>;
	StringSet exit_strings {
		"exit", "quit", "logout",
		"учше", "йгше", "дщпщге",
		"exit;", "quit;", "logout;",
		"учшеж", "йгшеж", "дщпщгеж",
		"q", "й", "\\q", "\\Q", "\\й", "\\Й", ":q", "Жй"
	};

	bool is_interactive = true;			/// Use either readline interface or batch mode.
	bool need_render_progress = true;	/// Render query execution progress.
	bool echo_queries = false;			/// Print queries before execution in batch mode.
	bool print_time_to_stderr = false;	/// Output execution time to stderr in batch mode.
	bool stdin_is_not_tty = false;		/// stdin is not a terminal.

	winsize terminal_size {};			/// Terminal size is needed to render progress bar.

	std::unique_ptr<Connection> connection;	/// Connection to DB.
	String query;						/// Current query.

	String format;						/// Query results output format.
	bool is_default_format = true;		/// false, if format is set in the config or command line.
	size_t format_max_block_size = 0;	/// Max block size for console output.
	String insert_format;				/// Format of INSERT data that is read from stdin in batch mode.
	size_t insert_format_max_block_size = 0; /// Max block size when reading INSERT data.

	bool has_vertical_output_suffix = false; /// Is \G present at the end of the query string?

	Context context;

	/// Buffer that reads from stdin in batch mode.
	ReadBufferFromFileDescriptor std_in {STDIN_FILENO};

	/// Console output.
	WriteBufferFromFileDescriptor std_out {STDOUT_FILENO};
	std::unique_ptr<ShellCommand> 
