#define DBMS_CLIENT 1	/// Используется в Context.h

#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <signal.h>

#include <readline/readline.h>
#include <readline/history.h>

#include <iostream>
#include <fstream>
#include <iomanip>

#include <tr1/unordered_set>

#include <boost/assign/list_inserter.hpp>

#include <Poco/File.h>
#include <Poco/SharedPtr.h>
#include <Poco/Util/Application.h>

#include <Yandex/Revision.h>

#include <statdaemons/Stopwatch.h>

#include <DB/Core/Exception.h>
#include <DB/Core/Types.h>
#include <DB/Core/QueryProcessingStage.h>

#include <DB/IO/ReadBufferFromFileDescriptor.h>
#include <DB/IO/WriteBufferFromFileDescriptor.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/IO/copyData.h>

#include <DB/DataStreams/AsynchronousBlockInputStream.h>

#include <DB/Parsers/ParserQuery.h>
#include <DB/Parsers/formatAST.h>

#include <DB/Interpreters/Context.h>

#include <DB/Client/Connection.h>


/** Клиент командной строки СУБД ClickHouse.
  */


namespace DB
{

using Poco::SharedPtr;


/** Пока существует объект этого класса - блокирует сигнал INT, при этом позволяет узнать, не пришёл ли он.
  * Это нужно, чтобы можно было прервать выполнение запроса с помощью Ctrl+C.
  * В один момент времени используйте только один экземпляр этого класса.
  * Если метод check вернул true (пришёл сигнал), то следующие вызовы будут ждать следующий сигнал.
  */
class InterruptListener
{
private:
	bool active;
	sigset_t sig_set;

public:
	InterruptListener() : active(false)
	{
		if (sigemptyset(&sig_set)
			|| sigaddset(&sig_set, SIGINT))
			throwFromErrno("Cannot manipulate with signal set.", ErrorCodes::CANNOT_MANIPULATE_SIGSET);
		
		block();
	}

	~InterruptListener()
	{
		unblock();
	}

	bool check()
	{
		if (!active)
			return false;
		
		timespec timeout = { 0, 0 };
		
		if (-1 == sigtimedwait(&sig_set, NULL, &timeout))
		{
			if (errno == EAGAIN)
				return false;
			else
				throwFromErrno("Cannot poll signal (sigtimedwait).", ErrorCodes::CANNOT_WAIT_FOR_SIGNAL);
		}

		return true;
	}

	void block()
	{
		if (!active)
		{
			if (pthread_sigmask(SIG_BLOCK, &sig_set, NULL))
				throwFromErrno("Cannot block signal.", ErrorCodes::CANNOT_BLOCK_SIGNAL);

			active = true;
		}
	}

	/// Можно прекратить блокировать сигнал раньше, чем в деструкторе.
	void unblock()
	{
		if (active)
		{
			if (pthread_sigmask(SIG_UNBLOCK, &sig_set, NULL))
				throwFromErrno("Cannot unblock signal.", ErrorCodes::CANNOT_UNBLOCK_SIGNAL);

			active = false;
		}
	}
};


class Client : public Poco::Util::Application
{
public:
	Client() : is_interactive(true), stdin_is_not_tty(false), query_id(0),
		format_max_block_size(0), std_in(STDIN_FILENO), std_out(STDOUT_FILENO), processed_rows(0),
		rows_read_on_server(0), bytes_read_on_server(0), written_progress_chars(0), written_first_block(false) {}
	
private:
	typedef std::tr1::unordered_set<String> StringSet;
	StringSet exit_strings;

	bool is_interactive;				/// Использовать readline интерфейс или batch режим.
	bool stdin_is_not_tty;				/// stdin - не терминал.

	SharedPtr<Connection> connection;	/// Соединение с БД.
	String query;						/// Текущий запрос.
	UInt64 query_id;					/// Идентификатор запроса. Его можно использовать, чтобы отменить запрос.

	String format;						/// Формат вывода результата в консоль.
	size_t format_max_block_size;		/// Максимальный размер блока при выводе в консоль.
	String insert_format;				/// Формат данных для INSERT-а при чтении их из stdin в batch режиме
	size_t insert_format_max_block_size; /// Максимальный размер блока при чтении данных INSERT-а.

	Context context;

	/// Чтение из stdin для batch режима
	ReadBufferFromFileDescriptor std_in;
	BlockInputStreamPtr block_std_in;

	/// Вывод в консоль
	WriteBufferFromFileDescriptor std_out;
	BlockOutputStreamPtr block_std_out;

	String home_path;
	
	/// Путь к файлу истории команд.
	String history_file;

	/// Строк прочитано или записано.
	size_t processed_rows;

	/// Распарсенный запрос. Оттуда берутся некоторые настройки (формат).
	ASTPtr parsed_query;
	
	/// Последнее полученное от сервера исключение.
	ExceptionPtr last_exception;

	Stopwatch watch;

	size_t rows_read_on_server;
	size_t bytes_read_on_server;
	size_t written_progress_chars;
	bool written_first_block;


	void initialize(Poco::Util::Application & self)
	{
		Poco::Util::Application::initialize(self);

		boost::assign::insert(exit_strings)
			("exit")("quit")("logout")
			("учше")("йгше")("дщпщге")
			("exit;")("quit;")("logout;")
			("учше;")("йгше;")("дщпщге;")
			("q")("й")("\\q")("\\Q");

		const char * home_path_cstr = getenv("HOME");
		if (!home_path_cstr)
			throw Exception("Cannot get HOME environment variable");
		else
			home_path = home_path_cstr;

		if (config().has("config-file"))
			loadConfiguration(config().getString("config-file"));
		else if (Poco::File("./clickhouse-client.xml").exists())
			loadConfiguration("./clickhouse-client.xml");
		else if (Poco::File(home_path + "/.clickhouse-client/config.xml").exists())
			loadConfiguration(home_path + "/.clickhouse-client/config.xml");
		else if (Poco::File("/etc/clickhouse-client/config.xml").exists())
			loadConfiguration("/etc/clickhouse-client/config.xml");
	}


	int main(const std::vector<std::string> & args)
	{
		try
		{
			return mainImpl(args);
		}
		catch (const Exception & e)
		{
			std::string text = e.displayText();
			
 			std::cerr << "Code: " << e.code() << ". " << text << std::endl << std::endl;

			/// Если есть стек-трейс на сервере, то не будем писать стек-трейс на клиенте.
			if (std::string::npos == text.find("Stack trace"))
				std::cerr << "Stack trace:" << std::endl
					<< e.getStackTrace().toString();
			
			return e.code();
		}
		catch (const Poco::Exception & e)
		{
			std::cerr << "Poco::Exception: " << e.displayText() << std::endl;
			return ErrorCodes::POCO_EXCEPTION;
		}
		catch (const std::exception & e)
		{
			std::cerr << "std::exception: " << e.what() << std::endl;
			return ErrorCodes::STD_EXCEPTION;
		}
		catch (...)
		{
			std::cerr << "Unknown exception" << std::endl;
			return ErrorCodes::UNKNOWN_EXCEPTION;
		}
	}
	

	int mainImpl(const std::vector<std::string> & args)
	{
		/** Будем работать в batch режиме, если выполнено одно из следующих условий:
		  * - задан параметр -e (--query)
		  *   (в этом случае - запрос или несколько запросов берём оттуда;
		  *    а если при этом stdin не терминал, то берём оттуда данные для INSERT-а первого запроса).
		  * - stdin - не терминал (в этом случае, считываем оттуда запросы);
		  */
		stdin_is_not_tty = !isatty(STDIN_FILENO);
		if (stdin_is_not_tty || config().has("query"))
			is_interactive = false;

		std::cout << std::fixed << std::setprecision(3);
		std::cerr << std::fixed << std::setprecision(3);
		
		if (is_interactive)
			std::cout << "ClickHouse client version " << DBMS_VERSION_MAJOR
				<< "." << DBMS_VERSION_MINOR
				<< "." << Revision::get()
				<< "." << std::endl;

		format = config().getString("format", is_interactive ? "PrettyCompact" : "TabSeparated");
		format_max_block_size = config().getInt("format_max_block_size", DEFAULT_BLOCK_SIZE);

		insert_format = "Values";
		insert_format_max_block_size = config().getInt("insert_format_max_block_size", format_max_block_size);

		connect();

		if (is_interactive)
		{
			/// Отключаем tab completion.
			rl_bind_key('\t', rl_insert);

			/// Загружаем историю команд, если есть.
			history_file = config().getString("history_file", home_path + "/.clickhouse-client-history");
			if (Poco::File(history_file).exists())
			{
				int res = read_history(history_file.c_str());
				if (res)
					throwFromErrno("Cannot read history from file " + history_file, ErrorCodes::CANNOT_READ_HISTORY);
			}
			else	/// Создаём файл с историей.
				Poco::File(history_file).createFile();

			/// Инициализируем DateLUT, чтобы потраченное время не отображалось, как время, потраченное на запрос.
			DateLUTSingleton::instance();

			loop();

			std::cout << "Bye." << std::endl;
			
			return 0;
		}
		else
		{
			nonInteractive();
			
			if (last_exception)
				return last_exception->code();
			
			return 0;
		}
	}


	void connect()
	{
		String host = config().getString("host", "localhost");
		UInt16 port = config().getInt("port", DBMS_DEFAULT_PORT);
		String default_database = config().getString("database", "");
		String user = config().getString("user", "");
		String password = config().getString("password", "");
		
		Protocol::Compression::Enum compression = config().getBool("compression", true)
			? Protocol::Compression::Enable
			: Protocol::Compression::Disable;

		if (is_interactive)
			std::cout << "Connecting to "
				<< (!default_database.empty() ? "database " + default_database + " at " : "")
				<< host << ":" << port
				<< (!user.empty() ? " as user " + user : "")
				<< "." << std::endl;

		connection = new Connection(host, port, default_database, user, password, context.getDataTypeFactory(), "client", compression,
			Poco::Timespan(config().getInt("connect_timeout", DBMS_DEFAULT_CONNECT_TIMEOUT_SEC), 0),
			Poco::Timespan(config().getInt("receive_timeout", DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC), 0),
			Poco::Timespan(config().getInt("send_timeout", DBMS_DEFAULT_SEND_TIMEOUT_SEC), 0));

		if (is_interactive)
		{
			String server_name;
			UInt64 server_version_major = 0;
			UInt64 server_version_minor = 0;
			UInt64 server_revision = 0;

			connection->getServerVersion(server_name, server_version_major, server_version_minor, server_revision);

			std::cout << "Connected to " << server_name
				<< " server version " << server_version_major
				<< "." << server_version_minor
				<< "." << server_revision
				<< "." << std::endl << std::endl;
		}
	}

	
	static bool isWhitespace(char c)
	{
		return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f';
	}
	

	void loop()
	{
		String query;
		String prev_query;
		while (char * line_ = readline(query.empty() ? ":) " : ":-] "))
		{
			String line = line_;
			free(line_);
			
			size_t ws = line.size();
			while (ws > 0 && isWhitespace(line[ws - 1]))
				--ws;
			
			if (ws == 0 && query.empty())
				continue;
			
			bool ends_with_semicolon = line[ws - 1] == ';';
			bool ends_with_backslash = line[ws - 1] == '\\';
			
			if (ends_with_backslash)
				line = line.substr(0, ws - 1);
			
			query += line;
			
			if (!ends_with_backslash && (ends_with_semicolon || !config().hasOption("multiline")))
			{
				if (query != prev_query)
				{
					add_history(query.c_str());

					if (append_history(1, history_file.c_str()))
						throwFromErrno("Cannot append history to file " + history_file, ErrorCodes::CANNOT_APPEND_HISTORY);

					prev_query = query;
				}

				try
				{
					if (!process(query))
						break;
				}
				catch (const Exception & e)
				{
					std::cerr << std::endl
						<< "Exception on client:" << std::endl
						<< "Code: " << e.code() << ". " << e.displayText() << std::endl
						<< std::endl;

					/** Эксепшен на клиенте в процессе обработки запроса может привести к рассинхронизации соединения.
					  * Установим соединение заново и позволим ввести следующий запрос.
					  */
					connect();
				}

				query = "";
			}
			else
			{
				query += '\n';
			}
		}
	}


	void nonInteractive()
	{
		if (config().has("query"))
			process(config().getString("query"));
		else
		{
			/** В случае, если параметр query не задан, то запрос будет читаться из stdin.
			  * При этом, запрос будет читаться не потоково (целиком в оперативку).
			  * Поддерживается только один запрос в stdin.
			  */
			
			String stdin_str;

			{
				ReadBufferFromFileDescriptor in(STDIN_FILENO);
				WriteBufferFromString out(stdin_str);
				copyData(in, out);
			}

			process(stdin_str);
		}
	}


	bool process(const String & line)
	{
		if (exit_strings.end() != exit_strings.find(line))
			return false;

		block_std_in = NULL;
		block_std_out = NULL;

		watch.restart();

		query = line;

		/// Некоторые части запроса выполняются на стороне клиента (форматирование результата). Поэтому, распарсим запрос.
		if (!parseQuery())
			return true;

		++query_id;
		processed_rows = 0;
		rows_read_on_server = 0;
		bytes_read_on_server = 0;
		written_progress_chars = 0;
		written_first_block = false;

		/// Запрос INSERT (но только тот, что требует передачи данных - не INSERT SELECT), обрабатывается отдельным способом.
		const ASTInsertQuery * insert = dynamic_cast<const ASTInsertQuery *>(&*parsed_query);

		if (insert && !insert->select)
			processInsertQuery();
		else
			processOrdinaryQuery();

		if (is_interactive)
		{
			std::cout << std::endl
				<< processed_rows << " rows in set. Elapsed: " << watch.elapsedSeconds() << " sec. ";

			if (rows_read_on_server)
				writeFinalProgress();

			std::cout << std::endl << std::endl;
		}

		return true;
	}


	/// Обработать запрос, который не требует передачи блоков данных на сервер.
	void processOrdinaryQuery()
	{
		connection->sendQuery(query, query_id, QueryProcessingStage::Complete);
		receiveResult();
	}


	/// Обработать запрос, который требует передачи блоков данных на сервер.
	void processInsertQuery()
	{
		/// Отправляем часть запроса - без данных, так как данные будут отправлены отдельно.
		const ASTInsertQuery & parsed_insert_query = dynamic_cast<const ASTInsertQuery &>(*parsed_query);
		String query_without_data = parsed_insert_query.data
			? query.substr(0, parsed_insert_query.data - query.data())
			: query;

		if ((is_interactive && !parsed_insert_query.data) || (stdin_is_not_tty && std_in.eof()))
			throw Exception("No data to insert", ErrorCodes::NO_DATA_TO_INSERT);

		connection->sendQuery(query_without_data, query_id, QueryProcessingStage::Complete);

		/// Получим структуру таблицы
		Block sample = receiveSampleBlock();

		sendData(sample);
		receivePacket();
	}


	bool parseQuery()
	{
		ParserQuery parser;
		std::string expected;

		const char * begin = query.data();
		const char * end = begin + query.size();
		const char * pos = begin;

		bool parse_res = parser.parse(pos, end, parsed_query, expected);

		/// Распарсенный запрос должен заканчиваться на конец входных данных или на точку с запятой.
		if (!parse_res || (pos != end && *pos != ';'))
		{
			std::stringstream message;

			message << "Syntax error: failed at position "
				<< (pos - begin) << ": "
				<< std::string(pos, std::min(SHOW_CHARS_ON_SYNTAX_ERROR, end - pos))
				<< ", expected " << (parse_res ? "end of query" : expected) << "."
				<< std::endl << std::endl;

			if (is_interactive)
				std::cerr << message.str();
			else
				throw Exception(message.str(), ErrorCodes::SYNTAX_ERROR);

			return false;
		}

		if (is_interactive)
		{
			std::cout << std::endl;
			formatAST(*parsed_query, std::cout);
			std::cout << std::endl << std::endl;
		}
		
		return true;
	}


	void sendData(Block & sample)
	{
		/// Если нужно отправить данные INSERT-а.
		const ASTInsertQuery * parsed_insert_query = dynamic_cast<const ASTInsertQuery *>(&*parsed_query);
		if (!parsed_insert_query)
			return;

		if (parsed_insert_query->data)
		{
			/// Отправляем данные из запроса.
			ReadBuffer data_in(const_cast<char *>(parsed_insert_query->data), parsed_insert_query->end - parsed_insert_query->data, 0);
			sendDataFrom(data_in, sample);
		}
		else if (!is_interactive)
		{
			/// Отправляем данные из stdin.
			sendDataFrom(std_in, sample);
		}
		else
			throw Exception("No data to insert", ErrorCodes::NO_DATA_TO_INSERT);
	}


	void sendDataFrom(ReadBuffer & buf, Block & sample)
	{
		String current_format = insert_format;

		/// Формат может быть указан в INSERT запросе.
		if (ASTInsertQuery * insert = dynamic_cast<ASTInsertQuery *>(&*parsed_query))
			if (!insert->format.empty())
				current_format = insert->format;

		block_std_in = new AsynchronousBlockInputStream(context.getFormatFactory().getInput(
			current_format, buf, sample, insert_format_max_block_size, context.getDataTypeFactory()));
		block_std_in->readPrefix();

		while (true)
		{
			Block block = block_std_in->read();
			connection->sendData(block);
			processed_rows += block.rows();

			if (!block)
				break;
		}
		
		block_std_in->readSuffix();
	}


	/** Получает и обрабатывает пакеты из сервера.
	  * Также следит, не требуется ли прервать выполнение запроса.
	  */
	void receiveResult()
	{
		InterruptListener interrupt_listener;
		bool cancelled = false;

		while (true)
		{
			/** Проверим, не требуется ли остановить выполнение запроса (Ctrl+C).
			  * Если требуется - отправим об этом информацию на сервер.
			  * После чего, получим оставшиеся пакеты с сервера (чтобы не было рассинхронизации).
			  */
			if (!cancelled)
			{
				if (interrupt_listener.check())
				{
					connection->sendCancel();
					cancelled = true;
					if (is_interactive)
						std::cout << "Cancelling query." << std::endl;

					/// Повторное нажатие Ctrl+C приведёт к завершению работы.
					interrupt_listener.unblock();
				}
				else if (!connection->poll(1000000))
					continue;	/// Если новых данных в ещё нет, то после таймаута продолжим проверять, не остановлено ли выполнение запроса.
			}

			if (!receivePacket())
				break;
		}

		if (cancelled && is_interactive)
			std::cout << "Query was cancelled." << std::endl;
	}


	/** Получить кусок результата или прогресс выполнения или эксепшен,
	  *  и обработать пакет соответствующим образом.
	  * Возвращает true, если нужно продолжать чтение пакетов.
	  */
	bool receivePacket()
	{
		Connection::Packet packet = connection->receivePacket();

		switch (packet.type)
		{
			case Protocol::Server::Data:
				onData(packet.block);
				return true;

			case Protocol::Server::Progress:
				onProgress(packet.progress);
				return true;
				
			case Protocol::Server::ProfileInfo:
				onProfileInfo(packet.profile_info);
				return true;

			case Protocol::Server::Totals:
				onTotals(packet.block);
				return true;

			case Protocol::Server::Extremes:
				onExtremes(packet.block);
				return true;

			case Protocol::Server::Exception:
				onException(*packet.exception);
				last_exception = packet.exception;
				return false;

			case Protocol::Server::EndOfStream:
				onEndOfStream();
				return false;

			default:
				throw Exception("Unknown packet from server", ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);
		}
	}


	/** Получить блок - пример структуры таблицы, в которую будут вставляться данные.
	  */
	Block receiveSampleBlock()
	{
		Connection::Packet packet = connection->receivePacket();

		switch (packet.type)
		{
			case Protocol::Server::Data:
				return packet.block;

			default:
				throw Exception("Unexpected packet from server (expected Data, got "
					+ String(Protocol::Server::toString(packet.type)) + ")", ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER);
		}
	}


	void onData(Block & block)
	{
		if (written_progress_chars)
		{
			for (size_t i = 0; i < written_progress_chars; ++i)
				std::cerr << "\b \b";

			written_progress_chars = 0;
		}

		if (!block)
			return;

		processed_rows += block.rows();
		if (!block_std_out)
		{
			String current_format = format;

			/// Формат может быть указан в запросе.
			if (ASTQueryWithOutput * query_with_output = dynamic_cast<ASTQueryWithOutput *>(&*parsed_query))
				if (query_with_output->format)
					if (ASTIdentifier * id = dynamic_cast<ASTIdentifier *>(&*query_with_output->format))
						current_format = id->name;

			block_std_out = context.getFormatFactory().getOutput(current_format, std_out, block);
			block_std_out->writePrefix();
		}

		/// Загаловочный блок с нулем строк использовался для инициализации block_std_out,
		/// выводить его не нужно
		if (block.rows() != 0)
		{
			block_std_out->write(block);
			written_first_block = true;
		}

		std_out.next();
	}


	void onTotals(Block & block)
	{
		block_std_out->setTotals(block);
	}

	void onExtremes(Block & block)
	{
		block_std_out->setExtremes(block);
	}


	void onProgress(const Progress & progress)
	{
		rows_read_on_server += progress.rows;
		bytes_read_on_server += progress.bytes;

		writeProgress();
	}


	void writeProgress()
	{
		static size_t increment = 0;
		static const char * indicators[8] =
		{
			"\033[1;30m→\033[0m",
			"\033[1;31m↘\033[0m",
			"\033[1;32m↓\033[0m",
			"\033[1;33m↙\033[0m",
			"\033[1;34m←\033[0m",
			"\033[1;35m↖\033[0m",
			"\033[1;36m↑\033[0m",
			"\033[1;37m↗\033[0m",
		};
		
		if (is_interactive)
		{
			std::cerr << std::string(written_progress_chars, '\b');

			std::stringstream message;
			message << indicators[increment % 8]
				<< std::fixed << std::setprecision(3)
				<< " Progress: " << rows_read_on_server << " rows, " << bytes_read_on_server / 1000000.0 << " MB";

			size_t elapsed_ns = watch.elapsed();
			if (elapsed_ns)
				message << " ("
					<< rows_read_on_server * 1000000000.0 / elapsed_ns << " rows/s., "
					<< bytes_read_on_server * 1000.0 / elapsed_ns << " MB/s.) ";
			else
				message << ". ";
			
			written_progress_chars = message.str().size() - 13;
			std::cerr << message.rdbuf();
			++increment;
		}
	}


	void writeFinalProgress()
	{
		std::cout << "Processed " << rows_read_on_server << " rows, " << bytes_read_on_server / 1000000.0 << " MB";

		size_t elapsed_ns = watch.elapsed();
		if (elapsed_ns)
			std::cout << " ("
				<< rows_read_on_server * 1000000000.0 / elapsed_ns << " rows/s., "
				<< bytes_read_on_server * 1000.0 / elapsed_ns << " MB/s.) ";
		else
			std::cout << ". ";
	}


	void onException(const Exception & e)
	{
		std::cerr << "Received exception from server:" << std::endl
			<< "Code: " << e.code() << ". " << e.displayText();
	}
	
	
	void onProfileInfo(const BlockStreamProfileInfo & profile_info)
	{
		if (profile_info.hasAppliedLimit() && block_std_out)
			block_std_out->setRowsBeforeLimit(profile_info.getRowsBeforeLimit());
	}


	void onEndOfStream()
	{
		if (block_std_in)
			block_std_in->readSuffix();

		if (block_std_out)
			block_std_out->writeSuffix();

		std_out.next();

		if (is_interactive && !written_first_block)
			std::cout << "Ok." << std::endl;
	}
	

	void defineOptions(Poco::Util::OptionSet & options)
	{
		Poco::Util::Application::defineOptions(options);

		options.addOption(
			Poco::Util::Option("config-file", "c")
				.required(false)
				.repeatable(false)
				.argument("<file>")
				.binding("config-file"));

		options.addOption(
			Poco::Util::Option("host", "h")
				.required(false)
				.repeatable(false)
				.argument("<host>")
				.binding("host"));

		options.addOption(
			Poco::Util::Option("port", "")
				.required(false)
				.repeatable(false)
				.argument("<number>")
				.binding("port"));

		options.addOption(
			Poco::Util::Option("user", "u")
				.required(false)
				.repeatable(false)
				.argument("<number>")
				.binding("user"));

		options.addOption(
			Poco::Util::Option("password", "")
				.required(false)
				.repeatable(false)
				.argument("<number>")
				.binding("password"));

		options.addOption(
			Poco::Util::Option("query", "e")
				.required(false)
				.repeatable(false)
				.argument("<string>")
				.binding("query"));

		options.addOption(
			Poco::Util::Option("database", "d")
				.required(false)
				.repeatable(false)
				.argument("<string>")
				.binding("database"));
		
		options.addOption(
			Poco::Util::Option("multiline", "m")
				.required(false)
				.repeatable(false)
				.binding("multiline"));
	}
};

}


int main(int argc, char ** argv)
{
	DB::Client client;
	client.init(argc, argv);
	return client.run();
}
