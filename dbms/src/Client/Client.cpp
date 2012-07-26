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

#include <boost/thread/thread.hpp>
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
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>

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
  * В один момент времени используйте только один экземпляр этого класса.
  */
class InterruptListener
{
public:
	InterruptListener() : is_interrupted(false)
	{
		sigset_t sig_set;
		if (sigemptyset(&sig_set)
			|| sigaddset(&sig_set, SIGINT)
			|| pthread_sigmask(SIG_BLOCK, &sig_set, NULL))
			throwFromErrno("Cannot block signal.", ErrorCodes::CANNOT_BLOCK_SIGNAL);

		// TODO: скорее всего, эта штука не уничтожается вовремя.
		boost::thread waiting_thread(&InterruptListener::wait, this);
	}

	~InterruptListener()
	{
		sigset_t sig_set;
		if (sigemptyset(&sig_set)
			|| sigaddset(&sig_set, SIGINT)
			|| pthread_sigmask(SIG_UNBLOCK, &sig_set, NULL))
			throwFromErrno("Cannot unblock signal.", ErrorCodes::CANNOT_UNBLOCK_SIGNAL);
	}

	bool check() { return is_interrupted; }

private:
	bool is_interrupted;

	void wait()
	{
		sigset_t sig_set;
		int sig = 0;

		if (sigemptyset(&sig_set)
			|| sigaddset(&sig_set, SIGINT)
			|| sigwait(&sig_set, &sig))
		{
			std::cerr << "Cannot wait for signal." << std::endl;
			return;
		}

		is_interrupted = true;
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
	bool expect_result;		/// Запрос предполагает получение результата.

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
			("q")("й");

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
		catch (const DB::Exception & e)
		{
 			std::cerr << "Code: " << e.code() << ". " << e.displayText() << std::endl
				<< std::endl
				<< "Stack trace:" << std::endl
				<< e.getStackTrace().toString();
			return 1;
		}
		catch (const Poco::Exception & e)
		{
			std::cerr << "Poco::Exception: " << e.displayText() << std::endl;
			return 1;
		}
		catch (const std::exception & e)
		{
			std::cerr << "std::exception: " << e.what() << std::endl;
			return 1;
		}
		catch (...)
		{
			std::cerr << "Unknown exception" << std::endl;
			return 1;
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

		context.format_factory = new FormatFactory();
		context.data_type_factory = new DataTypeFactory();

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

			loop();

			std::cout << "Bye." << std::endl;
		}
		else
			nonInteractive();

		return 0;
	}


	void connect()
	{
		String host = config().getString("host", "localhost");
		UInt16 port = config().getInt("port", DBMS_DEFAULT_PORT);
		String default_database = config().getString("database", "");
		
		Protocol::Compression::Enum compression = config().getBool("compression", true)
			? Protocol::Compression::Enable
			: Protocol::Compression::Disable;

		if (is_interactive)
			std::cout << "Connecting to " << (!default_database.empty() ? default_database + "@" : "") << host << ":" << port << "." << std::endl;

		connection = new Connection(host, port, default_database, *context.data_type_factory, "client", compression,
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


	void loop()
	{
		while (char * line_ = readline(":) "))
		{
			String line(line_);
			free(line_);

			if (line.empty())
				continue;

			if (!process(line))
				break;
			
			add_history(line.c_str());

			int res = append_history(1, history_file.c_str());
			if (res)
				throwFromErrno("Cannot append history to file " + history_file, ErrorCodes::CANNOT_APPEND_HISTORY);
		}
	}


	void nonInteractive()
	{
		if (config().has("query"))
			process(config().getString("query"));
		else
		{
			// TODO
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
			std::cout << std::endl
				<< processed_rows << " rows in set. Elapsed: " << watch.elapsedSeconds() << " sec."
				<< std::endl << std::endl;

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

		if ((is_interactive && !parsed_insert_query.data) || (!is_interactive && std_in.eof()))
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
			std::cerr << "Syntax error: failed at position "
				<< (pos - begin) << ": "
				<< std::string(pos, std::min(SHOW_CHARS_ON_SYNTAX_ERROR, end - pos))
				<< ", expected " << (parse_res ? "end of query" : expected) << "."
				<< std::endl << std::endl;

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
		else if (!is_interactive && !std_in.eof())
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

		block_std_in = context.format_factory->getInput(current_format, buf, sample, insert_format_max_block_size, *context.data_type_factory);
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

			case Protocol::Server::Exception:
				onException(*packet.exception);
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
					+ String(Protocol::Server::toString(Protocol::Server::Enum(packet.type))) + ")", ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER);
		}
	}


	void onData(Block & block)
	{
		if (written_progress_chars)
		{
			if (written_first_block)
				for (size_t i = 0; i < written_progress_chars; ++i)
					std::cerr << "\b \b";
			else
				std::cerr << "\n\n";

			written_progress_chars = 0;
		}

		written_first_block = true;

		if (block)
		{
			processed_rows += block.rows();
			if (!block_std_out)
			{
				String current_format = format;

				/// Формат может быть указан в SELECT запросе.
				if (ASTSelectQuery * select = dynamic_cast<ASTSelectQuery *>(&*parsed_query))
					if (select->format)
						if (ASTIdentifier * id = dynamic_cast<ASTIdentifier *>(&*select->format))
							current_format = id->name;
				
				block_std_out = context.format_factory->getOutput(current_format, std_out, block);
				block_std_out->writePrefix();
			}
			
			block_std_out->write(block);
			std_out.next();
		}
		else
		{
			if (block_std_out)
				block_std_out->writeSuffix();
			
			std_out.next();
		}
	}


	void onProgress(const Progress & progress)
	{
		rows_read_on_server += progress.rows;
		bytes_read_on_server += progress.bytes;

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
					<< rows_read_on_server * 1000000000 / elapsed_ns << " rows/s., "
					<< bytes_read_on_server * 1000.0 / elapsed_ns << " MB/s.) ";
			else
				message << ". ";
			
			written_progress_chars = message.str().size() - 13;
			std::cerr << message.rdbuf();
			++increment;
		}
	}


	void onException(const Exception & e)
	{
		std::cerr << "Received exception from server:" << std::endl
			<< "Code: " << e.code() << ". " << e.displayText();
	}


	void onEndOfStream()
	{
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
			Poco::Util::Option("port", "p")
				.required(false)
				.repeatable(false)
				.argument("<number>")
				.binding("port"));

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
	}
};

}


int main(int argc, char ** argv)
{
	DB::Client client;
	client.init(argc, argv);
	return client.run();
}
