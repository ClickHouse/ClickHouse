#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>

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
#include <Poco/Net/StreamSocket.h>

#include <Yandex/Revision.h>

#include <statdaemons/Stopwatch.h>

#include <DB/Core/Exception.h>
#include <DB/Core/Types.h>
#include <DB/Core/Protocol.h>

#include <DB/IO/ReadBufferFromPocoSocket.h>
#include <DB/IO/WriteBufferFromPocoSocket.h>
#include <DB/IO/CompressedReadBuffer.h>
#include <DB/IO/CompressedWriteBuffer.h>
#include <DB/IO/ChunkedReadBuffer.h>
#include <DB/IO/ChunkedWriteBuffer.h>
#include <DB/IO/ReadBufferFromFileDescriptor.h>
#include <DB/IO/WriteBufferFromFileDescriptor.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>

#include <DB/Parsers/ParserQuery.h>
#include <DB/Parsers/formatAST.h>

#include <DB/Interpreters/Context.h>


/** Клиент командной строки СУБД ClickHouse.
  */


namespace DB
{

using Poco::SharedPtr;


class Client : public Poco::Util::Application
{
public:
	Client() : is_interactive(true), stdin_is_not_tty(false), socket(), in(socket), out(socket), query_id(0), compression(Protocol::Compression::Enable),
		format_max_block_size(0), std_in(STDIN_FILENO), std_out(STDOUT_FILENO), received_rows(0) {}
	
private:
	typedef std::tr1::unordered_set<String> StringSet;
	StringSet exit_strings;

	bool is_interactive;				/// Использовать readline интерфейс или batch режим.
	bool stdin_is_not_tty;				/// stdin - не терминал.

	Poco::Net::StreamSocket socket;
	ReadBufferFromPocoSocket in;
	WriteBufferFromPocoSocket out;
	String query;						/// Текущий запрос.
	UInt64 query_id;					/// Идентификатор запроса. Его можно использовать, чтобы отменить запрос.

	UInt64 compression;					/// Сжимать ли данные при взаимодействии с сервером.
	String in_format;					/// Формат передачи данных (INSERT-а) на сервер.
	String out_format;					/// Формат приёма данных (результата) от сервера.
	String format;						/// Формат вывода результата в консоль.
	size_t format_max_block_size;		/// Максимальный размер блока при выводе в консоль.
	String insert_format;				/// Формат данных для INSERT-а при чтении их из stdin в batch режиме
	size_t insert_format_max_block_size; /// Максимальный размер блока при чтении данных INSERT-а.

	Context context;
	Block empty_block;

	/// Откуда читать результат выполнения запроса.
	SharedPtr<ReadBuffer> chunked_in;
	SharedPtr<ReadBuffer> maybe_compressed_in;
	BlockInputStreamPtr block_in;

	/// Куда писать данные INSERT-а.
	SharedPtr<WriteBuffer> chunked_out;
	SharedPtr<WriteBuffer> maybe_compressed_out;
	BlockOutputStreamPtr block_out;

	/// Чтение из stdin для batch режима
	ReadBufferFromFileDescriptor std_in;
	BlockInputStreamPtr block_std_in;

	/// Вывод в консоль
	WriteBufferFromFileDescriptor std_out;
	BlockOutputStreamPtr block_std_out;

	String home_path;
	
	/// Путь к файлу истории команд.
	String history_file;

	/// Строк прочитано.
	size_t received_rows;

	/// Распарсенный запрос. Оттуда берутся некоторые настройки (формат).
	ASTPtr parsed_query;
	bool expect_result;		/// Запрос предполагает получение результата.
	

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


	void throwFromErrno(const std::string & s, int code)
	{
		char buf[128];
		throw Exception(s + ", errno: " + Poco::NumberFormatter::format(errno) + ", strerror: " + std::string(strerror_r(errno, buf, sizeof(buf))), code);
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

		compression = config().getBool("compression", true) ? Protocol::Compression::Enable : Protocol::Compression::Disable;
		in_format 	= config().getString("in_format", 	"Native");
		out_format 	= config().getString("out_format", 	"Native");
		format 		= config().getString("format", is_interactive ? "PrettyCompact" : "TabSeparated");
		format_max_block_size = config().getInt("format_max_block_size", DEFAULT_BLOCK_SIZE);

		String host = config().getString("host", "localhost");
		UInt16 port = config().getInt("port", 9000);

		if (is_interactive)
			std::cout << "Connecting to " << host << ":" << port << "." << std::endl;

		socket.connect(Poco::Net::SocketAddress(host, port));

		/// Получить hello пакет.
		UInt64 packet_type = 0;
		String server_name;
		UInt64 server_version_major = 0;
		UInt64 server_version_minor = 0;
		UInt64 server_revision = 0;
		
		readVarUInt(packet_type, in);
		if (packet_type != Protocol::Server::Hello)
			throw Exception("Unexpected packet from server", ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER);
		
		readStringBinary(server_name, in);
		readVarUInt(server_version_major, in);
		readVarUInt(server_version_minor, in);
		readVarUInt(server_revision, in);

		if (is_interactive)
			std::cout << "Connected to " << server_name
				<< " server version " << server_version_major
				<< "." << server_version_minor
				<< "." << server_revision
				<< "." << std::endl << std::endl;

		context.format_factory = new FormatFactory();
		context.data_type_factory = new DataTypeFactory();

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

		Stopwatch watch;

		query = line;

		/// Некоторые части запроса выполняются на стороне клиента (форматирование результата). Поэтому, распарсим запрос.
		if (!parseQuery())
			return true;
		
		sendQuery();
		sendData();
		receiveResult();

		if (is_interactive)
			std::cout << std::endl
				<< received_rows << " rows in set. Elapsed: " << watch.elapsedSeconds() << " sec."
				<< std::endl << std::endl;

		block_in = NULL;
		maybe_compressed_in = NULL;
		chunked_in = NULL;

		return true;
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


	void sendQuery()
	{
		UInt64 stage = Protocol::QueryProcessingStage::Complete;

		writeVarInt(Protocol::Client::Query, out);
		writeIntBinary(query_id, out);
		writeVarInt(stage, out);
		writeVarInt(compression, out);
		writeStringBinary(in_format, out);
		writeStringBinary(out_format, out);

		writeStringBinary(query, out);
		out.next();
	}


	void sendData()
	{
		/// Если нужно отправить данные INSERT-а.
		const ASTInsertQuery * parsed_insert_query = dynamic_cast<const ASTInsertQuery *>(&*parsed_query);
		if (!parsed_insert_query)
			return;
		
		if (parsed_insert_query->data)
		{
			/// Отправляем данные из запроса.
			ReadBuffer data_in(const_cast<char *>(parsed_insert_query->data), parsed_insert_query->end - parsed_insert_query->data);
			sendDataFrom(data_in);
		}
		else if (!is_interactive)
		{
			/// Отправляем данные из stdin.
			sendDataFrom(std_in);
		}
		else
			throw Exception("No data to insert", ErrorCodes::NO_DATA_TO_INSERT);
	}


	void sendDataFrom(ReadBuffer & buf)
	{
/*		if (!block_out)
		{
			chunked_out = new ChunkedWriteBuffer(out, query_id);
			maybe_compressed_out = compression == Protocol::Compression::Enable
				? new CompressedWriteBuffer(*chunked_out)
				: chunked_out;

			block_out = context.format_factory->getOutput(
				in_format,
				*maybe_compressed_out,
				empty_block,
				insert_format_max_block_size,
				*context.data_type_factory);
		}

		/// Прочитать из сети один блок и вывести его в консоль
		Block block = block_in->read();
		if (block)
		{
			received_rows += block.rows();
			if (!block_std_out)
			{
				String current_format = format;

				/// Формат может быть указан в SELECT запросе.
				if (ASTSelectQuery * select = dynamic_cast<ASTSelectQuery *>(&*parsed_query))
					if (select->format)
						if (ASTIdentifier * id = dynamic_cast<ASTIdentifier *>(&*select->format))
							current_format = id->name;

				block_std_out = context.format_factory->getOutput(current_format, std_out, block);
			}

			block_std_out->write(block);
			std_out.next();
			return true;
		}
		else
			return false;*/
	}


	void receiveResult()
	{
		received_rows = 0;
		while (receivePacket())
			;

		block_std_out = NULL;
	}


	/// Получить кусок результата или прогресс выполнения или эксепшен.
	bool receivePacket()
	{
		UInt64 packet_type = 0;
		readVarUInt(packet_type, in);

		switch (packet_type)
		{
			case Protocol::Server::Data:
				return receiveData();

			case Protocol::Server::Exception:
				receiveException();
				return false;

			case Protocol::Server::Ok:
				receiveOk();
				return false;

			default:
				throw Exception("Unknown packet from server", ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);
		}
	}


	bool receiveData()
	{
		if (!block_in)
		{
			chunked_in = new ChunkedReadBuffer(in, query_id);
			maybe_compressed_in = compression == Protocol::Compression::Enable
				? new CompressedReadBuffer(*chunked_in)
				: chunked_in;

			block_in = context.format_factory->getInput(
				out_format,
				*maybe_compressed_in,
				empty_block,
				format_max_block_size,
				*context.data_type_factory);
		}

		/// Прочитать из сети один блок и вывести его в консоль
		Block block = block_in->read();
		if (block)
		{
			received_rows += block.rows();
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
			return true;
		}
		else
		{
			if (block_std_out)
				block_std_out->writeSuffix();
			
			std_out.next();
			return false;
		}
	}


	void receiveException()
	{
		Exception e;
		readException(e, in);

		std::cerr << "Received exception from server:" << std::endl
			<< "Code: " << e.code() << ". " << e.displayText();
	}


	void receiveOk()
	{
		if (is_interactive)
			std::cout << "Ok." << std::endl;
	}
	

	void defineOptions(Poco::Util::OptionSet & options)
	{
		Poco::Util::Application::defineOptions(options);

		options.addOption(
			Poco::Util::Option("config-file", "C")
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
	}
};

}


int main(int argc, char ** argv)
{
	DB::Client client;
	client.init(argc, argv);
	return client.run();
}
