#include <stdlib.h>

#include <readline/readline.h>
#include <readline/history.h>

#include <iostream>

#include <tr1/unordered_set>

#include <boost/assign/list_inserter.hpp>

#include <Poco/File.h>
#include <Poco/SharedPtr.h>
#include <Poco/Util/Application.h>
#include <Poco/Net/StreamSocket.h>

#include <Yandex/Revision.h>

#include <DB/Core/Exception.h>
#include <DB/Core/Types.h>
#include <DB/Core/Protocol.h>

#include <DB/IO/ReadBufferFromPocoSocket.h>
#include <DB/IO/WriteBufferFromPocoSocket.h>
#include <DB/IO/CompressedReadBuffer.h>
#include <DB/IO/CompressedWriteBuffer.h>
#include <DB/IO/ChunkedReadBuffer.h>
#include <DB/IO/ChunkedWriteBuffer.h>
#include <DB/IO/WriteBufferFromFileDescriptor.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>

#include <DB/Interpreters/Context.h>


/** Клиент командной строки СУБД ClickHouse.
  */


namespace DB
{

using Poco::SharedPtr;


class Client : public Poco::Util::Application
{
public:
	Client() : socket(), in(socket), out(socket), query_id(0), compression(Protocol::Compression::Enable), format_max_block_size(0), std_out(STDOUT_FILENO) {}
	
private:
	typedef std::tr1::unordered_set<String> StringSet;
	StringSet exit_strings;

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

	/// Вывод в консоль
	WriteBufferFromFileDescriptor std_out;
	BlockOutputStreamPtr block_std_out;
	

	void initialize(Poco::Util::Application & self)
	{
		Poco::Util::Application::initialize(self);

		boost::assign::insert(exit_strings)
			("exit")("quit")("logout")
			("учше")("йгше")("дщпщге")
			("exit;")("quit;")("logout;")
			("учше;")("йгше;")("дщпщге;")
			("q")("й");

		if (config().has("config-file"))
			loadConfiguration(config().getString("config-file"));
		else if (Poco::File("./clickhouse-client.xml").exists())
			loadConfiguration("./clickhouse-client.xml");
		else if (Poco::File("~/.clickhouse-client/config.xml").exists())
			loadConfiguration("~/.clickhouse-client/config.xml");
		else if (Poco::File("/etc/clickhouse-client/config.xml").exists())
			loadConfiguration("/etc/clickhouse-client/config.xml");
	}
	

	int main(const std::vector<std::string> & args)
	{
		std::cout << "ClickHouse client version " << DBMS_VERSION_MAJOR
			<< "." << DBMS_VERSION_MINOR
			<< "." << Revision::get()
			<< "." << std::endl;

		compression = config().getBool("compression", true) ? Protocol::Compression::Enable : Protocol::Compression::Disable;
		in_format 	= config().getString("in_format", 	"Native");
		out_format 	= config().getString("out_format", 	"Native");
		format 		= config().getString("format", 		"Pretty");
		format_max_block_size = config().getInt("format_max_block_size", DEFAULT_BLOCK_SIZE);

		String host = config().getString("host", "localhost");
		UInt16 port = config().getInt("port", 9000);
		
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

		std::cout << "Connected to " << server_name
			<< " server version " << server_version_major
			<< "." << server_version_minor
			<< "." << server_revision
			<< "." << std::endl;

		context.format_factory = new FormatFactory();
		context.data_type_factory = new DataTypeFactory();
		
		loop();

		std::cout << "Bye." << std::endl;
		
		return 0;
	}


	void loop()
	{
		while (char * line_ = readline(":) "))
		{
			String line(line_);
			free(line_);
			if (!process(line))
				break;
			add_history(line.c_str());
		}
	}


	bool process(const String & line)
	{
		if (line.empty())
			return true;
		
		if (exit_strings.end() != exit_strings.find(line))
			return false;

		query = line;
		sendQuery();
		receiveResult();

		block_in = NULL;
		maybe_compressed_in = NULL;
		chunked_in = NULL;

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


	void receiveResult()
	{
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
			if (!block_std_out)
				block_std_out = context.format_factory->getOutput(format, std_out, block);
			
			block_std_out->write(block);
			std_out.next();
			return true;
		}
		else
			return false;
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
	}
};

}


int main(int argc, char ** argv)
{
	try
	{
		DB::Client client;
		client.init(argc, argv);
		client.run();
	}
	catch (const DB::Exception & e)
	{
		std::cerr << "DB::Exception: " << e.what() << ", " << e.message() << std::endl
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

	return 0;
}
