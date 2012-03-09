#include <Yandex/Revision.h>

#include <statdaemons/Stopwatch.h>

#include <DB/Core/ErrorCodes.h>
#include <DB/Core/Protocol.h>

#include <DB/IO/ReadBufferFromPocoSocket.h>
#include <DB/IO/WriteBufferFromPocoSocket.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/IO/copyData.h>

#include <DB/Interpreters/executeQuery.h>

#include "TCPHandler.h"


namespace DB
{


static void sendHello(WriteBuffer & out)
{
	writeVarUInt(Protocol::Server::Hello, out);
	writeStringBinary(DBMS_NAME, out);
	writeVarUInt(DBMS_VERSION_MAJOR, out);
	writeVarUInt(DBMS_VERSION_MINOR, out);
	writeVarUInt(Revision::get(), out);
	out.next();
}


/// Считывает запрос из данных, имеющих вид последовательности блоков (размер, флаг конца, кусок данных).
class QueryReader : public DB::ReadBuffer
{
protected:
	DB::ReadBuffer & in;
	bool all_read;
	size_t read_in_block;
	size_t block_size;

	bool nextImpl()
	{
		/// Если прочитали ещё не весь блок - получим следующие данные. Если следующих данных нет - ошибка.
		if (read_in_block < block_size)
		{
			if (!in.next())
				throw Exception("Cannot read all query", ErrorCodes::CANNOT_READ_ALL_QUERY);

			working_buffer = in.buffer();
			if (block_size - read_in_block < working_buffer.size())
			{
				working_buffer.resize(block_size - read_in_block);
				read_in_block = block_size;
			}
			else
				read_in_block += working_buffer.size();
		}
		else
		{
			if (all_read)
				return false;
			
			/// Размер блока.
			readVarUInt(block_size, in);
			/// Флаг конца.
			readIntBinary(all_read, in);

			read_in_block = std::min(block_size, in.buffer().size() - in.offset());
			working_buffer = Buffer(in.position(), in.position() + read_in_block);
			in.position() += read_in_block;
		}
		
		return true;
	}

public:
	QueryReader(ReadBuffer & in_) : ReadBuffer(NULL, 0), in(in_), all_read(false), read_in_block(0), block_size(0) {}
};


void TCPHandler::runImpl()
{
	ReadBufferFromPocoSocket in(socket());
	WriteBufferFromPocoSocket out(socket());
	
	/// Сразу после соединения, отправляем hello-пакет.
	sendHello(out);

	//while (1)
	{
		/// Считываем заголовок запроса: идентификатор запроса и информацию, до какой стадии выполнять запрос.
		UInt64 query_id = 0;
		UInt64 query_processing_stage = Protocol::QueryProcessingStage::Complete;
		readIntBinary(query_id, in);
		readVarUInt(query_processing_stage, in);

		LOG_DEBUG(log, "Query ID: " << query_id);

		QueryReader query_reader(in);

		BlockInputStreamPtr query_plan;
		Context context = server.global_context;

		Stopwatch watch;
		executeQuery(query_reader, out, context, query_plan);
		watch.stop();
	}
}


void TCPHandler::run()
{
	try
	{
		runImpl();

		LOG_INFO(log, "Done processing connection.");
	}
	catch (Poco::Exception & e)
	{
		std::stringstream s;
		s << "Code: " << ErrorCodes::POCO_EXCEPTION << ", e.code() = " << e.code()
			<< ", e.message() = " << e.message() << ", e.what() = " << e.what();
		LOG_ERROR(log, s.str());
	}
	catch (std::exception & e)
	{
		std::stringstream s;
		s << "Code: " << ErrorCodes::STD_EXCEPTION << ". " << e.what();
		LOG_ERROR(log, s.str());
	}
	catch (...)
	{
		std::stringstream s;
		s << "Code: " << ErrorCodes::UNKNOWN_EXCEPTION << ". Unknown exception.";
		LOG_ERROR(log, s.str());
	}
}


}
