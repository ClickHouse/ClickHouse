#include <iomanip>

#include <Poco/Net/NetException.h>

#include <common/Revision.h>

#include <DB/Common/Stopwatch.h>

#include <DB/Core/ErrorCodes.h>
#include <DB/Core/Progress.h>

#include <DB/IO/CompressedReadBuffer.h>
#include <DB/IO/CompressedWriteBuffer.h>
#include <DB/IO/ReadBufferFromPocoSocket.h>
#include <DB/IO/WriteBufferFromPocoSocket.h>

#include <DB/IO/copyData.h>

#include <DB/DataStreams/AsynchronousBlockInputStream.h>
#include <DB/DataStreams/NativeBlockInputStream.h>
#include <DB/DataStreams/NativeBlockOutputStream.h>
#include <DB/Interpreters/executeQuery.h>
#include <DB/Interpreters/Quota.h>

#include <DB/Storages/StorageMemory.h>

#include <DB/Common/ExternalTable.h>

#include "TCPHandler.h"

#include <DB/Common/NetException.h>

namespace DB
{


void TCPHandler::runImpl()
{
	connection_context = *server.global_context;
	connection_context.setSessionContext(connection_context);

	Settings global_settings = server.global_context->getSettings();

	socket().setReceiveTimeout(global_settings.receive_timeout);
	socket().setSendTimeout(global_settings.send_timeout);
	socket().setNoDelay(true);

	in = new ReadBufferFromPocoSocket(socket());
	out = new WriteBufferFromPocoSocket(socket());

	try
	{
		receiveHello();
	}
	catch (const Exception & e)	/// Типично при неправильном имени пользователя, пароле, адресе.
	{
		if (e.code() == ErrorCodes::CLIENT_HAS_CONNECTED_TO_WRONG_PORT)
		{
			LOG_DEBUG(log, "Client has connected to wrong port.");
			return;
		}

		try
		{
			/// Пытаемся отправить информацию об ошибке клиенту.
			sendException(e);
		}
		catch (...) {}

		throw;
	}

	/// При соединении может быть указана БД по умолчанию.
	if (!default_database.empty())
	{
		if (!connection_context.isDatabaseExist(default_database))
		{
			Exception e("Database " + default_database + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);
			LOG_ERROR(log, "Code: " << e.code() << ", e.displayText() = " << e.displayText()
				<< ", Stack trace:\n\n" << e.getStackTrace().toString());
			sendException(e);
			return;
		}

		connection_context.setCurrentDatabase(default_database);
	}

	sendHello();

	connection_context.setProgressCallback([this] (const Progress & value) { return this->updateProgress(value); });

	while (1)
	{
		/// Ждём пакета от клиента. При этом, каждые POLL_INTERVAL сек. проверяем, не требуется ли завершить работу.
		while (!static_cast<ReadBufferFromPocoSocket &>(*in).poll(global_settings.poll_interval * 1000000) && !Daemon::instance().isCancelled())
			;

		/// Если требуется завершить работу, или клиент отсоединился.
		if (Daemon::instance().isCancelled() || in->eof())
			break;

		Stopwatch watch;
		state.reset();

		/** Исключение во время выполнения запроса (его надо отдать по сети клиенту).
		  * Клиент сможет его принять, если оно не произошло во время отправки другого пакета и клиент ещё не разорвал соединение.
		  */
		SharedPtr<Exception> exception;

		try
		{
			/// Восстанавливаем контекст запроса.
			query_context = connection_context;
			query_context.setInterface(Context::Interface::TCP);

			/** Если Query - обрабатываем. Если Ping или Cancel - возвращаемся в начало.
			  * Могут прийти настройки на отдельный запрос, которые модифицируют query_context.
			  */
			if (!receivePacket())
				continue;

			/// Получить блоки временных таблиц
			if (client_revision >= DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES)
				readData(global_settings);

			/// Очищаем, так как, получая данные внешних таблиц, мы получили пустой блок.
			/// А значит, stream помечен как cancelled и читать из него нельзя.
			state.block_in = nullptr;
			state.maybe_compressed_in = nullptr;	/// Для более корректного учёта MemoryTracker-ом.

			/// Обрабатываем Query
			state.io = executeQuery(state.query, query_context, false, state.stage);

			if (state.io.out)
				state.need_receive_data_for_insert = true;

			after_check_cancelled.restart();
			after_send_progress.restart();

			/// Запрос требует приёма данных от клиента?
			if (state.need_receive_data_for_insert)
				processInsertQuery(global_settings);
			else
				processOrdinaryQuery();

			sendEndOfStream();

			state.reset();
		}
		catch (const Exception & e)
		{
			state.io.onException();
			exception = e.clone();

			if (e.code() == ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT)
				throw;
		}
		catch (const Poco::Net::NetException & e)
		{
			/** Сюда мы можем попадать, если была ошибка в соединении с клиентом,
			  *  или в соединении с удалённым сервером, который использовался для обработки запроса.
			  * Здесь не получается отличить эти два случая.
			  * Хотя в одном из них, мы должны отправить эксепшен клиенту, а в другом - не можем.
			  * Будем пытаться отправить эксепшен клиенту в любом случае - см. ниже.
			  */
			state.io.onException();
			exception = new Exception(e.displayText(), ErrorCodes::POCO_EXCEPTION);
		}
		catch (const Poco::Exception & e)
		{
			state.io.onException();
			exception = new Exception(e.displayText(), ErrorCodes::POCO_EXCEPTION);
		}
		catch (const std::exception & e)
		{
			state.io.onException();
			exception = new Exception(e.what(), ErrorCodes::STD_EXCEPTION);
		}
		catch (...)
		{
			state.io.onException();
			exception = new Exception("Unknown exception", ErrorCodes::UNKNOWN_EXCEPTION);
		}

		bool network_error = false;

		try
		{
			if (exception)
				sendException(*exception);
		}
		catch (...)
		{
			/** Не удалось отправить информацию об эксепшене клиенту. */
			network_error = true;
			LOG_WARNING(log, "Client has gone away.");
		}

		try
		{
			state.reset();
		}
		catch (...)
		{
			/** В процессе обработки запроса было исключение, которое мы поймали и, возможно, отправили клиенту.
			  * При уничтожении конвейера выполнения запроса, было второе исключение.
			  * Например, конвейер мог выполняться в нескольких потоках, и в каждом из них могло возникнуть исключение.
			  * Проигнорируем его.
			  */
		}

		watch.stop();

		LOG_INFO(log, std::fixed << std::setprecision(3)
			<< "Processed in " << watch.elapsedSeconds() << " sec.");

		if (network_error)
			break;
	}
}


void TCPHandler::readData(const Settings & global_settings)
{
	while (1)
	{
		Stopwatch watch(CLOCK_MONOTONIC_COARSE);

		/// Ждём пакета от клиента. При этом, каждые POLL_INTERVAL сек. проверяем, не требуется ли завершить работу.
		while (1)
		{
			if (static_cast<ReadBufferFromPocoSocket &>(*in).poll(global_settings.poll_interval * 1000000))
				break;

			/// Если требуется завершить работу.
			if (Daemon::instance().isCancelled())
				return;

			/** Если ждём данных уже слишком долго.
			  * Если периодически poll-ить соединение, то receive_timeout у сокета сам по себе не срабатывает.
			  * Поэтому, добавлена дополнительная проверка.
			  */
			if (watch.elapsedSeconds() > global_settings.receive_timeout.totalSeconds())
				throw Exception("Timeout exceeded while receiving data from client", ErrorCodes::SOCKET_TIMEOUT);
		}

		/// Если клиент отсоединился.
		if (in->eof())
			return;

		/// Принимаем и обрабатываем данные. А если они закончились, то выходим.
		if (!receivePacket())
			break;
	}
}


void TCPHandler::processInsertQuery(const Settings & global_settings)
{
	/** Сделано выше остальных строк, чтобы в случае, когда функция writePrefix кидает эксепшен,
	  *  клиент получил эксепшен до того, как начнёт отправлять данные.
	  */
	state.io.out->writePrefix();

	/// Отправляем клиенту блок - структура таблицы.
	Block block = state.io.out_sample;
	sendData(block);

	readData(global_settings);
	state.io.out->writeSuffix();
	state.io.onFinish();
}


void TCPHandler::processOrdinaryQuery()
{
	/// Вынимаем результат выполнения запроса, если есть, и пишем его в сеть.
	if (state.io.in)
	{
		/// Отправим блок-заголовок, чтобы клиент мог подготовить формат вывода
		if (state.io.in_sample && client_revision >= DBMS_MIN_REVISION_WITH_HEADER_BLOCK)
			sendData(state.io.in_sample);

		AsynchronousBlockInputStream async_in(state.io.in);
		async_in.readPrefix();

		while (true)
		{
			Block block;

			while (true)
			{
				if (isQueryCancelled())
				{
					/// Получен пакет с просьбой прекратить выполнение запроса.
					async_in.cancel();
					break;
				}
				else
				{
					if (state.progress.rows && after_send_progress.elapsed() / 1000 >= query_context.getSettingsRef().interactive_delay)
					{
						/// Прошло некоторое время и есть прогресс.
						after_send_progress.restart();
						sendProgress();
					}

					if (async_in.poll(query_context.getSettingsRef().interactive_delay / 1000))
					{
						/// Есть следующий блок результата.
						block = async_in.read();
						break;
					}
				}
			}

			/** Если закончились данные, то отправим данные профайлинга и тотальные значения до
			  *  последнего нулевого блока, чтобы иметь возможность использовать
			  *  эту информацию в выводе суффикса output stream'а.
			  * Если запрос был прерван, то вызывать методы sendTotals и другие нельзя,
			  *  потому что мы прочитали ещё не все данные, и в это время могут производиться какие-то
			  *  вычисления в других потоках.
			  */
			if (!block && !isQueryCancelled())
			{
				sendTotals();
				sendExtremes();
				sendProfileInfo();
				sendProgress();
			}

			sendData(block);
			if (!block)
				break;
		}

		async_in.readSuffix();
	}

	state.io.onFinish();
}


void TCPHandler::sendProfileInfo()
{
	if (client_revision < DBMS_MIN_REVISION_WITH_PROFILING_PACKET)
		return;

	if (const IProfilingBlockInputStream * input = dynamic_cast<const IProfilingBlockInputStream *>(&*state.io.in))
	{
		writeVarUInt(Protocol::Server::ProfileInfo, *out);
		input->getInfo().write(*out);
		out->next();
	}
}


void TCPHandler::sendTotals()
{
	if (client_revision < DBMS_MIN_REVISION_WITH_TOTALS_EXTREMES)
		return;

	if (IProfilingBlockInputStream * input = dynamic_cast<IProfilingBlockInputStream *>(&*state.io.in))
	{
		const Block & totals = input->getTotals();

		if (totals)
		{
			initBlockOutput();

			writeVarUInt(Protocol::Server::Totals, *out);
			if (client_revision >= DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES)
				writeStringBinary("", *out);

			state.block_out->write(totals);
			state.maybe_compressed_out->next();
			out->next();
		}
	}
}


void TCPHandler::sendExtremes()
{
	if (client_revision < DBMS_MIN_REVISION_WITH_TOTALS_EXTREMES)
		return;

	if (const IProfilingBlockInputStream * input = dynamic_cast<const IProfilingBlockInputStream *>(&*state.io.in))
	{
		const Block & extremes = input->getExtremes();

		if (extremes)
		{
			initBlockOutput();

			writeVarUInt(Protocol::Server::Extremes, *out);
			if (client_revision >= DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES)
				writeStringBinary("", *out);

			state.block_out->write(extremes);
			state.maybe_compressed_out->next();
			out->next();
		}
	}
}


void TCPHandler::receiveHello()
{
	/// Получить hello пакет.
	UInt64 packet_type = 0;
	String client_name;
	UInt64 client_version_major = 0;
	UInt64 client_version_minor = 0;
	String user = "default";
	String password;

	readVarUInt(packet_type, *in);
	if (packet_type != Protocol::Client::Hello)
	{
		/** Если случайно обратились по протоколу HTTP на порт, предназначенный для внутреннего TCP-протокола,
		  *  то вместо номера пакета будет G (GET) или P (POST), в большинстве случаев.
		  */
		if (packet_type == 'G' || packet_type == 'P')
		{
			writeString("HTTP/1.0 400 Bad Request\r\n\r\n"
				"Port " + server.config().getString("tcp_port") + " is for clickhouse-client program.\r\n"
				"You must use port " + server.config().getString("http_port") + " for HTTP"
				+ (server.config().getBool("use_olap_http_server", false)
					? "\r\n or port " + server.config().getString("olap_http_port") + " for OLAPServer compatibility layer.\r\n"
					: ".\r\n"),
				*out);

			throw Exception("Client has connected to wrong port", ErrorCodes::CLIENT_HAS_CONNECTED_TO_WRONG_PORT);
		}
		else
			throw NetException("Unexpected packet from client", ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
	}

	readStringBinary(client_name, *in);
	readVarUInt(client_version_major, *in);
	readVarUInt(client_version_minor, *in);
	readVarUInt(client_revision, *in);
	readStringBinary(default_database, *in);

	if (client_revision >= DBMS_MIN_REVISION_WITH_USER_PASSWORD)
	{
		readStringBinary(user, *in);
		readStringBinary(password, *in);
	}

	LOG_DEBUG(log, "Connected " << client_name
		<< " version " << client_version_major
		<< "." << client_version_minor
		<< "." << client_revision
		<< (!default_database.empty() ? ", database: " + default_database : "")
		<< (!user.empty() ? ", user: " + user : "")
		<< ".");

	connection_context.setUser(user, password, socket().peerAddress().host(), "");
}


void TCPHandler::sendHello()
{
	writeVarUInt(Protocol::Server::Hello, *out);
	writeStringBinary(DBMS_NAME, *out);
	writeVarUInt(DBMS_VERSION_MAJOR, *out);
	writeVarUInt(DBMS_VERSION_MINOR, *out);
	writeVarUInt(Revision::get(), *out);
	out->next();
}


bool TCPHandler::receivePacket()
{
	UInt64 packet_type = 0;
	readVarUInt(packet_type, *in);

//	std::cerr << "Packet: " << packet_type << std::endl;

	switch (packet_type)
	{
		case Protocol::Client::Query:
			if (!state.empty())
				throw NetException("Unexpected packet Query received from client", ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
			receiveQuery();
			return true;

		case Protocol::Client::Data:
			if (state.empty())
				throw NetException("Unexpected packet Data received from client", ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
			return receiveData();

		case Protocol::Client::Ping:
			writeVarUInt(Protocol::Server::Pong, *out);
			out->next();
			return false;

		case Protocol::Client::Cancel:
			return false;

		case Protocol::Client::Hello:
			throw Exception("Unexpected packet " + String(Protocol::Client::toString(packet_type)) + " received from client",
				ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);

		default:
			throw Exception("Unknown packet from client", ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT);
	}
}


void TCPHandler::receiveQuery()
{
	UInt64 stage = 0;
	UInt64 compression = 0;

	state.is_empty = false;
	if (client_revision < DBMS_MIN_REVISION_WITH_STRING_QUERY_ID)
	{
		UInt64 query_id_int;
		readIntBinary(query_id_int, *in);
		state.query_id = "";
	}
	else
		readStringBinary(state.query_id, *in);

	query_context.setCurrentQueryId(state.query_id);

	/// Настройки на отдельный запрос.
	if (client_revision >= DBMS_MIN_REVISION_WITH_PER_QUERY_SETTINGS)
	{
		query_context.getSettingsRef().deserialize(*in);
	}

	readVarUInt(stage, *in);
	state.stage = QueryProcessingStage::Enum(stage);

	readVarUInt(compression, *in);
	state.compression = Protocol::Compression::Enum(compression);

	readStringBinary(state.query, *in);
}


bool TCPHandler::receiveData()
{
	initBlockInput();

	/// Имя временной таблицы для записи данных, по умолчанию пустая строка
	String external_table_name;
	if (client_revision >= DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES)
		readStringBinary(external_table_name, *in);

	/// Прочитать из сети один блок и записать его
	Block block = state.block_in->read();

	if (block)
	{
		/// Если запрос на вставку, то данные нужно писать напрямую в state.io.out.
		/// Иначе пишем блоки во временную таблицу external_table_name.
		if (!state.need_receive_data_for_insert)
		{
			StoragePtr storage;
			/// Если такой таблицы не существовало, создаем ее.
			if (!(storage = query_context.tryGetExternalTable(external_table_name)))
			{
				NamesAndTypesListPtr columns = new NamesAndTypesList(block.getColumnsList());
				storage = StorageMemory::create(external_table_name, columns);
				query_context.addExternalTable(external_table_name, storage);
			}
			/// Данные будем писать напрямую в таблицу.
			state.io.out = storage->write(ASTPtr(), query_context.getSettingsRef());
		}
		if (block)
			state.io.out->write(block);
		return true;
	}
	else
		return false;
}


void TCPHandler::initBlockInput()
{
	if (!state.block_in)
	{
		if (state.compression == Protocol::Compression::Enable)
			state.maybe_compressed_in = new CompressedReadBuffer(*in);
		else
			state.maybe_compressed_in = in;

		state.block_in = new NativeBlockInputStream(
			*state.maybe_compressed_in,
			client_revision);
	}
}


void TCPHandler::initBlockOutput()
{
	if (!state.block_out)
	{
		if (state.compression == Protocol::Compression::Enable)
			state.maybe_compressed_out = new CompressedWriteBuffer(*out, query_context.getSettings().network_compression_method);
		else
			state.maybe_compressed_out = out;

		state.block_out = new NativeBlockOutputStream(
			*state.maybe_compressed_out,
			client_revision);
	}
}


bool TCPHandler::isQueryCancelled()
{
	if (state.is_cancelled || state.sent_all_data)
		return true;

	if (after_check_cancelled.elapsed() / 1000 < query_context.getSettingsRef().interactive_delay)
		return false;

	after_check_cancelled.restart();

	/// Во время выполнения запроса, единственный пакет, который может прийти от клиента - это остановка выполнения запроса.
	if (static_cast<ReadBufferFromPocoSocket &>(*in).poll(0))
	{
		UInt64 packet_type = 0;
		readVarUInt(packet_type, *in);

		switch (packet_type)
		{
			case Protocol::Client::Cancel:
				if (state.empty())
					throw NetException("Unexpected packet Cancel received from client", ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
				LOG_INFO(log, "Query was cancelled.");
				state.is_cancelled = true;
				return true;

			default:
				throw NetException("Unknown packet from client", ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT);
		}
	}

	return false;
}


void TCPHandler::sendData(Block & block)
{
	initBlockOutput();

	writeVarUInt(Protocol::Server::Data, *out);
	if (client_revision >= DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES)
		writeStringBinary("", *out);

	state.block_out->write(block);
	state.maybe_compressed_out->next();
	out->next();
}


void TCPHandler::sendException(const Exception & e)
{
	writeVarUInt(Protocol::Server::Exception, *out);
	writeException(e, *out);
	out->next();
}


void TCPHandler::sendEndOfStream()
{
	state.sent_all_data = true;
	writeVarUInt(Protocol::Server::EndOfStream, *out);
	out->next();
}


void TCPHandler::updateProgress(const Progress & value)
{
	state.progress.incrementPiecewiseAtomically(value);
}


void TCPHandler::sendProgress()
{
	writeVarUInt(Protocol::Server::Progress, *out);
	Progress increment = state.progress.fetchAndResetPiecewiseAtomically();
	increment.write(*out, client_revision);
	out->next();
}


void TCPHandler::run()
{
	try
	{
		runImpl();

		LOG_INFO(log, "Done processing connection.");
	}
	catch (Exception & e)
	{
		LOG_ERROR(log, "Code: " << e.code() << ", e.displayText() = " << e.displayText() << ", e.what() = " << e.what()
			<< ", Stack trace:\n\n" << e.getStackTrace().toString());
	}
	catch (Poco::Exception & e)
	{
		std::stringstream message;
		message << "Poco::Exception. Code: " << ErrorCodes::POCO_EXCEPTION << ", e.code() = " << e.code()
			<< ", e.displayText() = " << e.displayText() << ", e.what() = " << e.what();

		/// Таймаут - не ошибка.
		if (!strcmp(e.what(), "Timeout"))
		{
			LOG_DEBUG(log, message.rdbuf());
		}
		else
		{
			LOG_ERROR(log, message.rdbuf());
		}
	}
	catch (std::exception & e)
	{
		LOG_ERROR(log, "std::exception. Code: " << ErrorCodes::STD_EXCEPTION << ". " << e.what());
	}
	catch (...)
	{
		LOG_ERROR(log, "Unknown exception. Code: " << ErrorCodes::UNKNOWN_EXCEPTION << ".");
	}
}


}
