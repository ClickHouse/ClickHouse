#pragma once

#include <Yandex/logger_useful.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/Common/VirtualColumnUtils.h>

#include <DB/Client/ConnectionPool.h>


namespace DB
{

/** Позволяет выполнить запрос (SELECT) на удалённом сервере и получить результат.
  */
class RemoteBlockInputStream : public IProfilingBlockInputStream
{
public:
	RemoteBlockInputStream(Connection & connection_, const String & query_, const Settings * settings_,
						QueryProcessingStage::Enum stage_ = QueryProcessingStage::Complete)
		: connection(connection_), query(query_), _host_column(""), _port_column(""), stage(stage_),
		sent_query(false), finished(false), was_cancelled(false), got_exception_from_server(false),
		log(&Logger::get("RemoteBlockInputStream (" + connection.getServerAddress() + ")"))
	{
		if (settings_)
		{
			send_settings = true;
			settings = *settings_;
		}
		else
			send_settings = false;
	}

	/// Захватывает владение соединением из пула.
	RemoteBlockInputStream(ConnectionPool::Entry pool_entry_, const String & query_, const Settings * settings_,
		QueryProcessingStage::Enum stage_ = QueryProcessingStage::Complete)
		: pool_entry(pool_entry_), connection(*pool_entry), query(query_), _host_column(""),
		_port_column(""), stage(stage_), sent_query(false), finished(false), was_cancelled(false),
		got_exception_from_server(false), log(&Logger::get("RemoteBlockInputStream (" + connection.getServerAddress() + ")"))
	{
		if (settings_)
		{
			send_settings = true;
			settings = *settings_;
		}
		else
			send_settings = false;
	}

	RemoteBlockInputStream(ConnectionPool::Entry pool_entry_, const String & query_, const Settings * settings_,
		const String & _host_column_, const String & _port_column_, QueryProcessingStage::Enum stage_ = QueryProcessingStage::Complete)
		: pool_entry(pool_entry_), connection(*pool_entry), query(query_), _host_column(_host_column_),
		_port_column(_port_column_), stage(stage_), sent_query(false), finished(false), was_cancelled(false),
		got_exception_from_server(false), log(&Logger::get("RemoteBlockInputStream (" + connection.getServerAddress() + ")"))
	{
		if (settings_)
		{
			send_settings = true;
			settings = *settings_;
		}
		else
			send_settings = false;
	}


	String getName() const { return "RemoteBlockInputStream"; }


	String getID() const
	{
		std::stringstream res;
		res << this;
		return res.str();
	}


	/** Отменяем умолчальное уведомление о прогрессе,
	  * так как колбэк прогресса вызывается самостоятельно.
	  */
	void progress(size_t rows, size_t bytes) {}


	void cancel()
	{
		if (!__sync_bool_compare_and_swap(&is_cancelled, false, true))
			return;

		if (sent_query && !was_cancelled && !finished && !got_exception_from_server)
		{
			LOG_TRACE(log, "Cancelling query");

			/// Если запрошено прервать запрос - попросим удалённый сервер тоже прервать запрос.
			connection.sendCancel();
			was_cancelled = true;
		}
	}


	~RemoteBlockInputStream()
	{
		/** Если прервались в середине цикла общения с сервером, то закрываем соединение,
		  *  чтобы оно не осталось висеть в рассихронизированном состоянии.
		  */
		if (sent_query && !finished)
			connection.disconnect();
	}

protected:
	void populateBlock(Block & res)
	{
		if (_host_column != "")
		{
			ColumnPtr column_ptr = ColumnConst<String> (res.rows(), connection.getHost(), new DataTypeString).convertToFullColumn();
			ColumnWithNameAndType column(column_ptr, new DataTypeString, _host_column);
			res.insert(column);
		}
		if (_port_column != "")
		{
			ColumnPtr column_ptr = ColumnConst<UInt16> (res.rows(), connection.getPort(), new DataTypeUInt16).convertToFullColumn();
			ColumnWithNameAndType column(column_ptr, new DataTypeUInt16, _port_column);
			res.insert(column);
		}
	}

	Block readImpl()
	{
		if (!sent_query)
		{
			connection.sendQuery(query, 1, stage, send_settings ? &settings : NULL);
			sent_query = true;
		}

		while (true)
		{
			Connection::Packet packet = connection.receivePacket();

			switch (packet.type)
			{
				case Protocol::Server::Data:
					/// Если блок не пуст и не является заголовочным блоком
					if (packet.block && packet.block.rows() > 0)
					{
						populateBlock(packet.block);
						return packet.block;
					}
					break;	/// Если блок пустой - получим другие пакеты до EndOfStream.

				case Protocol::Server::Exception:
					got_exception_from_server = true;
					packet.exception->rethrow();
					break;

				case Protocol::Server::EndOfStream:
					finished = true;
					return Block();

				case Protocol::Server::Progress:
					/** Используем прогресс с удалённого сервера.
					  * В том числе, запишем его в ProcessList,
					  *  и будем использовать его для проверки
					  *  ограничений (например, минимальная скорость выполнения запроса)
					  *  и квот (например, на количество строчек для чтения).
					  */
					progressImpl(packet.progress.rows, packet.progress.bytes);

					if (!was_cancelled && !finished && isCancelled())
						cancel();

					break;

				case Protocol::Server::ProfileInfo:
					info = packet.profile_info;
					break;

				case Protocol::Server::Totals:
					totals = packet.block;
					break;

				case Protocol::Server::Extremes:
					extremes = packet.block;
					break;

				default:
					throw Exception("Unknown packet from server", ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);
			}
		}
	}

	void readSuffixImpl()
	{
		/** Если одно из:
		 *   - ничего не начинали делать;
		 *   - получили все пакеты до EndOfStream;
		 *   - получили с сервера эксепшен;
		 * - то больше читать ничего не нужно.
		 */
		if (!sent_query || finished || got_exception_from_server)
			return;

		/** Если ещё прочитали не все данные, но они больше не нужны.
		 * Это может быть из-за того, что данных достаточно (например, при использовании LIMIT).
		 */

		/// Отправим просьбу прервать выполнение запроса, если ещё не отправляли.
		if (!was_cancelled)
		{
			LOG_TRACE(log, "Cancelling query because enough data has been read");

			was_cancelled = true;
			connection.sendCancel();
		}

		/// Получим оставшиеся пакеты, чтобы не было рассинхронизации в соединении с сервером.
		while (true)
		{
			Connection::Packet packet = connection.receivePacket();

			switch (packet.type)
			{
				case Protocol::Server::Data:
				case Protocol::Server::Progress:
				case Protocol::Server::ProfileInfo:
				case Protocol::Server::Totals:
				case Protocol::Server::Extremes:
					break;

				case Protocol::Server::EndOfStream:
					return;

				case Protocol::Server::Exception:
					got_exception_from_server = true;
					packet.exception->rethrow();
					break;

				default:
					throw Exception("Unknown packet from server", ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);
			}
		}

		finished = true;
	}

private:
	/// Используется, если нужно владеть соединением из пула
	ConnectionPool::Entry pool_entry;
	
	Connection & connection;

	const String query;
	bool send_settings;
	Settings settings;
	/// Имя столбца, куда записать имя хоста. Пустая строка, если записывать не надо.
	String _host_column;
	/// Имя столбца, куда записать номер порта. Пустая строка, если записывать не надо.
	String _port_column;
	QueryProcessingStage::Enum stage;

	/// Отправили запрос (это делается перед получением первого блока).
	bool sent_query;
	
	/** Получили все данные от сервера, до пакета EndOfStream.
	  * Если при уничтожении объекта, ещё не все данные считаны,
	  *  то для того, чтобы не было рассинхронизации, на сервер отправляется просьба прервать выполнение запроса,
	  *  и после этого считываются все пакеты до EndOfStream.
	  */
	bool finished;
	
	/** На сервер была отправлена просьба прервать выполенение запроса, так как данные больше не нужны.
	  * Это может быть из-за того, что данных достаточно (например, при использовании LIMIT),
	  *  или если на стороне клиента произошло исключение.
	  */
	bool was_cancelled;

	/// С сервера было получено исключение. В этом случае получать больше пакетов или просить прервать запрос не нужно.
	bool got_exception_from_server;

	Logger * log;
};

}
