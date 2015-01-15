#include <DB/Client/ShardReplicas.h>

namespace DB
{
	ShardReplicas::ShardReplicas(std::vector<ConnectionPool::Entry> & entries, Settings * settings_) :
		settings(settings_)
	{
		valid_replicas_count = entries.size();
		replica_hash.reserve(valid_replicas_count);

		for (auto & entry : entries)
		{
			Connection * connection = &*entry;
			replica_hash.insert(std::make_pair(connection->socket.impl()->sockfd(), Replica(connection)));
		}
	}

	int ShardReplicas::waitForReadEvent()
	{
		if (valid_replicas_count == 0)
			return 0;

		Poco::Net::Socket::SocketList write_list;
		Poco::Net::Socket::SocketList except_list;

		Poco::Net::Socket::SocketList read_list;
		read_list.reserve(valid_replicas_count);

		for (auto & e : replica_hash)
		{
			Replica & replica = e.second;
			replica.can_read = false;
			if (replica.is_valid)
				read_list.push_back(replica.connection->socket);
		}

        int n = Poco::Net::Socket::select(read_list, write_list, except_list, settings->poll_interval * 1000000);

        for (const auto & socket : read_list) 
		{
			auto it = replica_hash.find(socket.impl()->sockfd());
			if (it == replica_hash.end())
				throw Exception("Unexpected replica", ErrorCodes::UNEXPECTED_REPLICA);
			Replica & replica = it->second;
			replica.can_read = true;
        }

        return n;
	}

	ShardReplicas::Replica & ShardReplicas::pickReplica()
	{
		Replica * res = nullptr;

        int n = waitForReadEvent();
        if (n > 0)
		{
			int max_packet_number = -1;
			for (auto & e : replica_hash) 
			{
				Replica & replica = e.second;
				if (replica.can_read && (replica.next_packet_number > max_packet_number))
				{
					max_packet_number = replica.next_packet_number;
					res = &replica;
				}
			}
		}

		if (res == nullptr)
			throw Exception("No available replica", ErrorCodes::NO_AVAILABLE_REPLICA);

        return *res;
	}

	Connection::Packet ShardReplicas::receivePacket()
	{
		while (true)
		{
			Replica & replica = pickReplica();
			bool retry = false;

			while (replica.is_valid)
			{
				Connection::Packet packet = replica.connection->receivePacket();

				switch (packet.type)
				{
					case Protocol::Server::Data:
					case Protocol::Server::Progress:
					case Protocol::Server::ProfileInfo:
					case Protocol::Server::Totals:
					case Protocol::Server::Extremes:
						break;

					case Protocol::Server::EndOfStream:
					case Protocol::Server::Exception:
						replica.is_valid = false;
						--valid_replicas_count;
						/// Больше ничего не читаем. Отменяем выполнение всех оставшихся запросов,
						/// затем получаем оставшиеся пакеты, чтобы не было рассинхронизации с
						/// репликами.
						sendCancel();
						drainResidualPackets();
						break;

					default:
						/// Мы получили инвалидный пакет от реплики. Повторим попытку
						/// c другой реплики, если такая найдется.
						replica.is_valid = false;
						--valid_replicas_count;
						if (valid_replicas_count > 0)
							retry = true;
						break;
				}

				if ((replica.next_packet_number == next_packet_number) && !retry)
				{
					++replica.next_packet_number;
					++next_packet_number;
					return packet;
				}
				else
				{
					++replica.next_packet_number;
					retry = false;
				}
			}
		}
	}

	void ShardReplicas::sendQuery(const String & query, const String & query_id, UInt64 stage, 
				   const Settings * settings_, bool with_pending_data)
	{
		for (auto & e : replica_hash)
		{
			Connection * connection = e.second.connection;
			connection->sendQuery(query, query_id, stage, settings_, with_pending_data);
		}
	}

	void ShardReplicas::disconnect()
	{
		for (auto & e : replica_hash)
		{
			Replica & replica = e.second;
			if (replica.is_valid)
			{
				Connection * connection = replica.connection;
				connection->disconnect();
			}
		}
	}

	void ShardReplicas::sendCancel()
	{
		for (auto & e : replica_hash)
		{
			Replica & replica = e.second;
			if (replica.is_valid)
			{
				Connection * connection = replica.connection;
				connection->sendCancel();
			}
		}
	}

	void ShardReplicas::drainResidualPackets()
	{
		bool caught_exceptions = false;

		for (auto & e : replica_hash)
		{
			Replica & replica = e.second;
			if (replica.is_valid)
			{
				Connection * connection = replica.connection;
				bool again = true;

				while (again)
				{
					Connection::Packet packet = connection->receivePacket();

					switch (packet.type)
					{
						case Protocol::Server::Data:
						case Protocol::Server::Progress:
						case Protocol::Server::ProfileInfo:
						case Protocol::Server::Totals:
						case Protocol::Server::Extremes:
							break;

						case Protocol::Server::EndOfStream:
							again = false;
							continue;

						case Protocol::Server::Exception:
							// XXX Что делать?
							caught_exceptions = true;
							again = false;
							continue;

						default:
							// XXX Что делать?
							caught_exceptions = true;
							again = false;
							continue;
					}
				}
			}
		}

		if (caught_exceptions)
		{
			// XXX Что выкидываем?
		}
	}

	std::string ShardReplicas::dumpAddresses() const
	{
		if (valid_replicas_count == 0)
			return "";

		std::ostringstream os;
		for (auto & e : replica_hash)
		{
			char prefix = '\0';
			const Replica & replica = e.second;
			if (replica.is_valid)
			{
				const Connection * connection = replica.connection;
				os << prefix << connection->getServerAddress();
				if (prefix == '\0')
					prefix = ';';
			}
		}

		return os.str();
	}

	void ShardReplicas::sendExternalTablesData(std::vector<ExternalTablesData> & data)
	{
		if (data.size() != replica_hash.size())
			throw Exception("Mismatch between replicas and data sources", ErrorCodes::MISMATCH_REPLICAS_DATA_SOURCES);

		auto it = data.begin();
		for (auto & e : replica_hash)
		{
			Connection * connection = e.second.connection;
			connection->sendExternalTablesData(*it);
			++it;
		}
	}
}
