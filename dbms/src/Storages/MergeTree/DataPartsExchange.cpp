#include <DB/Storages/MergeTree/DataPartsExchange.h>
#include <DB/Storages/StorageReplicatedMergeTree.h>
#include <DB/Common/CurrentMetrics.h>
#include <DB/Common/NetException.h>
#include <DB/IO/ReadBufferFromHTTP.h>


namespace CurrentMetrics
{
	extern const Metric ReplicatedSend;
	extern const Metric ReplicatedFetch;
}

namespace DB
{

namespace ErrorCodes
{
	extern const int ABORTED;
	extern const int BAD_SIZE_OF_FILE_IN_DATA_PART;
}

namespace DataPartsExchange
{

namespace
{

std::string getEndpointId(const std::string & node_id)
{
	return "DataPartsExchange:" + node_id;
}

}

std::string Service::getId(const std::string & node_id) const
{
	return getEndpointId(node_id);
}

void Service::processQuery(const Poco::Net::HTMLForm & params, ReadBuffer & body, WriteBuffer & out)
{
	if (is_cancelled)
		throw Exception("Transferring part to replica was cancelled", ErrorCodes::ABORTED);

	String part_name = params.get("part");
	String shard_str = params.get("shard");

	bool send_sharded_part = !shard_str.empty();

	LOG_TRACE(log, "Sending part " << part_name);

	try
	{
		auto storage_lock = owned_storage->lockStructure(false);

		MergeTreeData::DataPartPtr part;

		if (send_sharded_part)
		{
			size_t shard_no = std::stoul(shard_str);
			part = findShardedPart(part_name, shard_no);
		}
		else
			part = findPart(part_name);

		Poco::ScopedReadRWLock part_lock(part->columns_lock);

		CurrentMetrics::Increment metric_increment{CurrentMetrics::ReplicatedSend};

		/// Список файлов возьмем из списка контрольных сумм.
		MergeTreeData::DataPart::Checksums checksums = part->checksums;
		/// Добавим файлы, которых нет в списке контрольных сумм.
		checksums.files["checksums.txt"];
		checksums.files["columns.txt"];

		MergeTreeData::DataPart::Checksums data_checksums;

		writeBinary(checksums.files.size(), out);
		for (const auto & it : checksums.files)
		{
			String file_name = it.first;

			String path;

			if (send_sharded_part)
				path = data.getFullPath() + "reshard/" + shard_str + "/" + part_name + "/" + file_name;
			else
				path = data.getFullPath() + part_name + "/" + file_name;

			UInt64 size = Poco::File(path).getSize();

			writeStringBinary(it.first, out);
			writeBinary(size, out);

			ReadBufferFromFile file_in(path);
			HashingWriteBuffer hashing_out(out);
			copyData(file_in, hashing_out, is_cancelled);

			if (is_cancelled)
				throw Exception("Transferring part to replica was cancelled", ErrorCodes::ABORTED);

			if (hashing_out.count() != size)
				throw Exception("Unexpected size of file " + path, ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART);

			writeBinary(hashing_out.getHash(), out);

			if (file_name != "checksums.txt" &&
				file_name != "columns.txt")
				data_checksums.addFile(file_name, hashing_out.count(), hashing_out.getHash());
		}

		part->checksums.checkEqual(data_checksums, false);
	}
	catch (const NetException & e)
	{
		/// Network error or error on remote side. No need to enquue part for check.
		throw;
	}
	catch (const Exception & e)
	{
		if (e.code() != ErrorCodes::ABORTED)
			typeid_cast<StorageReplicatedMergeTree &>(*owned_storage).enqueuePartForCheck(part_name);
		throw;
	}
	catch (...)
	{
		typeid_cast<StorageReplicatedMergeTree &>(*owned_storage).enqueuePartForCheck(part_name);
		throw;
	}
}

MergeTreeData::DataPartPtr Service::findPart(const String & name)
{
	MergeTreeData::DataPartPtr part = data.getPartIfExists(name);
	if (part)
		return part;
	throw Exception("No part " + name + " in table");
}

MergeTreeData::DataPartPtr Service::findShardedPart(const String & name, size_t shard_no)
{
	MergeTreeData::DataPartPtr part = data.getShardedPartIfExists(name, shard_no);
	if (part)
		return part;
	throw Exception("No part " + name + " in table");
}

MergeTreeData::MutableDataPartPtr Fetcher::fetchPart(
	const String & part_name,
	const String & replica_path,
	const String & host,
	int port,
	bool to_detached)
{
	return fetchPartImpl(part_name, replica_path, host, port, "", to_detached);
}

MergeTreeData::MutableDataPartPtr Fetcher::fetchShardedPart(
	const InterserverIOEndpointLocation & location,
	const String & part_name,
	size_t shard_no)
{
	return fetchPartImpl(part_name, location.name, location.host, location.port, toString(shard_no), true);
}

MergeTreeData::MutableDataPartPtr Fetcher::fetchPartImpl(
	const String & part_name,
	const String & replica_path,
	const String & host,
	int port,
	const String & shard_no,
	bool to_detached)
{
	ReadBufferFromHTTP::Params params =
	{
		{"endpoint", getEndpointId(replica_path)},
		{"part", part_name},
		{"shard", shard_no},
		{"compress", "false"}
	};

	ReadBufferFromHTTP in(host, port, "", params);

	String full_part_name = String(to_detached ? "detached/" : "") + "tmp_" + part_name;
	String part_path = data.getFullPath() + full_part_name + "/";
	Poco::File part_file(part_path);

	if (part_file.exists())
	{
		LOG_ERROR(log, "Directory " + part_path + " already exists. Removing.");
		part_file.remove(true);
	}

	CurrentMetrics::Increment metric_increment{CurrentMetrics::ReplicatedFetch};

	part_file.createDirectory();

	MergeTreeData::MutableDataPartPtr new_data_part = std::make_shared<MergeTreeData::DataPart>(data);
	new_data_part->name = full_part_name;
	new_data_part->is_temp = true;

	size_t files;
	readBinary(files, in);
	MergeTreeData::DataPart::Checksums checksums;
	for (size_t i = 0; i < files; ++i)
	{
		String file_name;
		UInt64 file_size;

		readStringBinary(file_name, in);
		readBinary(file_size, in);

		WriteBufferFromFile file_out(part_path + file_name);
		HashingWriteBuffer hashing_out(file_out);
		copyData(in, hashing_out, file_size, is_cancelled);

		if (is_cancelled)
		{
			/// NOTE Флаг is_cancelled также имеет смысл проверять при каждом чтении по сети, осуществляя poll с не очень большим таймаутом.
			/// А сейчас мы проверяем его только между прочитанными кусками (в функции copyData).
			part_file.remove(true);
			throw Exception("Fetching of part was cancelled", ErrorCodes::ABORTED);
		}

		uint128 expected_hash;
		readBinary(expected_hash, in);

		if (expected_hash != hashing_out.getHash())
			throw Exception("Checksum mismatch for file " + part_path + file_name + " transferred from " + replica_path);

		if (file_name != "checksums.txt" &&
			file_name != "columns.txt")
			checksums.addFile(file_name, file_size, expected_hash);
	}

	assertEOF(in);

	ActiveDataPartSet::parsePartName(part_name, *new_data_part);
	new_data_part->modification_time = time(0);
	new_data_part->loadColumns(true);
	new_data_part->loadChecksums(true);
	new_data_part->loadIndex();
	new_data_part->is_sharded = false;
	new_data_part->checksums.checkEqual(checksums, false);

	return new_data_part;
}

}

}
