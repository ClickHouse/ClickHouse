#include <DB/Storages/MergeTree/ShardedPartitionUploader.h>
#include <DB/Storages/MergeTree/ReplicatedMergeTreeAddress.h>
#include <DB/Storages/StorageReplicatedMergeTree.h>
#include <DB/IO/ReadBufferFromHTTP.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>

#include <DB/IO/InterserverWriteBuffer.h>

namespace DB
{

namespace ErrorCodes
{
	extern const int ABORTED;
	extern const int BAD_SIZE_OF_FILE_IN_DATA_PART;
}

namespace ShardedPartitionUploader
{

namespace
{

std::string getEndpointId(const std::string & node_id)
{
	return "ShardedPartitionUploader:" + node_id;
}

}

Service::Service(StoragePtr & storage_)
	: owned_storage{storage_}, data{static_cast<StorageReplicatedMergeTree &>(*storage_).getData()}
{
}

std::string Service::getId(const std::string & node_id) const
{
	return getEndpointId(node_id);
}

void Service::processQuery(const Poco::Net::HTMLForm & params, ReadBuffer & body, WriteBuffer & out)
{
	std::string part_name = params.get("path");
	std::string replica_path = params.get("endpoint");

	String full_part_name = std::string("detached/") + "tmp_" + part_name;
	String part_path = data.getFullPath() + full_part_name + "/";
	Poco::File part_file{part_path};

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
	readBinary(files, body);
	MergeTreeData::DataPart::Checksums checksums;
	for (size_t i = 0; i < files; ++i)
	{
		String file_name;
		UInt64 file_size;

		readStringBinary(file_name, body);
		readBinary(file_size, body);

		WriteBufferFromFile file_out{part_path + file_name};
		HashingWriteBuffer hashing_out{file_out};
		copyData(body, hashing_out, file_size, is_cancelled);

		if (is_cancelled)
		{
			part_file.remove(true);
			throw Exception{"Fetching of part was cancelled", ErrorCodes::ABORTED};
		}

		uint128 expected_hash;
		readBinary(expected_hash, body);

		if (expected_hash != hashing_out.getHash())
			throw Exception{"Checksum mismatch for file " + part_path + file_name + " transferred from " + replica_path};

		if (file_name != "checksums.txt" &&
			file_name != "columns.txt")
			checksums.addFile(file_name, file_size, expected_hash);
	}

	assertEOF(body);

	ActiveDataPartSet::parsePartName(part_name, *new_data_part);
	new_data_part->modification_time = time(0);
	new_data_part->loadColumns(true);
	new_data_part->loadChecksums(true);
	new_data_part->loadIndex();
	new_data_part->is_sharded = false;
	new_data_part->checksums.checkEqual(checksums, false);

	/// Now store permanently the received part.
	new_data_part->is_temp = false;
	const std::string old_part_path = data.getFullPath() + full_part_name;
	const std::string new_part_path = data.getFullPath() + "detached/" + part_name;

	Poco::File new_part_dir{new_part_path};
	if (new_part_dir.exists())
	{
		LOG_WARNING(log, "Directory " + new_part_path + " already exists. Removing.");
		new_part_dir.remove(true);
	}

	Poco::File{old_part_path}.renameTo(new_part_path);
}

Client::Client(StorageReplicatedMergeTree & storage_)
	: storage{storage_}, data{storage_.getData()}
{
}

MergeTreeData::DataPartPtr Client::findShardedPart(const String & name, size_t shard_no)
{
	MergeTreeData::DataPartPtr part = data.getShardedPartIfExists(name, shard_no);
	if (part)
		return part;
	throw Exception("No part " + name + " in table");
}

void Client::setCancellationHook(CancellationHook cancellation_hook_)
{
	cancellation_hook = cancellation_hook_;
}

bool Client::send(const std::string & part_name, size_t shard_no,
	const InterserverIOEndpointLocation & to_location)
{
	std::function<void()> copy_hook = std::bind(&ShardedPartitionUploader::Client::abortIfRequested, this);

	abortIfRequested();

	InterserverWriteBuffer out{to_location.host, to_location.port, getEndpointId(to_location.name), part_name};

	LOG_TRACE(log, "Sending part " << part_name);

	auto storage_lock = storage.lockStructure(false);

	MergeTreeData::DataPartPtr part = findShardedPart(part_name, shard_no);

	Poco::ScopedReadRWLock part_lock{part->columns_lock};

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
		String path = data.getFullPath() + "reshard/" + toString(shard_no) + "/" + part_name + "/" + file_name;
		UInt64 size = Poco::File(path).getSize();

		writeStringBinary(it.first, out);
		writeBinary(size, out);

		ReadBufferFromFile file_in{path};
		HashingWriteBuffer hashing_out{out};
		copyData(file_in, hashing_out, copy_hook);

		abortIfRequested();

		if (hashing_out.count() != size)
			throw Exception{"Unexpected size of file " + path, ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART};

		writeBinary(hashing_out.getHash(), out);

		if (file_name != "checksums.txt" &&
			file_name != "columns.txt")
			data_checksums.addFile(file_name, hashing_out.count(), hashing_out.getHash());
	}

	part->checksums.checkEqual(data_checksums, false);

	return true;
}

void Client::abortIfRequested()
{
	if (is_cancelled)
		throw Exception{"ShardedPartitionUploader service terminated", ErrorCodes::ABORTED};

	if (cancellation_hook)
		cancellation_hook();
}

}

}
