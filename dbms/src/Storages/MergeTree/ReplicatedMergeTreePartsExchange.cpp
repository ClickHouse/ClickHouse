#include <DB/Storages/MergeTree/ReplicatedMergeTreePartsExchange.h>
#include <DB/Storages/StorageReplicatedMergeTree.h>


namespace DB
{

void ReplicatedMergeTreePartsServer::processQuery(const Poco::Net::HTMLForm & params, WriteBuffer & out)
{
	String part_name = params.get("part");
	LOG_TRACE(log, "Sending part " << part_name);

	try
	{
		auto storage_lock = storage.lockStructure(false);

		MergeTreeData::DataPartPtr part = findPart(part_name);

		Poco::ScopedReadRWLock part_lock(part->columns_lock);

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

			String path = data.getFullPath() + part_name + "/" + file_name;
			UInt64 size = Poco::File(path).getSize();

			writeStringBinary(it.first, out);
			writeBinary(size, out);

			ReadBufferFromFile file_in(path);
			HashingWriteBuffer hashing_out(out);
			copyData(file_in, hashing_out);

			if (hashing_out.count() != size)
				throw Exception("Unexpected size of file " + path, ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART);

			writeBinary(hashing_out.getHash(), out);

			if (file_name != "checksums.txt" &&
				file_name != "columns.txt")
				data_checksums.addFile(file_name, hashing_out.count(), hashing_out.getHash());
		}

		part->checksums.checkEqual(data_checksums, false);
	}
	catch (...)
	{
		storage.enqueuePartForCheck(part_name);
		throw;
	}
}

MergeTreeData::MutableDataPartPtr ReplicatedMergeTreePartsFetcher::fetchPart(
	const String & part_name,
	const String & replica_path,
	const String & host,
	int port,
	bool to_detached)
{
	ReadBufferFromHTTP::Params params =
	{
		{"endpoint", "ReplicatedMergeTree:" + replica_path},
		{"part", part_name},
		{"compress", "false"}
	};

	ReadBufferFromHTTP in(host, port, params);

	String full_part_name = String(to_detached ? "detached/" : "") + "tmp_" + part_name;
	String part_path = data.getFullPath() + full_part_name + "/";
	Poco::File part_file(part_path);

	if (part_file.exists())
	{
		LOG_ERROR(log, "Directory " + part_path + " already exists. Removing.");
		part_file.remove(true);
	}

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
		copyData(in, hashing_out, file_size);

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

	new_data_part->checksums.checkEqual(checksums, false);

	return new_data_part;
}

}
