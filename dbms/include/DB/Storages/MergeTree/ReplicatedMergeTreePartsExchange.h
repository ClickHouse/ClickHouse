#pragma once

#include <DB/Interpreters/InterserverIOHandler.h>
#include <DB/Storages/MergeTree/MergeTreeData.h>
#include <DB/IO/ReadBufferFromHTTP.h>
#include <DB/IO/HashingWriteBuffer.h>
#include <DB/IO/copyData.h>


namespace DB
{

class ReplicatedMergeTreePartsServer : public InterserverIOEndpoint
{
public:
	ReplicatedMergeTreePartsServer(MergeTreeData & data_, StoragePtr owned_storage_) : data(data_),
		owned_storage(owned_storage_), log(&Logger::get("ReplicatedMergeTreePartsServer")) {}

	void processQuery(const Poco::Net::HTMLForm & params, WriteBuffer & out) override
	{
		String part_name = params.get("part");
		LOG_TRACE(log, "Sending part " << part_name);

		auto storage_lock = owned_storage->lockStructure(false);

		MergeTreeData::DataPartPtr part = findPart(part_name);

		/// Список файлов возьмем из списка контрольных сумм.
		MergeTreeData::DataPart::Checksums checksums = part->checksums;
		checksums.files["checksums.txt"];

		writeBinary(checksums.files.size(), out);
		for (const auto & it : checksums.files)
		{
			String path = data.getFullPath() + part_name + "/" + it.first;
			UInt64 size = Poco::File(path).getSize();

			writeStringBinary(it.first, out);
			writeBinary(size, out);

			ReadBufferFromFile file_in(path);
			HashingWriteBuffer hashing_out(out);
			copyData(file_in, hashing_out);

			if (hashing_out.count() != size)
				throw Exception("Unexpected size of file " + path, ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART);

			writeBinary(hashing_out.getHash(), out);
		}
	}

private:
	MergeTreeData & data;
	StoragePtr owned_storage;

	Logger * log;

	MergeTreeData::DataPartPtr findPart(const String & name)
	{
		MergeTreeData::DataParts parts = data.getDataParts();
		for (const auto & part : parts)
		{
			if (part->name == name)
				return part;
		}
		throw Exception("No part " + name + " in table");
	}
};

class ReplicatedMergeTreePartsFetcher
{
public:
	ReplicatedMergeTreePartsFetcher(MergeTreeData & data_) : data(data_), log(&Logger::get("ReplicatedMergeTreePartsFetcher")) {}

	/// Скачивает кусок в tmp_директорию, проверяет чексуммы.
	MergeTreeData::MutableDataPartPtr fetchPart(
		const String & part_name,
		const String & replica_path,
		const String & host,
		int port)
	{
		LOG_TRACE(log, "Fetching part " << part_name);
		ReadBufferFromHTTP::Params params = {
			std::make_pair("endpoint", "ReplicatedMergeTree:" + replica_path),
			std::make_pair("part", part_name)};
		ReadBufferFromHTTP in(host, port, params);

		String part_path = data.getFullPath() + "tmp_" + part_name + "/";
		if (!Poco::File(part_path).createDirectory())
			throw Exception("Directory " + part_path + " already exists");

		size_t files;
		readBinary(files, in);
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
		}

		assertEOF(in);

		MergeTreeData::MutableDataPartPtr new_data_part = std::make_shared<MergeTreeData::DataPart>(data);
		data.parsePartName(part_name, *new_data_part);
		new_data_part->name = "tmp_" + part_name;
		new_data_part->modification_time = time(0);
		new_data_part->loadIndex();
		new_data_part->loadChecksums();

		return new_data_part;
	}

private:
	MergeTreeData & data;

	Logger * log;
};

}
