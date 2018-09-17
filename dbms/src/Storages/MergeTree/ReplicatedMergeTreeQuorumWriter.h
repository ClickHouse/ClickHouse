#pragma once

#include <unordered_map>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadHelpers.h>

#include <Storages/MergeTree/MergeTreeDataPart.h>

namespace DB
{

struct ReplicatedMergeTreeQuorumWriter
{
	using PartitionIdToMaxBlock = std::unordered_map<String, Int64>;
	using PartitonIdToPartName= std::unordered_map<String, String>;

	PartitonIdToPartName added_parts;

	MergeTreeDataFormatVersion format_version;

	ReplicatedMergeTreeQuorumWriter(const std::string & old_added_parts, MergeTreeDataFormatVersion format_version_)
	: format_version(format_version_)
	{
		read(old_added_parts);
	}

    /// Write new parts in string with added parts.
	std::string write(const std::string & part_name)
	{
		WriteBufferFromOwnString out;

		auto partition_info = MergeTreePartInfo::fromPartName(part_name, format_version);
		added_parts[partition_info.partition_id] = part_name;

		out << "parts_count " << added_parts.size() << '\n';

		for (const auto & part : added_parts)
			out << part.first << '\t' << part.second << '\n';

		return out.str();
	}

	PartitonIdToPartName readParts()
	{
		return added_parts;
	}

	PartitionIdToMaxBlock readBlocks()
	{
		PartitionIdToMaxBlock max_added_blocks;

		for (const auto & part : added_parts)
		{
			auto partition_info = MergeTreePartInfo::fromPartName(part.second, format_version);
			max_added_blocks[part.first] = partition_info.max_block;
		}
		return max_added_blocks;
	}

	void read(const std::string & str)
	{
		ReadBufferFromString in(str);
		if (checkString("parts_count ", in))
		{
			added_parts = read_v2(in);
		}
		else
			added_parts = read_v3(in);
	}

    /// Read blocks when node in ZooKeeper suppors multiple partitions.
	PartitonIdToPartName read_v2(ReadBufferFromString & in)
	{
		PartitonIdToPartName parts_in_quorum;

		uint64_t parts_count;
		readText(parts_count, in);
		assertChar('\n', in);
		
		for (uint64_t i = 0; i < parts_count; ++i)
		{
			std::string partition_id;
			std::string part_name;

			readText(partition_id, in);
			assertChar('\t', in);
			readText(part_name, in);
			assertChar('\n', in);

			parts_in_quorum[partition_id] = part_name;
		}
		return parts_in_quorum;
	}

    /// Read added bloks when node in ZooKeeper supports only one partition.
	PartitonIdToPartName read_v3(ReadBufferFromString & in)
	{
		PartitonIdToPartName parts_in_quorum;

		std::string partition_name;

		readText(partition_name, in);

		auto partition_info = MergeTreePartInfo::fromPartName(partition_name, format_version);
        parts_in_quorum[partition_info.partition_id] = partition_name;

		return parts_in_quorum;
	}
};

}
