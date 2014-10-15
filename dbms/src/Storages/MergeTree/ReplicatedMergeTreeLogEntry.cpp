#include <DB/Storages/MergeTree/ReplicatedMergeTreeLogEntry.h>
#include <DB/Storages/StorageReplicatedMergeTree.h>


namespace DB
{


FuturePartTagger::FuturePartTagger(const String & part_, StorageReplicatedMergeTree & storage_)
	: part(part_), storage(storage_)
{
	if (!storage.future_parts.insert(part).second)
		throw Exception("Tagging already tagged future part " + part + ". This is a bug.", ErrorCodes::LOGICAL_ERROR);
}

FuturePartTagger::~FuturePartTagger()
{
	try
	{
		std::unique_lock<std::mutex> lock(storage.queue_mutex);
		if (!storage.future_parts.erase(part))
			throw Exception("Untagging already untagged future part " + part + ". This is a bug.", ErrorCodes::LOGICAL_ERROR);
	}
	catch (...)
	{
		tryLogCurrentException(__PRETTY_FUNCTION__);
	}
}


void ReplicatedMergeTreeLogEntry::addResultToVirtualParts(StorageReplicatedMergeTree & storage)
{
	if (type == MERGE_PARTS || type == GET_PART || type == DROP_RANGE || type == ATTACH_PART)
		storage.virtual_parts.add(new_part_name);
}

void ReplicatedMergeTreeLogEntry::tagPartAsFuture(StorageReplicatedMergeTree & storage)
{
	if (type == MERGE_PARTS || type == GET_PART || type == ATTACH_PART)
		future_part_tagger = new FuturePartTagger(new_part_name, storage);
}

void ReplicatedMergeTreeLogEntry::writeText(WriteBuffer & out) const
{
	writeString("format version: 1\n", out);
	writeString("source replica: ", out);
	writeString(source_replica, out);
	writeString("\n", out);
	switch (type)
	{
		case GET_PART:
			writeString("get\n", out);
			writeString(new_part_name, out);
			break;
		case MERGE_PARTS:
			writeString("merge\n", out);
			for (const String & s : parts_to_merge)
			{
				writeString(s, out);
				writeString("\n", out);
			}
			writeString("into\n", out);
			writeString(new_part_name, out);
			break;
		case DROP_RANGE:
			if (detach)
				writeString("detach\n", out);
			else
				writeString("drop\n", out);
			writeString(new_part_name, out);
			break;
		case ATTACH_PART:
			writeString("attach\n", out);
			if (attach_unreplicated)
				writeString("unreplicated\n", out);
			else
				writeString("detached\n", out);
			writeString(source_part_name, out);
			writeString("\ninto\n", out);
			writeString(new_part_name, out);
			break;
	}
	writeString("\n", out);
}

void ReplicatedMergeTreeLogEntry::readText(ReadBuffer & in)
{
	String type_str;

	assertString("format version: 1\n", in);
	assertString("source replica: ", in);
	readString(source_replica, in);
	assertString("\n", in);
	readString(type_str, in);
	assertString("\n", in);

	if (type_str == "get")
	{
		type = GET_PART;
		readString(new_part_name, in);
	}
	else if (type_str == "merge")
	{
		type = MERGE_PARTS;
		while (true)
		{
			String s;
			readString(s, in);
			assertString("\n", in);
			if (s == "into")
				break;
			parts_to_merge.push_back(s);
		}
		readString(new_part_name, in);
	}
	else if (type_str == "drop" || type_str == "detach")
	{
		type = DROP_RANGE;
		detach = type_str == "detach";
		readString(new_part_name, in);
	}
	else if (type_str == "attach")
	{
		type = ATTACH_PART;
		String source_type;
		readString(source_type, in);
		if (source_type == "unreplicated")
			attach_unreplicated = true;
		else if (source_type == "detached")
			attach_unreplicated = false;
		else
			throw Exception("Bad format: expected 'unreplicated' or 'detached', found '" + source_type + "'", ErrorCodes::CANNOT_PARSE_TEXT);
		assertString("\n", in);
		readString(source_part_name, in);
		assertString("\ninto\n", in);
		readString(new_part_name, in);
	}
	assertString("\n", in);
}

String ReplicatedMergeTreeLogEntry::toString() const
{
	String s;
	{
		WriteBufferFromString out(s);
		writeText(out);
	}
	return s;
}

ReplicatedMergeTreeLogEntry::Ptr ReplicatedMergeTreeLogEntry::parse(const String & s)
{
	ReadBufferFromString in(s);
	Ptr res = new ReplicatedMergeTreeLogEntry;
	res->readText(in);
	assertEOF(in);
	return res;
}

}
