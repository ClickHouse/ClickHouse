#include <zkutil/Types.h>

#include <DB/Storages/MergeTree/ReplicatedMergeTreeLogEntry.h>
#include <DB/Storages/StorageReplicatedMergeTree.h>
#include <DB/IO/Operators.h>


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
	out << "format version: 3\n"
		<< "create_time: " << mysqlxx::DateTime(create_time ? create_time : time(0)) << "\n"
		<< "source replica: " << source_replica << '\n'
		<< "block_id: " << escape << block_id << '\n';

	switch (type)
	{
		case GET_PART:
			out << "get\n" << new_part_name;
			break;

		case MERGE_PARTS:
			out << "merge\n";
			for (const String & s : parts_to_merge)
				out << s << '\n';
			out << "into\n" << new_part_name;
			break;

		case DROP_RANGE:
			if (detach)
				out << "detach\n";
			else
				out << "drop\n";
			out << new_part_name;
			break;

		case ATTACH_PART:
			out << "attach\n";
			if (attach_unreplicated)
				out << "unreplicated\n";
			else
				out << "detached\n";
			out << source_part_name << "\ninto\n" << new_part_name;
			break;

		default:
			throw Exception("Unknown log entry type: " + DB::toString(type), ErrorCodes::LOGICAL_ERROR);
	}

	out << '\n';

	if (quorum)
		out << "quorum: " << quorum << '\n';
}

void ReplicatedMergeTreeLogEntry::readText(ReadBuffer & in)
{
	UInt8 format_version = 0;
	String type_str;

	in >> "format version: " >> format_version >> "\n";

	if (format_version != 1 && format_version != 2 && format_version != 3)
		throw Exception("Unknown ReplicatedMergeTreeLogEntry format version: " + DB::toString(format_version), ErrorCodes::UNKNOWN_FORMAT_VERSION);

	if (format_version >= 2)
	{
		mysqlxx::DateTime create_time_dt;
		in >> "create_time: " >> create_time_dt >> "\n";
		create_time = create_time_dt;
	}

	in >> "source replica: " >> source_replica >> "\n";

	if (format_version >= 3)
	{
		in >> "block_id: " >> escape >> block_id >> "\n";
	}

	in >> type_str >> "\n";

	if (type_str == "get")
	{
		type = GET_PART;
		in >> new_part_name;
	}
	else if (type_str == "merge")
	{
		type = MERGE_PARTS;
		while (true)
		{
			String s;
			in >> s >> "\n";
			if (s == "into")
				break;
			parts_to_merge.push_back(s);
		}
		in >> new_part_name;
	}
	else if (type_str == "drop" || type_str == "detach")
	{
		type = DROP_RANGE;
		detach = type_str == "detach";
		in >> new_part_name;
	}
	else if (type_str == "attach")
	{
		type = ATTACH_PART;
		String source_type;
		in >> source_type;
		if (source_type == "unreplicated")
			attach_unreplicated = true;
		else if (source_type == "detached")
			attach_unreplicated = false;
		else
			throw Exception("Bad format: expected 'unreplicated' or 'detached', found '" + source_type + "'", ErrorCodes::CANNOT_PARSE_TEXT);
		in >> "\n" >> source_part_name >> "\ninto\n" >> new_part_name;
	}

	in >> "\n";

	/// Необязательное поле.
	if (!in.eof())
		in >> "quorum: " >> quorum >> "\n";
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

ReplicatedMergeTreeLogEntry::Ptr ReplicatedMergeTreeLogEntry::parse(const String & s, const zkutil::Stat & stat)
{
	ReadBufferFromString in(s);
	Ptr res = new ReplicatedMergeTreeLogEntry;
	res->readText(in);
	assertEOF(in);

	if (!res->create_time)
		res->create_time = stat.ctime / 1000;

	return res;
}

}
