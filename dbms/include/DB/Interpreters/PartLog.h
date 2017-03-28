#pragma once

#include <DB/Interpreters/SystemLog.h>


namespace DB
{

struct PartLogElement
{
	enum Type
	{
		NEW_PART = 1,
		MERGE_PARTS = 2,
		DOWNLOAD_PART = 3,
		REMOVE_PART = 4,
	};

	Type event_type = NEW_PART;

	time_t event_time{};

	UInt64 size_in_bytes{};
	UInt64 duration_ms{};

	String database_name;
	String table_name;
	String part_name;
	Strings merged_from;

	static std::string name() { return "PartLog"; }

	static Block createBlock();
	void appendToBlock(Block & block) const;

};


/// Instead of typedef - to allow forward declaration.
class PartLog : public SystemLog<PartLogElement>
{
	using SystemLog<PartLogElement>::SystemLog;
};

}
