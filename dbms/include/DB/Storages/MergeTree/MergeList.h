#pragma once

#include <DB/Common/Stopwatch.h>
#include <DB/Common/CurrentMetrics.h>
#include <memory>
#include <list>
#include <mutex>
#include <atomic>

namespace DB
{


struct MergeInfo
{
	const std::string database;
	const std::string table;
	const std::string result_part_name;
	Stopwatch watch;
	Float64 progress{};
	uint64_t num_parts{};
	uint64_t total_size_bytes_compressed{};
	uint64_t total_size_marks{};
	std::atomic<uint64_t> bytes_read_uncompressed{};
	std::atomic<uint64_t> rows_read{};
	std::atomic<uint64_t> bytes_written_uncompressed{};
	std::atomic<uint64_t> rows_written{};


	MergeInfo(const std::string & database, const std::string & table, const std::string & result_part_name)
		: database{database}, table{table}, result_part_name{result_part_name}
	{
	}

	MergeInfo(const MergeInfo & other)
		: database(other.database),
		table(other.table),
		result_part_name(other.result_part_name),
		watch(other.watch),
		progress(other.progress),
		num_parts(other.num_parts),
		total_size_bytes_compressed(other.total_size_bytes_compressed),
		total_size_marks(other.total_size_marks),
		bytes_read_uncompressed(other.bytes_read_uncompressed.load(std::memory_order_relaxed)),
		rows_read(other.rows_read.load(std::memory_order_relaxed)),
		bytes_written_uncompressed(other.bytes_written_uncompressed.load(std::memory_order_relaxed)),
		rows_written(other.rows_written.load(std::memory_order_relaxed))
	{
	}
};


class MergeList;

class MergeListEntry
{
	MergeList & list;

	using container_t = std::list<MergeInfo>;
	container_t::iterator it;

	CurrentMetrics::Increment num_merges {CurrentMetrics::Merge};

public:
	MergeListEntry(const MergeListEntry &) = delete;
	MergeListEntry & operator=(const MergeListEntry &) = delete;

	MergeListEntry(MergeList & list, const container_t::iterator it) : list(list), it{it} {}
	~MergeListEntry();

	MergeInfo * operator->() { return &*it; }
};


class MergeList
{
	friend class MergeListEntry;

	using container_t = std::list<MergeInfo>;

	mutable std::mutex mutex;
	container_t merges;

public:
	using Entry = MergeListEntry;
	using EntryPtr = std::unique_ptr<Entry>;

	template <typename... Args>
	EntryPtr insert(Args &&... args)
	{
		std::lock_guard<std::mutex> lock{mutex};
		return std::make_unique<Entry>(*this, merges.emplace(merges.end(), std::forward<Args>(args)...));
	}

	container_t get() const
	{
		std::lock_guard<std::mutex> lock{mutex};
		return merges;
	}
};


inline MergeListEntry::~MergeListEntry()
{
	std::lock_guard<std::mutex> lock{list.mutex};
	list.merges.erase(it);
}


}
