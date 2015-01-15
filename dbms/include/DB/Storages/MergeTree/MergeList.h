#pragma once

#include <statdaemons/Stopwatch.h>
#include <statdaemons/ext/memory.hpp>
#include <list>
#include <mutex>
#include <atomic>

namespace DB
{


class MergeList
{
	friend class Entry;

	struct MergeInfo
	{
		const std::string database;
		const std::string table;
		const std::string result_part_name;
		Stopwatch watch;
		Float64 progress{};
		std::uint64_t num_parts{};
		std::uint64_t total_size_bytes_compressed{};
		std::uint64_t total_size_marks{};
		std::uint64_t bytes_read_uncompressed{};
		std::uint64_t rows_read{};
		std::uint64_t bytes_written_uncompressed{};
		std::uint64_t rows_written{};


		MergeInfo(const std::string & database, const std::string & table, const std::string & result_part_name)
			: database{database}, table{table}, result_part_name{result_part_name}
		{
		}
	};

	using container_t = std::list<MergeInfo>;

	mutable std::mutex mutex;
	container_t merges;

public:
	class Entry
	{
		MergeList & list;
		container_t::iterator it;

	public:
		Entry(const Entry &) = delete;
		Entry & operator=(const Entry &) = delete;

		Entry(MergeList & list, const container_t::iterator it) : list(list), it{it} {}
		~Entry()
		{
			std::lock_guard<std::mutex> lock{list.mutex};
			list.merges.erase(it);
		}

		MergeInfo * operator->() { return &*it; }
	};

	using EntryPtr = std::unique_ptr<Entry>;

	template <typename... Args>
	EntryPtr insert(Args &&... args)
	{
		std::lock_guard<std::mutex> lock{mutex};
		return ext::make_unique<Entry>(*this, merges.emplace(merges.end(), std::forward<Args>(args)...));
	}

	container_t get() const
	{
		std::lock_guard<std::mutex> lock{mutex};
		return merges;
	}
};


}
