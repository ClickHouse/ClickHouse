#pragma once

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/Storages/MergeTree/MergeTreeData.h>
#include <DB/Storages/MergeTree/PKCondition.h>
#include <DB/Storages/MergeTree/MergeTreeReader.h>
#include <DB/Storages/MergeTree/MergeTreeReadPool.h>


namespace DB
{


/** Used in conjunction with MergeTreeReadPool, asking it for more work to do and performing whatever reads it is asked
 *	to perform. */
class MergeTreeThreadBlockInputStream : public IProfilingBlockInputStream
{
	/// "thread" index (there are N threads and each thread is assigned index in interval [0..N-1])
	std::size_t thread;
public:
	MergeTreeThreadBlockInputStream(
		const std::size_t thread,
		const MergeTreeReadPoolPtr & pool, const std::size_t min_marks_to_read, const std::size_t block_size,
		MergeTreeData & storage, const bool use_uncompressed_cache, const ExpressionActionsPtr & prewhere_actions,
		const String & prewhere_column, const Settings & settings, const Names & virt_column_names)
		: thread{thread}, pool{pool}, block_size_marks{block_size / storage.index_granularity},
		  /// round min_marks_to_read up to nearest multiple of block_size expressed in marks
		  min_marks_to_read{block_size
							? (min_marks_to_read * storage.index_granularity + block_size - 1)
							  / block_size * block_size / storage.index_granularity
							: min_marks_to_read
		  },
		  storage{storage}, use_uncompressed_cache{use_uncompressed_cache}, prewhere_actions{prewhere_actions},
		  prewhere_column{prewhere_column}, min_bytes_to_use_direct_io{settings.min_bytes_to_use_direct_io},
		  max_read_buffer_size{settings.max_read_buffer_size}, virt_column_names{virt_column_names},
		  log{&Logger::get("MergeTreeThreadBlockInputStream")}
	{}

	String getName() const override { return "MergeTreeThread"; }

	String getID() const override
	{
		std::stringstream res;
		/// @todo print some meaningful information
//		res << "MergeTreeThread(columns";
//
//		for (const auto & column : columns)
//			res << ", " << column.name;
//
//		if (prewhere_actions)
//			res << ", prewhere, " << prewhere_actions->getID();
//
//		res << ", marks";
//
//		for (size_t i = 0; i < all_mark_ranges.size(); ++i)
//			res << ", " << all_mark_ranges[i].begin << ", " << all_mark_ranges[i].end;
//
//		res << ")";

		res << static_cast<const void *>(this);
		return res.str();
	}

protected:
	/// Будем вызывать progressImpl самостоятельно.
	void progress(const Progress & value) override {}

	Block readImpl() override
	{
		Block res;

		while (!res && !isCancelled())
		{
			if (!task && !getNewTask())
				break;

			res = readFromPart();

			if (res)
				injectVirtualColumns(res);

			if (task->mark_ranges.empty())
				task = {};
		}

		return res;
	}

private:
	/// Requests read task from MergeTreeReadPool and signals whether it got one
	bool getNewTask()
	{
		task = pool->getTask(min_marks_to_read, thread);

		if (!task)
		{
			/** Закрываем файлы (ещё до уничтожения объекта).
			 *	Чтобы при создании многих источников, но одновременном чтении только из нескольких,
			 *	буферы не висели в памяти. */
			reader = {};
			pre_reader = {};
			return false;
		}

		const auto path = storage.getFullPath() + task->data_part->name + '/';

		/// Позволяет пулу уменьшать количество потоков в случае слишком медленных чтений.
		auto profile_callback = [this](ReadBufferFromFileBase::ProfileInfo info) { pool->profileFeedback(info); };

		if (!reader)
		{
			if (use_uncompressed_cache)
				owned_uncompressed_cache = storage.context.getUncompressedCache();

			owned_mark_cache = storage.context.getMarkCache();

			reader = std::make_unique<MergeTreeReader>(
				path, task->data_part, task->columns, owned_uncompressed_cache.get(), owned_mark_cache.get(), true,
				storage, task->mark_ranges, min_bytes_to_use_direct_io, max_read_buffer_size, MergeTreeReader::ValueSizeMap{}, profile_callback);

			if (prewhere_actions)
				pre_reader = std::make_unique<MergeTreeReader>(
					path, task->data_part, task->pre_columns, owned_uncompressed_cache.get(), owned_mark_cache.get(), true,
					storage, task->mark_ranges, min_bytes_to_use_direct_io,
					max_read_buffer_size, MergeTreeReader::ValueSizeMap{}, profile_callback);
		}
		else
		{
			/// retain avg_value_size_hints
			reader = std::make_unique<MergeTreeReader>(
				path, task->data_part, task->columns, owned_uncompressed_cache.get(), owned_mark_cache.get(), true,
				storage, task->mark_ranges, min_bytes_to_use_direct_io, max_read_buffer_size,
				reader->getAvgValueSizeHints(), profile_callback);

			if (prewhere_actions)
				pre_reader = std::make_unique<MergeTreeReader>(
					path, task->data_part, task->pre_columns, owned_uncompressed_cache.get(), owned_mark_cache.get(), true,
					storage, task->mark_ranges, min_bytes_to_use_direct_io,
					max_read_buffer_size, pre_reader->getAvgValueSizeHints(), profile_callback);
		}

		return true;
	}

	Block readFromPart()
	{
		Block res;

		if (prewhere_actions)
		{
			do
			{
				/// Прочитаем полный блок столбцов, нужных для вычисления выражения в PREWHERE.
				size_t space_left = std::max(1LU, block_size_marks);
				MarkRanges ranges_to_read;

				while (!task->mark_ranges.empty() && space_left && !isCancelled())
				{
					auto & range = task->mark_ranges.back();

					size_t marks_to_read = std::min(range.end - range.begin, space_left);
					pre_reader->readRange(range.begin, range.begin + marks_to_read, res);

					ranges_to_read.emplace_back(range.begin, range.begin + marks_to_read);
					space_left -= marks_to_read;
					range.begin += marks_to_read;
					if (range.begin == range.end)
						task->mark_ranges.pop_back();
				}

				/// В случае isCancelled.
				if (!res)
					return res;

				progressImpl({ res.rowsInFirstColumn(), res.bytes() });
				pre_reader->fillMissingColumns(res, task->ordered_names, task->should_reorder);

				/// Вычислим выражение в PREWHERE.
				prewhere_actions->execute(res);

				ColumnPtr column = res.getByName(prewhere_column).column;
				if (task->remove_prewhere_column)
					res.erase(prewhere_column);

				const auto pre_bytes = res.bytes();

				/** Если фильтр - константа (например, написано PREWHERE 1),
				  *  то либо вернём пустой блок, либо вернём блок без изменений.
				  */
				if (const auto column_const = typeid_cast<const ColumnConstUInt8 *>(column.get()))
				{
					if (!column_const->getData())
					{
						res.clear();
						return res;
					}

					for (const auto & range : ranges_to_read)
						reader->readRange(range.begin, range.end, res);

					progressImpl({ 0, res.bytes() - pre_bytes });
				}
				else if (const auto column_vec = typeid_cast<const ColumnUInt8 *>(column.get()))
				{
					size_t index_granularity = storage.index_granularity;

					const auto & pre_filter = column_vec->getData();
					IColumn::Filter post_filter(pre_filter.size());

					/// Прочитаем в нужных отрезках остальные столбцы и составим для них свой фильтр.
					size_t pre_filter_pos = 0;
					size_t post_filter_pos = 0;

					for (const auto & range : ranges_to_read)
					{
						auto begin = range.begin;
						auto pre_filter_begin_pos = pre_filter_pos;

						for (auto mark = range.begin; mark <= range.end; ++mark)
						{
							UInt8 nonzero = 0;

							if (mark != range.end)
							{
								const size_t limit = std::min(pre_filter.size(), pre_filter_pos + index_granularity);
								for (size_t row = pre_filter_pos; row < limit; ++row)
									nonzero |= pre_filter[row];
							}

							if (!nonzero)
							{
								if (mark > begin)
								{
									memcpy(
										&post_filter[post_filter_pos],
										&pre_filter[pre_filter_begin_pos],
										pre_filter_pos - pre_filter_begin_pos);
									post_filter_pos += pre_filter_pos - pre_filter_begin_pos;
									reader->readRange(begin, mark, res);
								}
								begin = mark + 1;
								pre_filter_begin_pos = std::min(pre_filter_pos + index_granularity, pre_filter.size());
							}

							if (mark < range.end)
								pre_filter_pos = std::min(pre_filter_pos + index_granularity, pre_filter.size());
						}
					}

					if (!post_filter_pos)
					{
						res.clear();
						continue;
					}

					progressImpl({ 0, res.bytes() - pre_bytes });

					post_filter.resize(post_filter_pos);

					/// Отфильтруем столбцы, относящиеся к PREWHERE, используя pre_filter,
					///  остальные столбцы - используя post_filter.
					size_t rows = 0;
					for (const auto i : ext::range(0, res.columns()))
					{
						auto & col = res.getByPosition(i);
						if (col.name == prewhere_column && res.columns() > 1)
							continue;
						col.column =
							col.column->filter(task->column_name_set.count(col.name) ? post_filter : pre_filter, -1);
						rows = col.column->size();
					}

					/// Заменим столбец со значением условия из PREWHERE на константу.
					if (!task->remove_prewhere_column)
						res.getByName(prewhere_column).column = new ColumnConstUInt8{rows, 1};
				}
				else
					throw Exception{
						"Illegal type " + column->getName() + " of column for filter. Must be ColumnUInt8 or ColumnConstUInt8.",
						ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER
					};

				if (res)
					reader->fillMissingColumnsAndReorder(res, task->ordered_names);
			}
			while (!task->mark_ranges.empty() && !res && !isCancelled());
		}
		else
		{
			size_t space_left = std::max(1LU, block_size_marks);

			while (!task->mark_ranges.empty() && space_left && !isCancelled())
			{
				auto & range = task->mark_ranges.back();

				const size_t marks_to_read = std::min(range.end - range.begin, space_left);
				reader->readRange(range.begin, range.begin + marks_to_read, res);

				space_left -= marks_to_read;
				range.begin += marks_to_read;
				if (range.begin == range.end)
					task->mark_ranges.pop_back();
			}

			/// В случае isCancelled.
			if (!res)
				return res;

			progressImpl({ res.rowsInFirstColumn(), res.bytes() });
			reader->fillMissingColumns(res, task->ordered_names, task->should_reorder);
		}

		return res;
	}

	void injectVirtualColumns(Block & block)
	{
		const auto rows = block.rowsInFirstColumn();

		/// add virtual columns
		if (!virt_column_names.empty())
		{
			for (const auto & virt_column_name : virt_column_names)
			{
				if (virt_column_name == "_part")
				{
					block.insert(ColumnWithTypeAndName{
						ColumnConst<String>{rows, task->data_part->name}.convertToFullColumn(),
						new DataTypeString,
						virt_column_name
					});
				}
				else if (virt_column_name == "_part_index")
				{
					block.insert(ColumnWithTypeAndName{
						ColumnConst<UInt64>{rows, task->part_index_in_query}.convertToFullColumn(),
						new DataTypeUInt64,
						virt_column_name
					});
				}
			}
		}
	}

	MergeTreeReadPoolPtr pool;
	const std::size_t block_size_marks;
	const std::size_t min_marks_to_read;
	MergeTreeData & storage;
	const bool use_uncompressed_cache;
	ExpressionActionsPtr prewhere_actions;
	const String prewhere_column;
	const std::size_t min_bytes_to_use_direct_io;
	const std::size_t max_read_buffer_size;
	const Names virt_column_names;

	Logger * log;

	using MergeTreeReaderPtr = std::unique_ptr<MergeTreeReader>;

	UncompressedCachePtr owned_uncompressed_cache;
	MarkCachePtr owned_mark_cache;

	MergeTreeReadTaskPtr task;
	MergeTreeReaderPtr reader;
	MergeTreeReaderPtr pre_reader;
};


}
