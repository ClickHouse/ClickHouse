#pragma once

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/Storages/MergeTree/MergeTreeData.h>
#include <DB/Storages/MergeTree/PKCondition.h>
#include <DB/Storages/MergeTree/MergeTreeReader.h>
#include <DB/Storages/MergeTree/MergeTreeReadPool.h>


namespace DB
{


class MergeTreeThreadBlockInputStream : public IProfilingBlockInputStream
{
public:
	MergeTreeThreadBlockInputStream(
		const MergeTreeReadPoolPtr & pool, const std::size_t min_marks_to_read, const std::size_t block_size,
		MergeTreeData & storage, const bool use_uncompressed_cache, const ExpressionActionsPtr & prewhere_actions,
		const String & prewhere_column, const std::size_t min_bytes_to_use_direct_io,
		const std::size_t max_read_buffer_size, const Names & virt_column_names)
		: pool{pool}, min_marks_to_read{min_marks_to_read}, block_size{block_size}, storage{storage},
		  use_uncompressed_cache{use_uncompressed_cache}, prewhere_actions{prewhere_actions},
		  prewhere_column{prewhere_column}, min_bytes_to_use_direct_io{min_bytes_to_use_direct_io},
		  max_read_buffer_size{max_read_buffer_size}, virt_column_names{virt_column_names},
		  log{&Logger::get("MergeTreeThreadBlockInputStream")}
	{}

	String getName() const override { return "MergeTreeThread"; }

	String getID() const override
	{
		std::stringstream res;
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
		return res.str();
	}

protected:
	/// Будем вызывать progressImpl самостоятельно.
	void progress(const Progress & value) override {}

	Block readImpl() override
	{
		Block res;

		while (!res)
		{
			if (!task && !getNewTask())
				break;

			res = readFromPart();

			if (res)
				injectVirtualColumns(res);

			if (task->mark_ranges.empty())
			{
				/** Закрываем файлы (ещё до уничтожения объекта).
				 *	Чтобы при создании многих источников, но одновременном чтении только из нескольких,
				 *	буферы не висели в памяти. */
				task = {};
				reader = {};
				pre_reader = {};
			}
		}

		return res;
	}

private:
	bool getNewTask()
	{
		task = pool->getTask(min_marks_to_read);

		if (!task)
			return false;

		const auto path = storage.getFullPath() + task->data_part->name + '/';

		if (!reader)
		{
			if (use_uncompressed_cache)
				owned_uncompressed_cache = storage.context.getUncompressedCache();

			owned_mark_cache = storage.context.getMarkCache();

			reader = std::make_unique<MergeTreeReader>(
				path, task->data_part, task->columns, owned_uncompressed_cache.get(), owned_mark_cache.get(),
				storage, task->mark_ranges, min_bytes_to_use_direct_io, max_read_buffer_size);

			if (prewhere_actions)
				pre_reader = std::make_unique<MergeTreeReader>(
					path, task->data_part, task->pre_columns, owned_uncompressed_cache.get(),
					owned_mark_cache.get(), storage, task->mark_ranges, min_bytes_to_use_direct_io,
					max_read_buffer_size);
		}
		else
		{
			reader->reconf(path, task->data_part, task->columns, task->mark_ranges);
			if (prewhere_actions)
				pre_reader->reconf(path, task->data_part, task->pre_columns, task->mark_ranges);
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
				size_t space_left = std::max(1LU, block_size / storage.index_granularity);
				MarkRanges ranges_to_read;
				while (!task->mark_ranges.empty() && space_left)
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
							col.column->filter(task->column_name_set.count(col.name) ? post_filter : pre_filter);
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

				reader->fillMissingColumnsAndReorder(res, task->ordered_names);
			}
			while (!task->mark_ranges.empty() && !res && !isCancelled());
		}
		else
		{
			size_t space_left = std::max(1LU, block_size / storage.index_granularity);

			while (!task->mark_ranges.empty() && space_left)
			{
				auto & range = task->mark_ranges.back();

				const size_t marks_to_read = std::min(range.end - range.begin, space_left);
				reader->readRange(range.begin, range.begin + marks_to_read, res);

				space_left -= marks_to_read;
				range.begin += marks_to_read;
				if (range.begin == range.end)
					task->mark_ranges.pop_back();
			}

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
					block.insert(ColumnWithNameAndType{
						ColumnConst<String>{rows, task->data_part->name}.convertToFullColumn(),
						new DataTypeString,
						virt_column_name
					});
				}
				else if (virt_column_name == "_part_index")
				{
					block.insert(ColumnWithNameAndType{
						ColumnConst<UInt64>{rows, task->part_index_in_query}.convertToFullColumn(),
						new DataTypeUInt64,
						virt_column_name
					});
				}
			}
		}
	}

	MergeTreeReadPoolPtr pool;
	const std::size_t min_marks_to_read;
	const std::size_t block_size;
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
