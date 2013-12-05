#pragma once

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/Storages/StorageMergeTree.h>
#include <DB/Storages/MergeTree/PKCondition.h>

#include <DB/Storages/MergeTree/MergeTreeReader.h>


namespace DB
{

/// Для чтения из одного куска. Для чтения сразу из многих, Storage использует сразу много таких объектов.
class MergeTreeBlockInputStream : public IProfilingBlockInputStream
{
public:
	/// Параметры storage_ и owned_storage разделены, чтобы можно было сделать поток, не владеющий своим storage
	/// (например, поток, сливаящий куски). В таком случае сам storage должен следить, чтобы не удалить данные, пока их читают.
	MergeTreeBlockInputStream(const String & path_,	/// Путь к куску
		size_t block_size_, const Names & column_names_,
		StorageMergeTree & storage_, const StorageMergeTree::DataPartPtr & owned_data_part_,
		const MarkRanges & mark_ranges_, StoragePtr owned_storage, bool use_uncompressed_cache_,
		ExpressionActionsPtr prewhere_actions_, String prewhere_column_)
		: IProfilingBlockInputStream(owned_storage),
		path(path_), block_size(block_size_), column_names(column_names_),
		storage(storage_), owned_data_part(owned_data_part_),
		all_mark_ranges(mark_ranges_), remaining_mark_ranges(mark_ranges_),
		use_uncompressed_cache(use_uncompressed_cache_),
		prewhere_actions(prewhere_actions_), prewhere_column(prewhere_column_)
	{
		std::reverse(remaining_mark_ranges.begin(), remaining_mark_ranges.end());

		if (prewhere_actions){
			pre_column_names = prewhere_actions->getRequiredColumns();
			if (pre_column_names.empty())
				pre_column_names.push_back(column_names[0]);
			NameSet pre_name_set(pre_column_names.begin(), pre_column_names.end());
			Names post_column_names;
			for (size_t i = 0; i < column_names.size(); ++i)
			{
				if (!pre_name_set.count(column_names[i]))
					post_column_names.push_back(column_names[i]);
			}
			column_names = post_column_names;
		}
		column_name_set.insert(column_names.begin(), column_names.end());

		LOG_TRACE(storage.log, "Reading " << all_mark_ranges.size() << " ranges from part " << owned_data_part->name
			<< ", up to " << (all_mark_ranges.back().end - all_mark_ranges.front().begin) * storage.index_granularity
			<< " rows starting from " << all_mark_ranges.front().begin * storage.index_granularity);
	}
	
	String getName() const { return "MergeTreeBlockInputStream"; }

	String getID() const
	{
		std::stringstream res;
		res << "MergeTree(" << owned_storage->getTableName() << ", " << path << ", columns";

		for (size_t i = 0; i < column_names.size(); ++i)
			res << ", " << column_names[i];

		res << ", marks";

		for (size_t i = 0; i < all_mark_ranges.size(); ++i)
			res << ", " << all_mark_ranges[i].begin << ", " << all_mark_ranges[i].end;

		res << ")";
		return res.str();
	}
	
	/// Получает набор диапазонов засечек, вне которых не могут находиться ключи из заданного диапазона.
	static MarkRanges markRangesFromPkRange(const String & path,
											size_t marks_count,
											StorageMergeTree & storage,
											PKCondition & key_condition)
	{
		MarkRanges res;
		
		/// Если индекс не используется.
		if (key_condition.alwaysTrue())
		{
			res.push_back(MarkRange(0, marks_count));
		}
		else
		{
			/// Читаем индекс.
			typedef AutoArray<Row> Index;
			size_t key_size = storage.sort_descr.size();
			Index index(marks_count);
			for (size_t i = 0; i < marks_count; ++i)
				index[i].resize(key_size);
			
			{
				String index_path = path + "primary.idx";
				ReadBufferFromFile index_file(index_path, std::min(static_cast<size_t>(DBMS_DEFAULT_BUFFER_SIZE), Poco::File(index_path).getSize()));
				
				for (size_t i = 0; i < marks_count; ++i)
				{
					for (size_t j = 0; j < key_size; ++j)
						storage.primary_key_sample.getByPosition(j).type->deserializeBinary(index[i][j], index_file);
				}
				
				if (!index_file.eof())
					throw Exception("index file " + index_path + " is unexpectedly long", ErrorCodes::EXPECTED_END_OF_FILE);
			}
			
			/// В стеке всегда будут находиться непересекающиеся подозрительные отрезки, самый левый наверху (back).
			/// На каждом шаге берем левый отрезок и проверяем, подходит ли он.
			/// Если подходит, разбиваем его на более мелкие и кладем их в стек. Если нет - выбрасываем его.
			/// Если отрезок уже длиной в одну засечку, добавляем его в ответ и выбрасываем.
			std::vector<MarkRange> ranges_stack;
			ranges_stack.push_back(MarkRange(0, marks_count));
			while (!ranges_stack.empty())
			{
				MarkRange range = ranges_stack.back();
				ranges_stack.pop_back();
				
				bool may_be_true;
				if (range.end == marks_count)
					may_be_true = key_condition.mayBeTrueAfter(index[range.begin]);
				else
					may_be_true = key_condition.mayBeTrueInRange(index[range.begin], index[range.end]);
				
				if (!may_be_true)
					continue;
				
				if (range.end == range.begin + 1)
				{
					/// Увидели полезный промежуток между соседними засечками. Либо добавим его к последнему диапазону, либо начнем новый диапазон.
					if (res.empty() || range.begin - res.back().end > storage.min_marks_for_seek)
					{
						res.push_back(range);
					}
					else
					{
						res.back().end = range.end;
					}
				}
				else
				{
					/// Разбиваем отрезок и кладем результат в стек справа налево.
					size_t step = (range.end - range.begin - 1) / storage.settings.coarse_index_granularity + 1;
					size_t end;
					for (end = range.end; end > range.begin + step; end -= step)
					{
						ranges_stack.push_back(MarkRange(end - step, end));
					}
					ranges_stack.push_back(MarkRange(range.begin, end));
				}
			}
		}
		
		return res;
	}
	
protected:
	Block readImpl()
	{
		Block res;

		if (remaining_mark_ranges.empty())
			return res;

		if (!reader)
		{
			UncompressedCache * uncompressed_cache = use_uncompressed_cache ? storage.context.getUncompressedCache() : NULL;
			reader = new MergeTreeReader(path, column_names, uncompressed_cache, storage);
			if (prewhere_actions)
				pre_reader = new MergeTreeReader(path, pre_column_names, uncompressed_cache, storage);
		}

		if (prewhere_actions)
		{
			do
			{
				/// Прочитаем полный блок столбцов, нужных для вычисления выражения в PREWHERE.
				size_t space_left = std::max(1LU, block_size / storage.index_granularity);
				MarkRanges ranges_to_read;
				while (!remaining_mark_ranges.empty() && space_left)
				{
					MarkRange & range = remaining_mark_ranges.back();

					size_t marks_to_read = std::min(range.end - range.begin, space_left);
					pre_reader->readRange(range.begin, range.begin + marks_to_read, res);

					ranges_to_read.push_back(MarkRange(range.begin, range.begin + marks_to_read));
					space_left -= marks_to_read;
					range.begin += marks_to_read;
					if (range.begin == range.end)
						remaining_mark_ranges.pop_back();
				}
				pre_reader->fillMissingColumns(res);

				/// Вычислим выражение в PREWHERE.
				prewhere_actions->execute(res);

				ColumnPtr column = res.getByName(prewhere_column).column;

				/** Если фильтр - константа (например, написано PREWHERE 1),
				*  то либо вернём пустой блок, либо вернём блок без изменений.
				*/
				if (ColumnConstUInt8 * column_const = dynamic_cast<ColumnConstUInt8 *>(&*column))
				{
					if (!column_const->getData())
					{
						res.clear();
						return res;
					}

					for (size_t i = 0; i < ranges_to_read.size(); ++i){
						const MarkRange & range = ranges_to_read[i];

						reader->readRange(range.begin, range.end, res);
					}
				}
				else if (ColumnUInt8 * column_vec = dynamic_cast<ColumnUInt8 *>(&*column))
				{
					size_t index_granularity = storage.index_granularity;

					const IColumn::Filter & pre_filter = column_vec->getData();
					IColumn::Filter post_filter(pre_filter.size());

					/// Прочитаем в нужных отрезках остальные столбцы и составим для них свой фильтр.
					size_t pre_filter_pos = 0;
					size_t post_filter_pos = 0;
					for (size_t i = 0; i < ranges_to_read.size(); ++i){
						const MarkRange & range = ranges_to_read[i];

						size_t begin = range.begin;
						size_t pre_filter_begin_pos = pre_filter_pos;
						for (size_t mark = range.begin; mark <= range.end; ++mark)
						{
							UInt8 nonzero = 0;
							if (mark != range.end)
							{
								size_t limit = std::min(pre_filter.size(), pre_filter_pos + index_granularity);
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
							pre_filter_pos = std::min(pre_filter_pos + index_granularity, pre_filter.size());
						}
					}

					if (!post_filter_pos)
					{
						res.clear();
						continue;
					}

					post_filter.resize(post_filter_pos);

					/// Отфильтруем столбцы, относящиеся к PREWHERE, используя pre_filter,
					///  остальные столбцы - используя post_filter.
					size_t rows = 0;
					for (size_t i = 0; i < res.columns(); ++i)
					{
						ColumnWithNameAndType & column = res.getByPosition(i);
						if (column.name == prewhere_column)
							continue;
						column.column = column.column->filter(column_name_set.count(column.name) ? post_filter : pre_filter);
						rows = column.column->size();
					}

					/// Заменим столбец со значением условия из PREWHERE на константу.
					res.getByName(prewhere_column).column = new ColumnConstUInt8(rows, 1);
				}
				else
					throw Exception("Illegal type " + column->getName() + " of column for filter. Must be ColumnUInt8 or ColumnConstUInt8.", ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER);

				reader->fillMissingColumns(res);
			}
			while (!remaining_mark_ranges.empty() && !res && !isCancelled());
		}
		else
		{
			size_t space_left = std::max(1LU, block_size / storage.index_granularity);
			while (!remaining_mark_ranges.empty() && space_left)
			{
				MarkRange & range = remaining_mark_ranges.back();

				size_t marks_to_read = std::min(range.end - range.begin, space_left);
				reader->readRange(range.begin, range.begin + marks_to_read, res);

				space_left -= marks_to_read;
				range.begin += marks_to_read;
				if (range.begin == range.end)
					remaining_mark_ranges.pop_back();
			}

			reader->fillMissingColumns(res);
		}

		if (remaining_mark_ranges.empty())
		{
			/** Закрываем файлы (ещё до уничтожения объекта).
				* Чтобы при создании многих источников, но одновременном чтении только из нескольких,
				*  буферы не висели в памяти.
				*/
			reader = NULL;
		}

		return res;
	}
	
private:
	const String path;
	size_t block_size;
	Names column_names;
	NameSet column_name_set;
	Names pre_column_names;
	StorageMergeTree & storage;
	const StorageMergeTree::DataPartPtr owned_data_part;	/// Кусок не будет удалён, пока им владеет этот объект.
	MarkRanges all_mark_ranges; /// В каких диапазонах засечек читать. В порядке возрастания номеров.
	MarkRanges remaining_mark_ranges; /// В каких диапазонах засечек еще не прочли.
									  /// В порядке убывания номеров, чтобы можно было выбрасывать из конца.
	bool use_uncompressed_cache;
	Poco::SharedPtr<MergeTreeReader> reader;
	Poco::SharedPtr<MergeTreeReader> pre_reader;
	ExpressionActionsPtr prewhere_actions;
	String prewhere_column;
};

}
