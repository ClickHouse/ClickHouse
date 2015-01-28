#include <DB/DataStreams/MergeSortingBlockInputStream.h>
#include <DB/DataStreams/MergingSortedBlockInputStream.h>
#include <DB/DataStreams/NativeBlockOutputStream.h>
#include <DB/DataStreams/copyData.h>
#include <DB/IO/WriteBufferFromFile.h>
#include <DB/IO/CompressedWriteBuffer.h>


namespace DB
{


Block MergeSortingBlockInputStream::readImpl()
{
	/** Алгоритм:
	  * - читать в оперативку блоки из источника;
	  * - когда их становится слишком много и если возможна внешняя сортировка
	  *   - слить блоки вместе в сортированный поток и записать его во временный файл;
	  * - в конце, слить вместе все сортированные потоки из временных файлов, а также из накопившихся в оперативке блоков.
	  */

	/// Ещё не прочитали блоки.
	if (!impl)
	{
		while (Block block = children.back()->read())
		{
			blocks.push_back(block);
			sum_bytes_in_blocks += block.bytes();

			/** Если блоков стало слишком много и возможна внешняя сортировка,
			  *  то сольём вместе те блоки, которые успели накопиться, и сбросим сортированный поток во временный (сжатый) файл.
			  * NOTE. Возможно - проверка наличия свободного места на жёстком диске.
			  */
			if (max_bytes_before_external_sort && sum_bytes_in_blocks > max_bytes_before_external_sort)
			{
				temporary_files.emplace_back(new Poco::TemporaryFile(tmp_path));
				const std::string & path = temporary_files.back()->path();
				WriteBufferFromFile file_buf(path);
				CompressedWriteBuffer compressed_buf(file_buf);
				NativeBlockOutputStream block_out(compressed_buf);
				MergeSortingBlocksBlockInputStream block_in(blocks, description, max_merged_block_size, limit);

				LOG_INFO(log, "Sorting and writing part of data into temporary file " + path);
				copyData(block_in, block_out, &is_cancelled);	/// NOTE. Возможно, ограничение на потребление места на дисках.
				LOG_INFO(log, "Done writing part of data into temporary file " + path);

				blocks.clear();
				sum_bytes_in_blocks = 0;
			}
		}

		if ((blocks.empty() && temporary_files.empty()) || isCancelled())
			return Block();

		if (temporary_files.empty())
		{
			impl.reset(new MergeSortingBlocksBlockInputStream(blocks, description, max_merged_block_size, limit));
		}
		else
		{
			/// Если были сброшены временные данные в файлы.

			LOG_INFO(log, "There are " << temporary_files.size() << " temporary sorted parts to merge.");

			/// Сформируем сортированные потоки для слияния.
			for (const auto & file : temporary_files)
			{
				temporary_inputs.emplace_back(new TemporaryFileStream(file->path(), data_type_factory));
				inputs_to_merge.emplace_back(temporary_inputs.back()->block_in);
			}

			/// Оставшиеся в оперативке блоки.
			if (!blocks.empty())
				inputs_to_merge.emplace_back(new MergeSortingBlocksBlockInputStream(blocks, description, max_merged_block_size, limit));

			/// Будем сливать эти потоки.
			impl.reset(new MergingSortedBlockInputStream(inputs_to_merge, description, max_merged_block_size, limit));
		}
	}

	return impl->read();
}


MergeSortingBlocksBlockInputStream::MergeSortingBlocksBlockInputStream(
	Blocks & blocks_, SortDescription & description_, size_t max_merged_block_size_, size_t limit_)
	: blocks(blocks_), description(description_), max_merged_block_size(max_merged_block_size_), limit(limit_)
{
	Blocks nonempty_blocks;
	for (const auto & block : blocks)
	{
		if (block.rowsInFirstColumn() == 0)
			continue;

		nonempty_blocks.push_back(block);
		cursors.emplace_back(block, description);
		has_collation |= cursors.back().has_collation;
	}

	blocks.swap(nonempty_blocks);

	if (!has_collation)
	{
		for (size_t i = 0; i < cursors.size(); ++i)
			queue.push(SortCursor(&cursors[i]));
	}
	else
	{
		for (size_t i = 0; i < cursors.size(); ++i)
			queue_with_collation.push(SortCursorWithCollation(&cursors[i]));
	}
}


Block MergeSortingBlocksBlockInputStream::readImpl()
{
	if (blocks.empty())
		return Block();

	if (blocks.size() == 1)
	{
		Block res = blocks[0];
		blocks.clear();
		return res;
	}

	return !has_collation
		? mergeImpl<SortCursor>(queue)
		: mergeImpl<SortCursorWithCollation>(queue_with_collation);
}


template <typename TSortCursor>
Block MergeSortingBlocksBlockInputStream::mergeImpl(std::priority_queue<TSortCursor> & queue)
{
	Block merged = blocks[0].cloneEmpty();
	size_t num_columns = blocks[0].columns();

	ColumnPlainPtrs merged_columns;
	for (size_t i = 0; i < num_columns; ++i)	/// TODO: reserve
		merged_columns.push_back(merged.getByPosition(i).column.get());

	/// Вынимаем строки в нужном порядке и кладём в merged.
	size_t merged_rows = 0;
	while (!queue.empty())
	{
		TSortCursor current = queue.top();
		queue.pop();

		for (size_t i = 0; i < num_columns; ++i)
			merged_columns[i]->insertFrom(*current->all_columns[i], current->pos);

		if (!current->isLast())
		{
			current->next();
			queue.push(current);
		}

		++total_merged_rows;
		if (limit && total_merged_rows == limit)
		{
			blocks.clear();
			return merged;
		}

		++merged_rows;
		if (merged_rows == max_merged_block_size)
			return merged;
	}

	if (merged_rows == 0)
		merged.clear();

	return merged;
}


}
