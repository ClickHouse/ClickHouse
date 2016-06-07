#pragma once

#include <queue>
#include <Poco/TemporaryFile.h>

#include <common/logger_useful.h>

#include <DB/Core/SortDescription.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/DataStreams/NativeBlockInputStream.h>

#include <DB/IO/ReadBufferFromFile.h>
#include <DB/IO/CompressedReadBuffer.h>


namespace DB
{

/** Соединяет поток сортированных по отдельности блоков в сортированный целиком поток.
  * Если данных для сортировки слишком много - может использовать внешнюю сортировку, с помощью временных файлов.
  */

/** Часть реализации. Сливает набор готовых (уже прочитанных откуда-то) блоков.
  * Возвращает результат слияния в виде потока блоков не более max_merged_block_size строк.
  */
class MergeSortingBlocksBlockInputStream : public IProfilingBlockInputStream
{
public:
	/// limit - если не 0, то можно выдать только первые limit строк в сортированном порядке.
	MergeSortingBlocksBlockInputStream(Blocks & blocks_, SortDescription & description_,
		size_t max_merged_block_size_, size_t limit_ = 0);

	String getName() const override { return "MergeSortingBlocks"; }
	String getID() const override { return getName(); }

protected:
	Block readImpl() override;

private:
	Blocks & blocks;
	SortDescription description;
	size_t max_merged_block_size;
	size_t limit;
	size_t total_merged_rows = 0;

	using CursorImpls = std::vector<SortCursorImpl>;
	CursorImpls cursors;

	bool has_collation = false;

	std::priority_queue<SortCursor> queue;
	std::priority_queue<SortCursorWithCollation> queue_with_collation;

	/** Делаем поддержку двух разных курсоров - с Collation и без.
	 *  Шаблоны используем вместо полиморфных SortCursor'ов и вызовов виртуальных функций.
	 */
	template <typename TSortCursor>
	Block mergeImpl(std::priority_queue<TSortCursor> & queue);
};


class MergeSortingBlockInputStream : public IProfilingBlockInputStream
{
public:
	/// limit - если не 0, то можно выдать только первые limit строк в сортированном порядке.
	MergeSortingBlockInputStream(BlockInputStreamPtr input_, SortDescription & description_,
		size_t max_merged_block_size_, size_t limit_,
		size_t max_bytes_before_external_sort_, const std::string & tmp_path_)
		: description(description_), max_merged_block_size(max_merged_block_size_), limit(limit_),
		max_bytes_before_external_sort(max_bytes_before_external_sort_), tmp_path(tmp_path_)
	{
		children.push_back(input_);
	}

	String getName() const override { return "MergeSorting"; }

	String getID() const override
	{
		std::stringstream res;
		res << "MergeSorting(" << children.back()->getID();

		for (size_t i = 0; i < description.size(); ++i)
			res << ", " << description[i].getID();

		res << ")";
		return res.str();
	}

protected:
	Block readImpl() override;

private:
	SortDescription description;
	size_t max_merged_block_size;
	size_t limit;

	size_t max_bytes_before_external_sort;
	const std::string tmp_path;

	Logger * log = &Logger::get("MergeSortingBlockInputStream");

	Blocks blocks;
	size_t sum_bytes_in_blocks = 0;
	std::unique_ptr<IBlockInputStream> impl;

	/// Всё ниже - для внешней сортировки.
	std::vector<std::unique_ptr<Poco::TemporaryFile>> temporary_files;

	/// Для чтения сброшенных во временный файл данных.
	struct TemporaryFileStream
	{
		ReadBufferFromFile file_in;
		CompressedReadBuffer compressed_in;
		BlockInputStreamPtr block_in;

		TemporaryFileStream(const std::string & path)
			: file_in(path), compressed_in(file_in), block_in(new NativeBlockInputStream(compressed_in)) {}
	};

	std::vector<std::unique_ptr<TemporaryFileStream>> temporary_inputs;

	BlockInputStreams inputs_to_merge;
};

}
