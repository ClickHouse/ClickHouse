#pragma once

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/Interpreters/HashSet.h>
#include <DB/Interpreters/AggregationCommon.h>
#include <DB/Interpreters/Limits.h>


namespace DB
{

/** Из потока блоков оставляет только уникальные строки.
  * Для реализации SELECT DISTINCT ... .
  * Если указан ненулевой limit - прекращает выдавать строки после того, как накопилось limit строк
  *  - для оптимизации SELECT DISTINCT ... LIMIT ... .
  *
  * TODO: Ограничение на максимальное количество строк в множестве.
  */
class DistinctBlockInputStream : public IProfilingBlockInputStream
{
public:
	DistinctBlockInputStream(BlockInputStreamPtr input_, const Limits & limits, size_t limit_ = 0)
		: limit(limit_),
		max_rows(limits.max_rows_in_distinct),
		max_bytes(limits.max_bytes_in_distinct),
		overflow_mode(limits.distinct_overflow_mode)
	{
		children.push_back(input_);
	}

	String getName() const { return "DistinctBlockInputStream"; }

	String getID() const
	{
		std::stringstream res;
		res << "Distinct(" << children.back()->getID() << ")";
		return res.str();
	}

protected:
	Block readImpl()
	{
		/// Пока не встретится блок, после фильтрации которого что-нибудь останется, или поток не закончится.
		while (1)
		{
			/// Если уже прочитали достаточно строк - то больше читать не будем.
			if (limit && set.size() >= limit)
				return Block();

			Block block = children[0]->read();

			if (!block)
				return Block();
			
			size_t rows = block.rows();
			size_t columns = block.columns();

			ConstColumnPlainPtrs column_ptrs(columns);
			for (size_t i = 0; i < columns; ++i)
				column_ptrs[i] = block.getByPosition(i).column;

			/// Будем фильтровать блок, оставляя там только строки, которых мы ещё не видели.
			IColumn::Filter filter(rows);

			size_t old_set_size = set.size();

			for (size_t i = 0; i < rows; ++i)
			{
				/** Уникальность строк будем отслеживать с помощью множества значений SipHash128.
				  * Делается несколько допущений.
				  * 1. Допускается неточная работа в случае коллизий SipHash128.
				  * 2. Допускается неточная работа, если строковые поля содержат нулевые байты.
				  * 3. Не поддерживаются массивы.
				  *
				  * Для оптимизации, можно добавить другие методы из Set.h.
				  */

				union
				{
					UInt128 key;
					char char_key[16];
				};

				SipHash hash;

				for (size_t j = 0; j < columns; ++j)
				{
					StringRef data = column_ptrs[j]->getDataAtWithTerminatingZero(i);
					hash.update(data.data, data.size);
				}

				hash.final(char_key);

				/// Если вставилось в множество - строчку оставляем, иначе - удаляем.
				filter[i] = set.insert(key).second;
				
				if (!checkLimits())
				{
					if (overflow_mode == Limits::THROW)
						throw Exception("DISTINCT-Set size limit exceeded."
							" Rows: " + Poco::NumberFormatter::format(set.size()) +
							", limit: " + Poco::NumberFormatter::format(max_rows) +
							". Bytes: " + Poco::NumberFormatter::format(set.getBufferSizeInBytes()) +
							", limit: " + Poco::NumberFormatter::format(max_bytes) + ".",
							ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
					
					if (overflow_mode == Limits::BREAK)
						break;
					
					throw Exception("Logical error: unknown overflow mode", ErrorCodes::LOGICAL_ERROR);
				}

				if (limit && set.size() == limit)
					break;
			}

			/// Если ни одной новой строки не было в блоке - перейдём к следующему блоку.
			if (set.size() == old_set_size)
				continue;

			for (size_t i = 0; i < columns; ++i)
				block.getByPosition(i).column = block.getByPosition(i).column->filter(filter);

			return block;
		}
	}

private:
	
	bool checkLimits() const
	{
		if (max_rows && set.size() > max_rows)
			return false;
		if (max_bytes && set.getBufferSizeInBytes() > max_bytes)
			return false;
		return true;
	}
	
	size_t limit;
	
	/// Ограничения на максимальный размер множества
	size_t max_rows;
	size_t max_bytes;
	Limits::OverflowMode overflow_mode;

	typedef HashSet<UInt128, UInt128Hash, UInt128ZeroTraits> SetHashed;
	SetHashed set;
};

}
