#pragma once

#include <queue>

#include <DB/Common/HashTable/HashTable.h>


/** Позволяет проитерироваться по данным в поряке возрастания остатка от деления хэш-функции на размер таблицы,
  *  а для элементов с одинаковым остатком - в порядке ключа.
  *
  * Для разных хэш-таблиц с одинаковым размером буфера, итерация производится в одном и том же порядке.
  * То есть, это может быть использовано, чтобы пройти одновременно несколько хэш-таблиц так,
  *  чтобы одинаковые элементы из разных хэш-таблиц обрабатывались последовательно.
  *
  * В процессе итерации, меняется расположение элементов в хэш-таблице.
  */
template <typename Table>
class HashTableMergeCursor
{
private:
	typedef typename Table::cell_type Cell;
	typedef typename Table::value_type value_type;

	Table * container;
	Cell * ptr;

	/** Начало цепочки разрешения коллизий, которая доходит до конца хэш-таблицы,
		* или buf + buf_size, если такой нет.
		*/
	Cell * begin_of_last_chain;

	size_t current_place_value;

	/** Находимся ли мы в цепочке разрешения коллизий в самом начале буфера?
		* В этом случае, цепочка может содержать ключи, которые должны были бы быть в конце буфера.
		*/
	bool in_first_chain;

	bool overlapped = false;

	size_t place(const Cell * ptr) const
	{
		return container->grower.place(ptr->getHash(*container));
	}

public:
	HashTableMergeCursor(Table * container_) : container(container_)
	{
		in_first_chain = !container->buf->isZero(*container);

		begin_of_last_chain = container->buf + container->grower.bufSize();

		/// Есть ли цепочка, доходящая до конца таблицы? И если есть, то где её начало?
		while (!begin_of_last_chain[-1].isZero(*container))
			--begin_of_last_chain;

		ptr = container->buf - 1;
		next();
	}

	void next()
	{
		++ptr;

		/// Дойдём до следующей цепочки разрешения коллизий.
		while (ptr < container->buf + container->grower.bufSize() && ptr->isZero(*container))
		{
			in_first_chain = false;
			++ptr;
		}

		/// Если мы в первой цепочке, над элементом, который должен был бы быть в конце, то пропустим все такие элементы.
		if (in_first_chain)
		{
			while (!ptr->isZero(*container) && place(ptr) * 2 > container->grower.bufSize() && ptr < container->buf + container->grower.bufSize())
				++ptr;

			while (ptr->isZero(*container) && ptr < container->buf + container->grower.bufSize())
				++ptr;
		}

		/// Если мы в последней цепочке, и, возможно, уже перешли через конец буфера, то пропустим все слишком маленькие элементы.
		if (overlapped)
		{
			if (ptr == container->buf + container->grower.bufSize())
				ptr = container->buf;

			while (!ptr->isZero(*container) && place(ptr) * 2 < container->grower.bufSize())
				++ptr;

			/// Конец.
			if (ptr->isZero(*container))
			{
				ptr = container->buf + container->grower.bufSize();
				return;
			}
		}

		if (ptr == container->buf + container->grower.bufSize())
			return;

		/// Положим под курсор минимальный элемент в цепочке разрешения коллизий, поменяв его местами с тем, что уже там есть.
		size_t min_place_value = place(ptr);
		Cell * cell_with_min_place_value = ptr;

		if (ptr < begin_of_last_chain && !overlapped)
		{
			for (Cell * lookahead = ptr + 1; !lookahead->isZero(*container); ++lookahead)
			{
				size_t place_of_lookahead = place(lookahead);

//					std::cerr << place_of_lookahead << ", " << min_place_value << std::endl;

				if (place_of_lookahead < min_place_value
					|| (place_of_lookahead == min_place_value && lookahead->less(*cell_with_min_place_value)))
				{
					min_place_value = place_of_lookahead;
					cell_with_min_place_value = lookahead;
				}
			}

			if (ptr != cell_with_min_place_value)
				ptr->swap(*cell_with_min_place_value);
		}
		else
		{
			size_t lookahead_pos = container->grower.next(ptr - container->buf + 1);
			overlapped = true;

			for (; !container->buf[lookahead_pos].isZero(*container); lookahead_pos = container->grower.next(lookahead_pos))
			{
				size_t place_of_lookahead = place(&container->buf[lookahead_pos]);
				if ((place_of_lookahead < min_place_value
					|| (place_of_lookahead == min_place_value && container->buf[lookahead_pos].less(*cell_with_min_place_value)))
					&& place_of_lookahead * 2 > container->grower.bufSize())
				{
					min_place_value = place_of_lookahead;
					cell_with_min_place_value = &container->buf[lookahead_pos];
				}
			}

			if (ptr != cell_with_min_place_value)
				ptr->swap(*cell_with_min_place_value);
		}

		current_place_value = min_place_value;
	}

	bool isValid() const
	{
		return ptr != container->buf + container->grower.bufSize();
	}

	value_type & get()
	{
		return ptr->getValue();
	}

	Cell * getCell()
	{
		return ptr;
	}

	bool operator< (const HashTableMergeCursor & rhs) const
	{
		return current_place_value < rhs.current_place_value
			|| (current_place_value == rhs.current_place_value && ptr->less(*rhs.ptr));
	}
};


/** Позволяет обработать одинаковые ключи нескольких разных хэш-таблиц.
  * Если встречает два или более одинаковых ключей, то вызывает
  *  merge_func(value_type & dst, value_type & src) с одним аргументом dst.
  * После обработки всех записей одного ключа, вызывает
  *  callback(value_type & dst).
  */
template <typename Table, typename MergeFunction, typename Callback>
void processMergedHashTables(std::vector<Table*> & tables, MergeFunction && merge_func, Callback && callback)
{
	typedef HashTableMergeCursor<Table> Cursor;
	typedef typename Table::cell_type Cell;

	size_t tables_size = tables.size();

	/// Определим максимальный размер таблиц.
	size_t max_buf_size = 0;
	for (size_t i = 0; i < tables_size; ++i)
		if (tables[i]->grower.bufSize() > max_buf_size)
			max_buf_size = tables[i]->grower.bufSize();

	std::cerr << "max_buf_size: " << max_buf_size << std::endl;

	/// Ресайзим все таблицы к этому размеру.
	for (size_t i = 0; i < tables_size; ++i)
		if (tables[i]->grower.bufSize() < max_buf_size)
			tables[i]->resize(0, max_buf_size);

	for (size_t i = 0; i < tables_size; ++i)
		std::cerr << "buf_size: " << tables[i]->grower.bufSize() << std::endl;

	typedef std::vector<Cursor> Queue;
	Queue queue;
	queue.reserve(tables_size);

	for (size_t i = 0; i < tables_size; ++i)
	{
		Cursor cursor(tables[i]);
		if (cursor.isValid())
			queue.emplace_back(cursor);
	}

	Cell * prev_cell = nullptr;
	while (!queue.empty())
	{
		size_t min_pos = 0;
		for (size_t i = 1, size = queue.size(); i < size; ++i)
			if (queue[i] < queue[min_pos])
				min_pos = i;

		Cell * current_cell = queue[min_pos].getCell();

		if (!prev_cell)
		{
			prev_cell = current_cell;
		}
		else if (!prev_cell->keyEquals(*current_cell))
		{
			callback(prev_cell->getValue());
			prev_cell = current_cell;
		}
		else
		{
			merge_func(prev_cell->getValue(), current_cell->getValue());
		}

		queue[min_pos].next();
		if (!queue[min_pos].isValid())
			queue.erase(queue.begin() + min_pos);
	}

	if (prev_cell)
		callback(prev_cell->getValue());
}
