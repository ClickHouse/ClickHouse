#include <DB/DataStreams/MergeSortingBlockInputStream.h>


namespace DB
{

Block MergeSortingBlockInputStream::readImpl()
{
	/** На данный момент - очень простой алгоритм:
	  * - прочитать в оперативку все блоки;
	  * - объединять по два соседних блока;
	  */
	
	typedef std::list<Block> Blocks;
	Blocks blocks;

	while (Block block = input->read())
		blocks.push_back(block);

	if (blocks.empty())
		return Block();

	while (blocks.size() > 1)
	{
		for (Blocks::iterator it = blocks.begin(); it != blocks.end();)
		{
			Blocks::iterator next = it;
			++next;
			if (next == blocks.end())
				break;
			merge(*it, *next);
			++it;
			blocks.erase(it++);
		}
	}

	return blocks.front();
}


void MergeSortingBlockInputStream::merge(Block & left, Block & right)
{
	Block merged;

	size_t left_size = left.rows();
	size_t right_size = right.rows();

	size_t left_pos = 0;
	size_t right_pos = 0;

	typedef std::vector<const IColumn *> ConstColumns;
	typedef std::vector<IColumn *> Columns;

	/// Все столбцы блоков.
	ConstColumns left_columns;
	ConstColumns right_columns;
	Columns merged_columns;

	/// Столбцы, по которым идёт сортировка.
	ConstColumns left_sort_columns;
	ConstColumns right_sort_columns;

	size_t num_columns = left.columns();
	for (size_t i = 0; i < num_columns; ++i)
	{
		ColumnWithNameAndType col;
		col.name = left.getByPosition(i).name;
		col.type = left.getByPosition(i).type;
		col.column = left.getByPosition(i).column->cloneEmpty();
		merged.insert(col);
		
		merged_columns.push_back(&*col.column);
		left_columns.push_back(&*left.getByPosition(i).column);
		right_columns.push_back(&*right.getByPosition(i).column);
	}

	for (size_t i = 0, size = description.size(); i < size; ++i)
	{
		size_t column_number = !description[i].column_name.empty()
			? left.getPositionByName(description[i].column_name)
			: description[i].column_number;
		
		left_sort_columns.push_back(&*left.getByPosition(column_number).column);
		right_sort_columns.push_back(&*right.getByPosition(column_number).column);
	}

	/// Объединяем.
	while (right_pos < right_size || left_pos < left_size)
	{
		/// Откуда брать строку - из левого или из правого блока?
		int res = 0;
		if (right_pos == right_size)
			res = -1;
		else if (left_pos == left_size)
			res = 1;
		else
			for (size_t i = 0, size = description.size(); i < size; ++i)
				if ((res = description[i].direction * left_sort_columns[i]->compareAt(left_pos, right_pos, *right_sort_columns[i])))
					break;

		/// Вставляем строку в объединённый блок.
		if (res <= 0)
		{
			for (size_t i = 0; i < num_columns; ++i)
				merged_columns[i]->insert((*left_columns[i])[left_pos]);
			++left_pos;
		}
		else
		{
			for (size_t i = 0; i < num_columns; ++i)
				merged_columns[i]->insert((*right_columns[i])[right_pos]);
			++right_pos;
		}
	}

	left = merged;
}

}
