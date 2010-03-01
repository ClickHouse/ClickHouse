#include <DB/Core/Block.h>


namespace DB
{

void Block::rebuildIndexByPosition()
{
	index_by_position.resize(columns.size());
	size_t pos = 0;
	for (Container_t::iterator it = columns.begin(); it != columns.end(); ++it, ++pos)
		index_by_position[pos] = it;
}


void Block::insert(size_t position, const ColumnWithNameAndType & elem)
{
	Container_t::iterator it = columns.insert(index_by_position[position], elem);
	rebuildIndexByPosition();
	index_by_name[elem.name] = it; 
}


void Block::erase(size_t position)
{
	Container_t::iterator it = index_by_position[position];
	index_by_name.erase(index_by_name.find(it->name));
	columns.erase(it);
	rebuildIndexByPosition();
}


Block::ColumnWithNameAndType & Block::getByPosition(size_t position)
{
	return *index_by_position[position];
}


const Block::ColumnWithNameAndType & Block::getByPosition(size_t position) const
{
	return *index_by_position[position];
}


Block::ColumnWithNameAndType & Block::getByName(const std::string & name)
{
	return *index_by_name[name];
}


const Block::ColumnWithNameAndType & Block::getByName(const std::string & name) const
{
	return *index_by_name[name];
}

}

#endif
