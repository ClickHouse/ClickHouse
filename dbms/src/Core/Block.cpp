#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>
#include <DB/Core/ColumnVisitors.h>

#include <DB/Core/Block.h>


namespace DB
{

void Block::rebuildIndexByPosition()
{
	index_by_position.resize(data.size());
	size_t pos = 0;
	for (Container_t::iterator it = data.begin(); it != data.end(); ++it, ++pos)
		index_by_position[pos] = it;
}


void Block::insert(size_t position, const ColumnWithNameAndType & elem)
{
	if (position >= index_by_position.size())
		throw Exception("Position out of bound in Block::insert()", ErrorCodes::POSITION_OUT_OF_BOUND);
		
	Container_t::iterator it = data.insert(index_by_position[position], elem);
	rebuildIndexByPosition();
	index_by_name[elem.name] = it; 
}


void Block::insert(const ColumnWithNameAndType & elem)
{
	Container_t::iterator it = data.insert(data.end(), elem);
	rebuildIndexByPosition();
	index_by_name[elem.name] = it; 
}


void Block::erase(size_t position)
{
	if (position >= index_by_position.size())
		throw Exception("Position out of bound in Block::erase()", ErrorCodes::POSITION_OUT_OF_BOUND);
		
	Container_t::iterator it = index_by_position[position];
	index_by_name.erase(index_by_name.find(it->name));
	data.erase(it);
	rebuildIndexByPosition();
}


ColumnWithNameAndType & Block::getByPosition(size_t position)
{
	return *index_by_position[position];
}


const ColumnWithNameAndType & Block::getByPosition(size_t position) const
{
	return *index_by_position[position];
}


ColumnWithNameAndType & Block::getByName(const std::string & name)
{
	return *index_by_name[name];
}


const ColumnWithNameAndType & Block::getByName(const std::string & name) const
{
	IndexByName_t::const_iterator it = index_by_name.find(name);
	if (index_by_name.end() == it)
		throw Exception("Not found column " + name + " in block.", ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);

	return *it->second;
}


size_t Block::rows() const
{
	size_t res = 0;
	ColumnVisitorSize visitor;
	for (Container_t::const_iterator it = data.begin(); it != data.end(); ++it)
	{
		size_t size = boost::apply_visitor(visitor, *it->column);

		if (size == 0)
			throw Exception("Empty column in block.", ErrorCodes::EMPTY_COLUMN_IN_BLOCK);

		if (size != 1 && res != 0 && size != res)
			throw Exception("Sizes of columns doesn't match.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		res = size;
	}

	return res;
}


size_t Block::columns() const
{
	return data.size();
}

}
