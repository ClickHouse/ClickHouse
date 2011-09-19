#include <iterator>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/Core/Block.h>


namespace DB
{


Block::Block(const Block & other)
{
	*this = other;
}


Block & Block::operator= (const Block & other)
{
	data = other.data;
	rebuildIndexByPosition();
	index_by_name.clear();
	
	for (IndexByName_t::const_iterator it = other.index_by_name.begin(); it != other.index_by_name.end(); ++it)
	{
		Container_t::iterator value = data.begin();
		std::advance(value, std::distance(const_cast<Block&>(other).data.begin(), it->second));
		index_by_name[it->first] = value;
	}
	
	return *this;
}


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
		throw Exception("Position out of bound in Block::insert(), max position = "
			+ Poco::NumberFormatter::format(index_by_position.size()), ErrorCodes::POSITION_OUT_OF_BOUND);
		
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


void Block::insertUnique(const ColumnWithNameAndType & elem)
{
	if (index_by_name.end() == index_by_name.find(elem.name))
		insert(elem);
}


void Block::erase(size_t position)
{
	if (position >= index_by_position.size())
		throw Exception("Position out of bound in Block::erase(), max position = "
			+ Poco::NumberFormatter::format(index_by_position.size()), ErrorCodes::POSITION_OUT_OF_BOUND);
		
	Container_t::iterator it = index_by_position[position];
	index_by_name.erase(index_by_name.find(it->name));
	data.erase(it);
	rebuildIndexByPosition();
}


ColumnWithNameAndType & Block::getByPosition(size_t position)
{
	if (position >= index_by_position.size())
		throw Exception("Position " + Poco::NumberFormatter::format(position)
			+ " is out of bound in Block::getByPosition(), max position = "
			+ Poco::NumberFormatter::format(index_by_position.size() - 1)
			+ ", there are columns: " + dumpNames(), ErrorCodes::POSITION_OUT_OF_BOUND);
		
	return *index_by_position[position];
}


const ColumnWithNameAndType & Block::getByPosition(size_t position) const
{
	if (position >= index_by_position.size())
		throw Exception("Position " + Poco::NumberFormatter::format(position)
			+ " is out of bound in Block::getByPosition(), max position = "
			+ Poco::NumberFormatter::format(index_by_position.size() - 1)
			+ ", there are columns: " + dumpNames(), ErrorCodes::POSITION_OUT_OF_BOUND);
		
	return *index_by_position[position];
}


ColumnWithNameAndType & Block::getByName(const std::string & name)
{
	IndexByName_t::const_iterator it = index_by_name.find(name);
	if (index_by_name.end() == it)
		throw Exception("Not found column " + name + " in block. There are only columns: " + dumpNames()
			, ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);

	return *it->second;
}


const ColumnWithNameAndType & Block::getByName(const std::string & name) const
{
	IndexByName_t::const_iterator it = index_by_name.find(name);
	if (index_by_name.end() == it)
		throw Exception("Not found column " + name + " in block. There are only columns: " + dumpNames()
			, ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);

	return *it->second;
}


size_t Block::getPositionByName(const std::string & name) const
{
	IndexByName_t::const_iterator it = index_by_name.find(name);
	if (index_by_name.end() == it)
		throw Exception("Not found column " + name + " in block. There are only columns: " + dumpNames()
			, ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);

	return std::distance(const_cast<Container_t &>(data).begin(), it->second);
}


size_t Block::rows() const
{
	size_t res = 0;
	for (Container_t::const_iterator it = data.begin(); it != data.end(); ++it)
	{
		size_t size = it->column->size();

		if (size == 0)
			throw Exception("Empty column " + it->name + " in block.", ErrorCodes::EMPTY_COLUMN_IN_BLOCK);

		if (res != 0 && size != res)
			throw Exception("Sizes of columns doesn't match: "
				+ data.begin()->name + ": " + Poco::NumberFormatter::format(res)
				+ ", " + it->name + ": " + Poco::NumberFormatter::format(size)
				, ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		res = size;
	}

	return res;
}


size_t Block::columns() const
{
	return data.size();
}


std::string Block::dumpNames() const
{
	std::stringstream res;
	for (Container_t::const_iterator it = data.begin(); it != data.end(); ++it)
	{
		if (it != data.begin())
			res << ", ";
		res << it->name;
	}
	return res.str();
}


Block Block::cloneEmpty() const
{
	Block res;

	for (Container_t::const_iterator it = data.begin(); it != data.end(); ++it)
		res.insert(it->cloneEmpty());

	return res;
}


}
