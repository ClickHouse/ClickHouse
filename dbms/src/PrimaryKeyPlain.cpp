#include <Poco/File.h>
#include <Poco/FileStream.h>
#include <Poco/BinaryWriter.h>

#include <DB/PrimaryKeyPlain.h>


namespace DB
{
	
PrimaryKeyPlain::PrimaryKeyPlain(const std::string & path_, const std::string & name_)
	: path(path_),
	name(name_),
	data_file_name(path + name + ".dat"),
	offsets_file_name(path + name + ".idx"),
	free_blocks_file_name(path + name + ".blk"),
	data_file(data_file_name),
	offsets_file(offsets_file_name),
	free_blocks_file(free_blocks_file_name)
{
	/// создаём файлы, если их ещё нет
	data_file.createFile();
	offsets_file.createFile();
	free_blocks_file.createFile();

	/// прочитаем список свободных блоков
	Poco::FileInputStream free_blocks_istr(free_blocks_file_name);
	Poco::BinaryReader free_blocks_reader(free_blocks_istr);

	while (1)
	{
		Offset offset;
		free_blocks_reader >> offset.offset >> offset.size;

		if (free_blocks_istr.eof())
			break;

		if (free_blocks_istr.fail())
			throw Exception("Cannot read index file " + free_blocks_file_name
				, ErrorCodes::CANT_READ_INDEX_FILE);

		free_blocks.insert(offset);
	}
}


void PrimaryKeyPlain::addToTable(Table * table_, ColumnGroup * column_group_)
{
	PrimaryKeyBase::addToTable(table_, column_group_);

	/// прочитаем список смещений
	Poco::FileInputStream offsets_istr(offsets_file_name);
	Poco::BinaryReader offsets_reader(offsets_istr);

	while (1)
	{
		Row key;
		for (Table::ColumnNumbers::const_iterator it = table->primary_key_column_numbers->begin();
			it != table->primary_key_column_numbers->end();
			++it)
		{
			key.push_back(Field());
			table->columns->at(*it).type->deserializeBinary(key.back(), offsets_istr);
		}
		Offset offset;
		offsets_reader >> offset.offset >> offset.size;

		if (offsets_istr.eof())
			break;

		if (offsets_istr.fail())
			throw Exception("Cannot read index file " + offsets_file_name
				, ErrorCodes::CANT_READ_INDEX_FILE);

		offsets[key] = offset;
	}
}


void PrimaryKeyPlain::merge(const AggregatedRowSet & data, const ColumnMask & mask)
{
/*	std::set<Row> keys;

	for (Result::const_iterator it = data.begin(); it != data.end(); ++it)
	{
		if (data->first.size() < columns.size())
			throw Exception("Too few columns for key", ErrorCodes::TOO_FEW_COLUMNS_FOR_KEY);

		Row key;
		for (size_t j = 0; j < columns.size(); ++j)
			key.push_back(it->first[j]);
		
		keys.insert(key);
	}*/
}


}

