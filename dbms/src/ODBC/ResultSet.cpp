#include "ResultSet.h"
#include "Statement.h"
#include "Log.h"

#include <Poco/Types.h>


void ResultSet::init(Statement & statement_)
{
	statement = &statement_;

	if (in().peek() == EOF)
		return;

	/// Заголовок: количество столбцов, их имена и типы.
	Poco::UInt64 num_columns = 0;
	readSize(num_columns, in());

	if (!num_columns)
		return;

	columns_info.resize(num_columns);
	for (size_t i = 0; i < num_columns; ++i)
	{
		readString(columns_info[i].name, in());
		readString(columns_info[i].type, in());

		columns_info[i].type_without_parameters = columns_info[i].type;
		auto pos = columns_info[i].type_without_parameters.find('(');
		if (std::string::npos != pos)
			columns_info[i].type_without_parameters.resize(pos);
	}

	readNextBlock();

	/// Отображаемые размеры столбцов, вычисляются по первому блоку.
	for (const auto & row : current_block.data)
		for (size_t i = 0; i < num_columns; ++i)
			columns_info[i].display_size = std::max(row.data[i].data.size(), columns_info[i].display_size);

	for (const auto & column : columns_info)
		LOG(column.name << ", " << column.type << ", " << column.display_size);
}


std::istream & ResultSet::in()
{
	return *statement->in;
}


bool ResultSet::readNextBlock()
{
	static constexpr auto max_block_size = 8192;

	current_block.data.clear();
	current_block.data.reserve(max_block_size);

	for (size_t i = 0; i < max_block_size && in().peek() != EOF; ++i)
	{
		size_t num_columns = getNumColumns();
		Row row(num_columns);

		for (size_t j = 0; j < num_columns; ++j)
			readString(row.data[j].data, in());

		current_block.data.emplace_back(std::move(row));
	}

	iterator = current_block.data.begin();
	return !current_block.data.empty();
}
