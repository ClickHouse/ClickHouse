#pragma once

#include <vector>
#include <Poco/NumberParser.h>
#include <Poco/Types.h>
#include <sqltypes.h>

#include "ReadHelpers.h"

class Statement;


class Field
{
public:
	std::string data;

	Poco::UInt64 getUInt() const{ return Poco::NumberParser::parseUnsigned64(data); }
	Poco::Int64 getInt() const	{ return Poco::NumberParser::parse64(data); }
	float getFloat() const		{ return Poco::NumberParser::parseFloat(data); }
	double getDouble() const	{ return Poco::NumberParser::parseFloat(data); }

	SQL_DATE_STRUCT getDate() const
	{
		if (data.size() != 10)
			throw std::runtime_error("Cannot interpret '" + data + "' as Date");

		SQL_DATE_STRUCT res;
		res.year = (data[0] - '0') * 1000 + (data[1] - '0') * 100 + (data[2] - '0') * 10 + (data[3] - '0');
		res.month = (data[5] - '0') * 10 + (data[6] - '0');
		res.day = (data[8] - '0') * 10 + (data[9] - '0');
		return res;
	}

	SQL_TIMESTAMP_STRUCT getDateTime() const
	{
		if (data.size() != 19)
			throw std::runtime_error("Cannot interpret '" + data + "' as DateTime");

		SQL_TIMESTAMP_STRUCT res;
		res.year = (data[0] - '0') * 1000 + (data[1] - '0') * 100 + (data[2] - '0') * 10 + (data[3] - '0');
		res.month = (data[5] - '0') * 10 + (data[6] - '0');
		res.day = (data[8] - '0') * 10 + (data[9] - '0');
		res.hour = (data[11] - '0') * 10 + (data[12] - '0');
		res.minute = (data[14] - '0') * 10 + (data[15] - '0');
		res.second = (data[17] - '0') * 10 + (data[18] - '0');
		res.fraction = 0;
		return res;
	}
};


class Row
{
public:
	Row() {}
	Row(size_t num_columns) : data(num_columns) {}

	std::vector<Field> data;

	operator bool() { return !data.empty(); }
};


class Block
{
public:
	using Data = std::vector<Row>;
	Data data;
};


struct ColumnInfo
{
	std::string name;
	std::string type;
	std::string type_without_parameters;
	size_t display_size = 0;
};


class ResultSet
{
public:
	ResultSet() {}

	void init(Statement & statement_);

	bool empty() const { return columns_info.empty(); }
	size_t getNumColumns() const { return columns_info.size(); }
	const ColumnInfo & getColumnInfo(size_t i) const { return columns_info.at(i); }
	size_t getNumRows() const { return rows; }

	Row fetch()
	{
		if (empty())
			return {};

		if (current_block.data.end() == iterator && !readNextBlock())
			return {};

		++rows;
		const Row & row = *iterator;
		++iterator;
		return row;
	}

private:
	Statement * statement = nullptr;

	std::vector<ColumnInfo> columns_info;
	Block current_block;
	Block::Data::const_iterator iterator;
	size_t rows = 0;

	std::istream & in();

	void throwIncompleteResult() const
	{
		throw std::runtime_error("Incomplete result received.");
	}

	bool readNextBlock();
};
