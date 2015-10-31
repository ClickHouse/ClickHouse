#pragma once

#include <vector>
#include <Poco/NumberParser.h>

#include "ReadHelpers.h"

class Statement;


class Field
{
public:
	std::string data;

	uint64_t getUInt() const 	{ return Poco::NumberParser::parseUnsigned64(data); }
	int64_t getInt() const		{ return Poco::NumberParser::parse64(data); }
	float getFloat() const		{ return Poco::NumberParser::parseFloat(data); }
	double getDouble() const	{ return Poco::NumberParser::parseFloat(data); }
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
		return std::move(row);
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
