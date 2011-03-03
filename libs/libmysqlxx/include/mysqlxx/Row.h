#ifndef MYSQLXX_ROW_H
#define MYSQLXX_ROW_H

#include <mysqlxx/Types.h>
#include <mysqlxx/String.h>
#include <mysqlxx/ResultBase.h>

#include <iostream>

namespace mysqlxx
{

class ResultBase;

class Row
{
public:
	Row() : row(NULL), res(NULL)
	{
	}
	
	Row(MYSQL_ROW row_, ResultBase * res_)
		: row(row_), res(res_)
	{
		lengths = mysql_fetch_lengths(&res->getRes());
	}

	String operator[] (int n)
	{
		std::cerr << lengths[0] << std::endl;
		return String(row[n], lengths[n]);
	}

	String operator[] (const char * name)
	{
		std::cerr << "???" << std::endl;
		unsigned n = res->getNumFields();
		MYSQL_FIELDS fields = res->getFields();

		for (unsigned i = 0; i < n; ++i)
			if (!strcmp(name, fields[i].name))
				return operator[](i);

		throw Exception(std::string("Unknown column ") + name);
	}

	String at(size_t n)
	{
		return operator[](n);
	}

	operator bool() 	{ return row; }
	bool operator !() 	{ return !row; }

private:
	MYSQL_ROW row;
	MYSQL_LENGTHS lengths;
	ResultBase * res;
};

}

#endif
