#ifndef MYSQLXX_ROW_H
#define MYSQLXX_ROW_H

#include <mysqlxx/Types.h>
#include <mysqlxx/String.h>
#include <mysqlxx/ResultBase.h>


namespace mysqlxx
{

class ResultBase;

class Row
{
private:
	/** @brief Pointer to bool data member, for use by safe bool conversion operator.
	  * @see http://www.artima.com/cppsource/safebool.html
	  * Взято из mysql++.
	  */
	typedef MYSQL_ROW Row::*private_bool_type;
	void this_type_does_not_support_comparisons() const {}

public:
	Row() : row(NULL), res(NULL)
	{
	}
	
	Row(MYSQL_ROW row_, ResultBase * res_)
		: row(row_), res(res_)
	{
		lengths = mysql_fetch_lengths(res->getRes());
	}

	String operator[] (int n) const
	{
		return String(row[n], lengths[n]);
	}

	String operator[] (const char * name) const
	{
		unsigned n = res->getNumFields();
		MYSQL_FIELDS fields = res->getFields();

		for (unsigned i = 0; i < n; ++i)
			if (!strcmp(name, fields[i].name))
				return operator[](i);

		throw Exception(std::string("Unknown column ") + name);
	}

	String at(size_t n) const
	{
		return operator[](n);
	}

	size_t size() const { return res->getNumFields(); }
	bool empty() const { return row == NULL; }

	operator private_bool_type() const	{ return row == NULL ? NULL : &Row::row; }

private:
	MYSQL_ROW row;
	MYSQL_LENGTHS lengths;
	ResultBase * res;
};


template <typename T>
bool operator!=(const Row & lhs, const T & rhs)
{
	lhs.this_type_does_not_support_comparisons();
	return false;
}

template <typename T>
bool operator==(const Row & lhs, const T & rhs)
{
	lhs.this_type_does_not_support_comparisons();
	return false;
}

}

#endif
