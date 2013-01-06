#pragma once

#include <mysqlxx/Types.h>
#include <mysqlxx/String.h>
#include <mysqlxx/ResultBase.h>


namespace mysqlxx
{

class ResultBase;


/** Строка результата.
  * В отличие от mysql++,
  *  представляет собой обёртку над MYSQL_ROW (char**), ссылается на ResultBase, не владеет сам никакими данными.
  * Это значит, что если будет уничтожен объект результата или соединение,
  *  или будет задан следующий запрос, то Row станет некорректным.
  * При использовании UseQueryResult, в памяти хранится только одна строка результата,
  *  это значит, что после чтения следующей строки, предыдущая становится некорректной.
  */
class Row
{
public:
	/** Для возможности отложенной инициализации. */
	Row() : row(NULL), res(NULL)
	{
	}

	/** Для того, чтобы создать Row, используйте соответствующие методы UseQueryResult или StoreQueryResult. */
	Row(MYSQL_ROW row_, ResultBase * res_, MYSQL_LENGTHS lengths_)
		: row(row_), res(res_), lengths(lengths_)
	{
	}

	/** Получить значение по индексу.
	  * Здесь используется int, а не unsigned, чтобы не было неоднозначности с тем же методом, принимающим const char *.
	  */
	String operator[] (int n) const
	{
		if (unlikely(static_cast<size_t>(n) >= res->getNumFields()))
			throw Exception("Index of column is out of range.");
		return String(row[n], lengths[n], res);
	}

	/** Получить значение по имени. Слегка менее эффективно. */
	String operator[] (const char * name) const
	{
		unsigned n = res->getNumFields();
		MYSQL_FIELDS fields = res->getFields();

		for (unsigned i = 0; i < n; ++i)
			if (!strcmp(name, fields[i].name))
				return operator[](i);

		throw Exception(std::string("Unknown column ") + name);
	}

	/** Получить значение по индексу. */
	String at(size_t n) const
	{
		return operator[](n);
	}

	/** Количество столбцов. */
	size_t size() const { return res->getNumFields(); }

	/** Является ли пустым? Такой объект используется, чтобы обозначить конец результата
	  * при использовании UseQueryResult. Или это значит, что объект не инициализирован.
	  * Вы можете использовать вместо этого преобразование в bool.
	  */
	bool empty() const { return row == NULL; }

private:
	MYSQL_ROW row;
	ResultBase * res;
	MYSQL_LENGTHS lengths;
};



}
