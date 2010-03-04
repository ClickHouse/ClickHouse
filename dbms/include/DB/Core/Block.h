#ifndef DBMS_CORE_BLOCK_H
#define DBMS_CORE_BLOCK_H

#include <vector>
#include <map>
#include <list>

#include <DB/Core/ColumnWithNameAndType.h>


namespace DB
{

/** Тип данных для представления подмножества строк и столбцов в оперативке.
  * Содержит также метаданные (типы) столбцов и их имена.
  * Позволяет вставлять, удалять столбцы в любом порядке, менять порядок столбцов.
  */

class Block
{
public:
	typedef std::list<ColumnWithNameAndType> Container_t;
	typedef std::vector<Container_t::iterator> IndexByPosition_t;
	typedef std::map<String, Container_t::iterator> IndexByName_t;
	
private:
	Container_t data;
	IndexByPosition_t index_by_position;
	IndexByName_t index_by_name;

	void rebuildIndexByPosition();
	
public:
	void insert(size_t position, const ColumnWithNameAndType & elem);
	void erase(size_t position);

	ColumnWithNameAndType & getByPosition(size_t position);
	const ColumnWithNameAndType & getByPosition(size_t position) const;

	ColumnWithNameAndType & getByName(const std::string & name);
	const ColumnWithNameAndType & getByName(const std::string & name) const;

	/** Возвращает количество строк в блоке.
	  * Заодно проверяет, что все столбцы кроме констант (которые содержат единственное значение),
	  *  содержат одинаковое число значений.
	  */
	size_t rows() const;
	size_t columns() const;

	operator bool() const { return !data.empty(); }
	bool operator!() const { return data.empty(); }
};

}

#endif
