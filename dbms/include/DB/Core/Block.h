#pragma once

#include <vector>
#include <map>
#include <list>

#include <DB/Core/ColumnWithNameAndType.h>
#include <DB/Core/NamesAndTypes.h>
#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>
#include "ColumnsWithNameAndType.h"


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
	Block() {}
	
	/// нужны, чтобы правильно скопировались индексы
	Block(const Block & other);
	Block & operator= (const Block & other);

	/// вставить столбец в заданную позицию
	void insert(size_t position, const ColumnWithNameAndType & elem);
	/// вставить столбец в конец
	void insert(const ColumnWithNameAndType & elem);
	/// вставить столбец в конец, если столбца с таким именем ещё нет
	void insertUnique(const ColumnWithNameAndType & elem);
	/// удалить столбец в заданной позиции
	void erase(size_t position);
	/// удалить столбец с заданным именем
	void erase(const String & name);

	ColumnWithNameAndType & getByPosition(size_t position);
	const ColumnWithNameAndType & getByPosition(size_t position) const;

	ColumnWithNameAndType & unsafeGetByPosition(size_t position) { return *index_by_position[position]; }
	const ColumnWithNameAndType & unsafeGetByPosition(size_t position) const { return *index_by_position[position]; }

	ColumnWithNameAndType & getByName(const std::string & name);
	const ColumnWithNameAndType & getByName(const std::string & name) const;

	bool has(const std::string & name) const;

	size_t getPositionByName(const std::string & name) const;

	ColumnsWithNameAndType getColumns() const;
	NamesAndTypesList getColumnsList() const;

	/** Возвращает количество строк в блоке.
	  * Заодно проверяет, что все столбцы содержат одинаковое число значений.
	  */
	size_t rows() const;

	/** То же самое, но без проверки - берёт количество строк из первого столбца, если он есть или возвращает 0.
	  */
	size_t rowsInFirstColumn() const;
	
	size_t columns() const;

	/// Приблизительное количество байт в оперативке - для профайлинга.
	size_t bytes() const;

	operator bool() const { return !data.empty(); }
	bool operator!() const { return data.empty(); }

	/** Получить список имён столбцов через запятую. */
	std::string dumpNames() const;

	/** Получить такой же блок, но пустой. */
	Block cloneEmpty() const;
};

typedef std::vector<Block> Blocks;
typedef std::list<Block> BlocksList;

}
