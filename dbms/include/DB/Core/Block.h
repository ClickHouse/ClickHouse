#pragma once

#include <vector>
#include <map>
#include <initializer_list>

#include <DB/Common/Exception.h>
#include <DB/Core/BlockInfo.h>
#include <DB/Core/NamesAndTypes.h>
#include <DB/Core/ColumnWithTypeAndName.h>
#include <DB/Core/ColumnsWithTypeAndName.h>
#include <DB/Core/ColumnNumbers.h>
#include <DB/Common/Exception.h>



namespace DB
{

/** Тип данных для представления подмножества строк и столбцов в оперативке.
  * Содержит также метаданные (типы) столбцов и их имена.
  * Позволяет вставлять, удалять столбцы в любом порядке, менять порядок столбцов.
  */

class Context;

class Block
{
private:
	using Container = std::vector<ColumnWithTypeAndName>;
	using IndexByName = std::map<String, size_t>;

	Container data;
	IndexByName index_by_name;

public:
	BlockInfo info;

	Block() = default;
	Block(std::initializer_list<ColumnWithTypeAndName> il) : data{il}
	{
		size_t i = 0;
		for (const auto & elem : il)
		{
			index_by_name[elem.name] = i;
			++i;
		}
	}

	/// вставить столбец в заданную позицию
	void insert(size_t position, const ColumnWithTypeAndName & elem);
	void insert(size_t position, ColumnWithTypeAndName && elem);
	/// вставить столбец в конец
	void insert(const ColumnWithTypeAndName & elem);
	void insert(ColumnWithTypeAndName && elem);
	/// вставить столбец в конец, если столбца с таким именем ещё нет
	void insertUnique(const ColumnWithTypeAndName & elem);
	void insertUnique(ColumnWithTypeAndName && elem);
	/// удалить столбец в заданной позиции
	void erase(size_t position);
	/// удалить столбец с заданным именем
	void erase(const String & name);
	/// Добавляет в блок недостающие столбцы со значениями по-умолчанию
	void addDefaults(const NamesAndTypesList & required_columns);

	/// References are invalidated after calling functions above.

	ColumnWithTypeAndName & getByPosition(size_t position);
	const ColumnWithTypeAndName & getByPosition(size_t position) const;

	ColumnWithTypeAndName & unsafeGetByPosition(size_t position) { return data[position]; }
	const ColumnWithTypeAndName & unsafeGetByPosition(size_t position) const { return data[position]; }

	ColumnWithTypeAndName & getByName(const std::string & name);
	const ColumnWithTypeAndName & getByName(const std::string & name) const;

	bool has(const std::string & name) const;

	size_t getPositionByName(const std::string & name) const;

	ColumnsWithTypeAndName getColumns() const;
	NamesAndTypesList getColumnsList() const;

	/** Возвращает количество строк в блоке.
	  * Заодно проверяет, что все столбцы содержат одинаковое число значений.
	  */
	size_t rows() const;

	/** То же самое, но без проверки - берёт количество строк из первого столбца, если он есть или возвращает 0.
	  */
	size_t rowsInFirstColumn() const;

	size_t columns() const { return data.size(); }

	/// Приблизительное количество байт в оперативке - для профайлинга.
	size_t bytes() const;

	operator bool() const { return !data.empty(); }
	bool operator!() const { return data.empty(); }

	/** Получить список имён столбцов через запятую. */
	std::string dumpNames() const;

	/** Список имен, типов и длин столбцов. Предназначен для отладки. */
	std::string dumpStructure() const;

	/** Получить такой же блок, но пустой. */
	Block cloneEmpty() const;

	/** Получить блок со столбцами, переставленными в порядке их имён. */
	Block sortColumns() const;

	/** Заменяет столбцы смещений внутри вложенных таблиц на один общий для таблицы.
	 *  Кидает исключение, если эти смещения вдруг оказались неодинаковы.
	 */
	void optimizeNestedArraysOffsets();
	/** Тоже самое, только без замены смещений. */
	void checkNestedArraysOffsets() const;

	void clear();
	void swap(Block & other) noexcept;

	/** Some column implementations (ColumnArray) may have shared parts between different columns
	  * (common array sizes of elements of nested data structures).
	  * Before doing mutating operations on such columns, you must unshare that parts.
	  * Also unsharing columns, if whole columns are shared_ptrs pointing to same instances.
	  */
	void unshareColumns();

private:
	void eraseImpl(size_t position);
};

using Blocks = std::vector<Block>;
using BlocksList = std::list<Block>;


/// Сравнить типы столбцов у блоков. Порядок столбцов имеет значение. Имена не имеют значения.
bool blocksHaveEqualStructure(const Block & lhs, const Block & rhs);

/// Calculate difference in structure of blocks and write description into output strings.
void getBlocksDifference(const Block & lhs, const Block & rhs, std::string & out_lhs_diff, std::string & out_rhs_diff);


/** Дополнительные данные к блокам. Они пока нужны только для запроса
  * DESCRIBE TABLE с Distributed-таблицами.
  */
struct BlockExtraInfo
{
	BlockExtraInfo() {}
	operator bool() const { return is_valid; }
	bool operator!() const { return !is_valid; }

	std::string host;
	std::string resolved_address;
	std::string user;
	UInt16 port = 0;

	bool is_valid = false;
};

}
