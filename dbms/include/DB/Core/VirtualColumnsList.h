#pragma once

#include <Poco/SharedPtr.h>
#include <DB/Storages/StoragePtr.h>
#include <DB/Columns/ColumnConst.h>

#include <vector>
#include <map>
#include <list>
#include <algorithm>
#include <boost/iterator/iterator_concepts.hpp>

#include <DB/Core/ColumnWithNameAndType.h>
#include <DB/Core/NamesAndTypes.h>
#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>
#include <DB/Core/Names.h>

namespace DB
{

using Poco::SharedPtr;


/** Виртуальные столбцы - столбцы, предоставляемые Storage-ом, независимо от определения таблицы.
  * То есть, такие столбцы не указываются в CREATE TABLE, но доступны для SELECT-а.
  */


/// Интерфейс для одного виртуального столбца
class IVirtualColumn
{
public:
	virtual String getName() const = 0; 
	virtual void setName(const String &s) = 0;
	virtual NameAndTypePair getNameAndType() const = 0;
	virtual ColumnPtr getColumnPtr(StoragePtr storage, size_t s) const = 0;
	virtual ColumnWithNameAndType getColumn(StoragePtr storage, size_t s) const = 0;
};

/// Шаблон для одного виртуального столбца типа T
template <typename T>
class VirtualColumn : public IVirtualColumn
{
public:
	typedef T Type;
	typedef Type (*extractor_func)(StoragePtr);

private:
	String name; /// Имя столбца
	extractor_func extractor; /// Функция, которая получает значение столбца по указателю на таблицу
	DataTypePtr data_type; /// Тип данных

public:
	VirtualColumn(const String & desired_name, extractor_func extractor, DataTypePtr data_type) : name(desired_name), extractor(extractor), data_type(data_type) { }

	String getName() const { return name; }
	void setName(const String &s) { name = s; }

	NameAndTypePair getNameAndType() const { return make_pair(name, data_type); }

	/// Создает константный столбец размера s, вычисляя значение extractor на storage
	ColumnPtr getColumnPtr(StoragePtr storage, size_t s) const
	{
		return new ColumnConst<Type> (s, extractor(storage), data_type);
	}

	/// Возвращает столбец, готовый для вставки в блок
	ColumnWithNameAndType getColumn(StoragePtr storage, size_t s) const
	{
		return ColumnWithNameAndType (getColumnPtr(storage, s), data_type, name);
	}
};

/// Структура со всеми виртуальными столбцами для таблицы
class VirtualColumnList
{
private:
	std::vector< SharedPtr<IVirtualColumn> > columns; /// Список всех виртуальных столбцов

public:
	/// Добавить столбец
	void addColumn(SharedPtr<IVirtualColumn> a)
	{
		columns.push_back(a);
	}

	NamesAndTypesList getColumnsList() const
	{
		NamesAndTypesList res;
		for (size_t i = 0; i < columns.size(); ++i)
		{
			res.push_back(columns[i]->getNameAndType());
		}
		return res;
	}

	/// Вычислить суффиксы для имен столбцов, зная уже присутствующие в таблице столбцы
	void calculateNames(const NamesAndTypesList &already_in_storage)
	{
		int id = 0;
		while (true)
		{
			String suf;
			{
				int now = id;
				while (now > 0)
				{
					char cur = '0' + now % 10;
					suf = cur + suf;
					now /= 10;
				}
			}
			bool done = true;

			for (size_t i = 0; i < columns.size(); ++i)
			{
				String temp_name = columns[i]->getName() + suf;
				for (NamesAndTypesList::const_iterator j = already_in_storage.begin(); j != already_in_storage.end(); ++j)
					if (j->first == temp_name)
					{
						done = false;
						break;
					}
				if (!done) break;
			}

			if (done)
			{
				for (size_t i = 0; i < columns.size(); ++i)
					columns[i]->setName(columns[i]->getName() + suf);
				break;
			}
			id ++;
		}
	}

	/// Разбить множество имен столбцов на два: виртуальные и невиртуальные
	void splitNames(const Names & column_names, Names &notvirt, VirtualColumnList &virt) const
	{
		notvirt.clear();
		for (Names::const_iterator i = column_names.begin(); i != column_names.end(); ++i)
		{
			bool is_virt = false;
			size_t id = -1;
			for (size_t j = 0; j < columns.size(); ++j)
				if (columns[j]->getName() == *i)
				{
					is_virt = true;
					id = j;
					break;
				}
			if (is_virt)
				virt.addColumn(columns[id]);
			else
				notvirt.push_back(*i);
		}
	}

	/// Пополнить блок всеми столбцами.
	void populate(Block &res, StoragePtr storage)
	{
		int rows = res.rows();
		for (size_t i = 0; i < columns.size(); ++i)
		{
			res.insert(columns[i]->getColumn(storage, rows));
		}
	}
};

/// Static класс со всеми функциями экстракторами.
class Extractors {
public:
	static String nameExtractor(StoragePtr a)
	{
		return a->getTableName();
	}
};

}
