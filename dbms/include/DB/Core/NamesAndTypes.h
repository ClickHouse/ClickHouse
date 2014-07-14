#pragma once

#include <map>
#include <list>
#include <string>
#include <set>

#include <Poco/SharedPtr.h>

#include <DB/DataTypes/IDataType.h>
#include <DB/DataTypes/DataTypeFactory.h>
#include <DB/IO/ReadBufferFromString.h>


namespace DB
{

using Poco::SharedPtr;

struct NameAndTypePair
{
	String name;
	DataTypePtr type;

	NameAndTypePair() {}
	NameAndTypePair(const String & name_, const DataTypePtr & type_) : name(name_), type(type_) {}

	bool operator<(const NameAndTypePair & rhs) const
	{
		return std::forward_as_tuple(name, type->getName()) < std::forward_as_tuple(rhs.name, rhs.type->getName());
	}

	bool operator==(const NameAndTypePair & rhs) const
	{
		return name == rhs.name && type->getName() == rhs.type->getName();
	}
};

typedef std::vector<NameAndTypePair> NamesAndTypes;

class NamesAndTypesList : public std::list<NameAndTypePair>
{
public:
	using std::list<NameAndTypePair>::list;

	void readText(ReadBuffer & buf, const DataTypeFactory & data_type_factory)
	{
		DB::assertString("columns format version: 1\n", buf);
		size_t count;
		DB::readText(count, buf);
		DB::assertString(" columns:\n", buf);
		resize(count);
		for (NameAndTypePair & it : *this)
		{
			DB::readBackQuotedString(it.name, buf);
			DB::assertString(" ", buf);
			String type_name;
			DB::readString(type_name, buf);
			it.type = data_type_factory.get(type_name);
			DB::assertString("\n", buf);
		}
	}

	void writeText(WriteBuffer & buf) const
	{
		DB::writeString("columns format version: 1\n", buf);
		DB::writeText(size(), buf);
		DB::writeString(" columns:\n", buf);
		for (const auto & it : *this)
		{
			DB::writeBackQuotedString(it.name, buf);
			DB::writeChar(' ', buf);
			DB::writeString(it.type->getName(), buf);
			DB::writeChar('\n', buf);
		}
	}

	String toString() const
	{
		String s;
		{
			WriteBufferFromString out(s);
			writeText(out);
		}
		return s;
	}

	static NamesAndTypesList parse(const String & s, const DataTypeFactory & data_type_factory)
	{
		ReadBufferFromString in(s);
		NamesAndTypesList res;
		res.readText(in, data_type_factory);
		assertEOF(in);
		return res;
	}

	/// Все элементы rhs должны быть различны.
	bool isSubsetOf(const NamesAndTypesList & rhs) const
	{
		NamesAndTypes vector(rhs.begin(), rhs.end());
		vector.insert(vector.end(), begin(), end());
		std::sort(vector.begin(), vector.end());
		return std::unique(vector.begin(), vector.end()) == vector.begin() + rhs.size();
	}

	/// Расстояние Хемминга между множествами
	///  (иными словами, добавленные и удаленные столбцы считаются один раз; столбцы, изменившие тип, - дважды).
	size_t sizeOfDifference(const NamesAndTypesList & rhs) const
	{
		NamesAndTypes vector(rhs.begin(), rhs.end());
		vector.insert(vector.end(), begin(), end());
		std::sort(vector.begin(), vector.end());
		return (std::unique(vector.begin(), vector.end()) - vector.begin()) * 2 - size() - rhs.size();
	}
};

typedef SharedPtr<NamesAndTypesList> NamesAndTypesListPtr;

}
