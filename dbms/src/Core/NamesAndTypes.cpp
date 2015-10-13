#include <DB/Core/NamesAndTypes.h>
#include <DB/DataTypes/DataTypeFactory.h>
#include <DB/IO/ReadBuffer.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>


namespace DB
{

void NamesAndTypesList::readText(ReadBuffer & buf)
{
	const DataTypeFactory & data_type_factory = DataTypeFactory::instance();

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

void NamesAndTypesList::writeText(WriteBuffer & buf) const
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

String NamesAndTypesList::toString() const
{
	String s;
	{
		WriteBufferFromString out(s);
		writeText(out);
	}
	return s;
}

NamesAndTypesList NamesAndTypesList::parse(const String & s)
{
	ReadBufferFromString in(s);
	NamesAndTypesList res;
	res.readText(in);
	assertEOF(in);
	return res;
}

bool NamesAndTypesList::isSubsetOf(const NamesAndTypesList & rhs) const
{
	NamesAndTypes vector(rhs.begin(), rhs.end());
	vector.insert(vector.end(), begin(), end());
	std::sort(vector.begin(), vector.end());
	return std::unique(vector.begin(), vector.end()) == vector.begin() + rhs.size();
}

size_t NamesAndTypesList::sizeOfDifference(const NamesAndTypesList & rhs) const
{
	NamesAndTypes vector(rhs.begin(), rhs.end());
	vector.insert(vector.end(), begin(), end());
	std::sort(vector.begin(), vector.end());
	return (std::unique(vector.begin(), vector.end()) - vector.begin()) * 2 - size() - rhs.size();
}

Names NamesAndTypesList::getNames() const
{
	Names res;
	res.reserve(size());
	for (const NameAndTypePair & column : *this)
	{
		res.push_back(column.name);
	}
	return res;
}

NamesAndTypesList NamesAndTypesList::filter(const NameSet & names) const
{
	NamesAndTypesList res;
	for (const NameAndTypePair & column : *this)
	{
		if (names.count(column.name))
			res.push_back(column);
	}
	return res;
}

NamesAndTypesList NamesAndTypesList::filter(const Names & names) const
{
	return filter(NameSet(names.begin(), names.end()));
}

NamesAndTypesList NamesAndTypesList::addTypes(const Names & names) const
{
	/// NOTE Лучше сделать map в IStorage, чем создавать его здесь каждый раз заново.
	google::dense_hash_map<StringRef, const DataTypePtr *, StringRefHash> types;
	types.set_empty_key(StringRef());

	for (const NameAndTypePair & column : *this)
		types[column.name] = &column.type;

	NamesAndTypesList res;
	for (const String & name : names)
	{
		auto it = types.find(name);
		if (it == types.end())
			throw Exception("No column " + name, ErrorCodes::THERE_IS_NO_COLUMN);
		res.push_back(NameAndTypePair(name, *it->second));
	}
	return res;
}

}
