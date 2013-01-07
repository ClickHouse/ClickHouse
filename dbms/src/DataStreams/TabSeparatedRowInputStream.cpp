#include <DB/IO/ReadHelpers.h>

#include <DB/DataStreams/TabSeparatedRowInputStream.h>


namespace DB
{

using Poco::SharedPtr;

TabSeparatedRowInputStream::TabSeparatedRowInputStream(ReadBuffer & istr_, const Block & sample_, bool with_names_, bool with_types_)
	: istr(istr_), sample(sample_), with_names(with_names_), with_types(with_types_)
{
	size_t columns = sample.columns();
	data_types.resize(columns);
	for (size_t i = 0; i < columns; ++i)
		data_types[i] = sample.getByPosition(i).type;
}


void TabSeparatedRowInputStream::readPrefix()
{
	size_t columns = sample.columns();
	String tmp;

	if (with_names)
	{
		for (size_t i = 0; i < columns; ++i)
		{
			readEscapedString(tmp, istr);
			assertString(i == columns - 1 ? "\n" : "\t", istr);
		}
	}

	if (with_types)
	{
		for (size_t i = 0; i < columns; ++i)
		{
			readEscapedString(tmp, istr);
			assertString(i == columns - 1 ? "\n" : "\t", istr);
		}
	}
}


bool TabSeparatedRowInputStream::read(Row & row)
{
	size_t size = data_types.size();
	row.resize(size);
	
	for (size_t i = 0; i < size; ++i)
	{
		if (i == 0 && istr.eof())
		{
			row.clear();
			return false;
		}
		
		data_types[i]->deserializeTextEscaped(row[i], istr);

		/// пропускаем разделители
		if (i + 1 == size)
		{
			if (!istr.eof())
				assertString("\n", istr);
		}
		else
			assertString("\t", istr);
	}

	return true;
}

}
