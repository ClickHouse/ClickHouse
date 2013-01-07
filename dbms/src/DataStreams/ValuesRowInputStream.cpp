#include <DB/IO/ReadHelpers.h>

#include <DB/DataStreams/ValuesRowInputStream.h>


namespace DB
{

using Poco::SharedPtr;

ValuesRowInputStream::ValuesRowInputStream(ReadBuffer & istr_, const Block & sample_)
	: istr(istr_), sample(sample_)
{
	size_t columns = sample.columns();
	data_types.resize(columns);
	for (size_t i = 0; i < columns; ++i)
		data_types[i] = sample.getByPosition(i).type;
}


bool ValuesRowInputStream::read(Row & row)
{
	size_t size = data_types.size();
	row.resize(size);

	skipWhitespaceIfAny(istr);

	if (istr.eof() || *istr.position() == ';')
	{
		row.clear();
		return false;
	}

	assertString("(", istr);
	
	for (size_t i = 0; i < size; ++i)
	{
		if (i != 0)
			assertString(",", istr);
		
		skipWhitespaceIfAny(istr);
		data_types[i]->deserializeTextQuoted(row[i], istr);
		skipWhitespaceIfAny(istr);
	}
	
	assertString(")", istr);

	skipWhitespaceIfAny(istr);
	if (!istr.eof() && *istr.position() == ',')
		++istr.position();

	return true;
}

}
