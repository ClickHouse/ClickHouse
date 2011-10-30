#include <DB/IO/ReadHelpers.h>

#include <DB/DataStreams/ValuesRowInputStream.h>


namespace DB
{

using Poco::SharedPtr;

ValuesRowInputStream::ValuesRowInputStream(ReadBuffer & istr_, SharedPtr<DataTypes> & data_types_)
	: istr(istr_), data_types(data_types_)
{
}


Row ValuesRowInputStream::read()
{
	Row res;
	size_t size = data_types->size();
	res.resize(size);

	skipWhitespaceIfAny(istr);

	if (istr.eof())
	{
		res.clear();
		return res;
	}
	
	assertString("(", istr);
	
	for (size_t i = 0; i < size; ++i)
	{
		if (i != 0)
			assertString(",", istr);
		
		skipWhitespaceIfAny(istr);
		(*data_types)[i]->deserializeTextQuoted(res[i], istr);
		skipWhitespaceIfAny(istr);
	}
	
	assertString(")", istr);

	skipWhitespaceIfAny(istr);
	if (!istr.eof() && *istr.position() == ',')
		++istr.position();

	return res;
}

}
