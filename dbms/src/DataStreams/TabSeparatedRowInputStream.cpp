#include <DB/IO/ReadHelpers.h>

#include <DB/DataStreams/TabSeparatedRowInputStream.h>


namespace DB
{

using Poco::SharedPtr;

TabSeparatedRowInputStream::TabSeparatedRowInputStream(ReadBuffer & istr_, SharedPtr<DataTypes> & data_types_)
	: istr(istr_), data_types(data_types_)
{
}


Row TabSeparatedRowInputStream::read()
{
	Row res;
	size_t size = data_types->size();
	res.resize(size);
	
	for (size_t i = 0; i < size; ++i)
	{
		if (i == 0 && istr.eof())
		{
			res.clear();
			return res;
		}
		
		(*data_types)[i]->deserializeTextEscaped(res[i], istr);

		/// пропускаем разделители
		if (i + 1 == size)
		{
			if (!istr.eof())
				assertString("\n", istr);
		}
		else
			assertString("\t", istr);
	}

	return res;
}

}
