#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/DataStreams/TabSeparatedRowInputStream.h>

#include <iostream>

namespace DB
{

using Poco::SharedPtr;

TabSeparatedRowInputStream::TabSeparatedRowInputStream(std::istream & istr_, SharedPtr<DataTypes> data_types_)
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
		(*data_types)[i]->deserializeTextEscaped(res[i], istr);

		if (i == 0 && istr.eof())
		{
			res.clear();
			return res;
		}
		else if (!istr.good())
			throw Exception("Cannot read all data from tab separated input",
				ErrorCodes::CANNOT_READ_ALL_DATA_FROM_TAB_SEPARATED_INPUT);

		/// пропускаем разделители
		if (i + 1 == size)
		{
			if (istr.peek() == '\n')
				istr.ignore();
			else if (!istr.eof())
				throw Exception("Cannot parse all value in tab separated input",
					ErrorCodes::CANNOT_PARSE_ALL_VALUE_FROM_TAB_SEPARATED_INPUT);
		}
		else
		{
			if (istr.peek() == '\t')
				istr.ignore();
			else
				throw Exception("Cannot parse all value in tab separated input",
					ErrorCodes::CANNOT_PARSE_ALL_VALUE_FROM_TAB_SEPARATED_INPUT);
		}
	}

	return res;
}

}
