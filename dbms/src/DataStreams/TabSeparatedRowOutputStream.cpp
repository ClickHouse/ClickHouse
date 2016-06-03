#include <DB/DataStreams/TabSeparatedRowOutputStream.h>

#include <DB/IO/WriteHelpers.h>


namespace DB
{

using Poco::SharedPtr;


TabSeparatedRowOutputStream::TabSeparatedRowOutputStream(WriteBuffer & ostr_, const Block & sample_, bool with_names_, bool with_types_)
	: ostr(ostr_), sample(sample_), with_names(with_names_), with_types(with_types_)
{
}


void TabSeparatedRowOutputStream::writePrefix()
{
	size_t columns = sample.columns();

	if (with_names)
	{
		for (size_t i = 0; i < columns; ++i)
		{
			writeEscapedString(sample.getByPosition(i).name, ostr);
			writeChar(i == columns - 1 ? '\n' : '\t', ostr);
		}
	}

	if (with_types)
	{
		for (size_t i = 0; i < columns; ++i)
		{
			writeEscapedString(sample.getByPosition(i).type->getName(), ostr);
			writeChar(i == columns - 1 ? '\n' : '\t', ostr);
		}
	}
}


void TabSeparatedRowOutputStream::writeField(const IColumn & column, const IDataType & type, size_t row_num)
{
	type.serializeTextEscaped(column, row_num, ostr);
}


void TabSeparatedRowOutputStream::writeFieldDelimiter()
{
	writeChar('\t', ostr);
}


void TabSeparatedRowOutputStream::writeRowEndDelimiter()
{
	writeChar('\n', ostr);
}


void TabSeparatedRowOutputStream::writeSuffix()
{
	writeTotals();
	writeExtremes();
}


void TabSeparatedRowOutputStream::writeTotals()
{
	if (totals)
	{
		size_t columns = totals.columns();

		writeChar('\n', ostr);
		writeRowStartDelimiter();

		for (size_t j = 0; j < columns; ++j)
		{
			if (j != 0)
				writeFieldDelimiter();
			writeField(*totals.unsafeGetByPosition(j).column.get(), *totals.unsafeGetByPosition(j).type.get(), 0);
		}

		writeRowEndDelimiter();
	}
}


void TabSeparatedRowOutputStream::writeExtremes()
{
	if (extremes)
	{
		size_t rows = extremes.rows();
		size_t columns = extremes.columns();

		writeChar('\n', ostr);

		for (size_t i = 0; i < rows; ++i)
		{
			if (i != 0)
				writeRowBetweenDelimiter();

			writeRowStartDelimiter();

			for (size_t j = 0; j < columns; ++j)
			{
				if (j != 0)
					writeFieldDelimiter();
				writeField(*extremes.unsafeGetByPosition(j).column.get(), *extremes.unsafeGetByPosition(j).type.get(), i);
			}

			writeRowEndDelimiter();
		}
	}
}


}
