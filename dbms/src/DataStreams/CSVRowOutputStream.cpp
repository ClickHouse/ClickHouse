#include <DB/DataStreams/CSVRowOutputStream.h>

#include <DB/IO/WriteHelpers.h>


namespace DB
{


CSVRowOutputStream::CSVRowOutputStream(WriteBuffer & ostr_, const Block & sample_, bool with_names_, bool with_types_)
	: ostr(ostr_), sample(sample_), with_names(with_names_), with_types(with_types_), field_number(0)
{
	size_t columns = sample.columns();
	data_types.resize(columns);
	for (size_t i = 0; i < columns; ++i)
		data_types[i] = sample.getByPosition(i).type;
}


void CSVRowOutputStream::writePrefix()
{
	size_t columns = sample.columns();

	if (with_names)
	{
		for (size_t i = 0; i < columns; ++i)
		{
			writeCSVString(sample.getByPosition(i).name, ostr);
			writeChar(i == columns - 1 ? '\n' : ',', ostr);
		}
	}

	if (with_types)
	{
		for (size_t i = 0; i < columns; ++i)
		{
			writeCSVString(sample.getByPosition(i).type->getName(), ostr);
			writeChar(i == columns - 1 ? '\n' : ',', ostr);
		}
	}
}


void CSVRowOutputStream::writeField(const Field & field)
{
	data_types[field_number]->serializeTextCSV(field, ostr);
	++field_number;
}


void CSVRowOutputStream::writeFieldDelimiter()
{
	writeChar(',', ostr);
}


void CSVRowOutputStream::writeRowEndDelimiter()
{
	writeChar('\n', ostr);
	field_number = 0;
}


void CSVRowOutputStream::writeSuffix()
{
	writeTotals();
	writeExtremes();
}


void CSVRowOutputStream::writeTotals()
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
			writeField((*totals.getByPosition(j).column)[0]);
		}

		writeRowEndDelimiter();
	}
}


void CSVRowOutputStream::writeExtremes()
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
				writeField((*extremes.getByPosition(j).column)[i]);
			}

			writeRowEndDelimiter();
		}
	}
}


}
