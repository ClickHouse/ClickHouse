#pragma once

#include <DB/DataStreams/TabSeparatedRowOutputStream.h>


namespace DB
{

/** Интерфейс потока для вывода данных в формате tsv, но без эскейпинга отдельных значений.
  * (То есть - вывод необратимый.)
  */
class TabSeparatedRawRowOutputStream : public TabSeparatedRowOutputStream
{
public:
	TabSeparatedRawRowOutputStream(WriteBuffer & ostr_, const Block & sample_, bool with_names_ = false, bool with_types_ = false)
		: TabSeparatedRowOutputStream(ostr, sample, with_names, with_types) {}

	void writeField(const Field & field)
	{
		data_types[field_number]->serializeText(field, ostr);
		++field_number;
	}

	RowOutputStreamPtr clone() { return new TabSeparatedRawRowOutputStream(ostr, sample, with_names, with_types); }
};

}

