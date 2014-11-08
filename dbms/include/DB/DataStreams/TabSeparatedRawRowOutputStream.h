#pragma once

#include <DB/DataStreams/TabSeparatedRowOutputStream.h>


namespace DB
{

/** Поток для вывода данных в формате tsv, но без эскейпинга отдельных значений.
  * (То есть - вывод необратимый.)
  */
class TabSeparatedRawRowOutputStream : public TabSeparatedRowOutputStream
{
public:
	TabSeparatedRawRowOutputStream(WriteBuffer & ostr_, const Block & sample_, bool with_names_ = false, bool with_types_ = false)
		: TabSeparatedRowOutputStream(ostr_, sample_, with_names_, with_types_) {}

	void writeField(const Field & field) override
	{
		data_types[field_number]->serializeText(field, ostr);
		++field_number;
	}
};

}

