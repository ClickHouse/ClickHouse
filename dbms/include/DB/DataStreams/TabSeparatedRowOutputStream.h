#ifndef DBMS_DATA_STREAMS_TABSEPARATEDROWOUTPUTSTREAM_H
#define DBMS_DATA_STREAMS_TABSEPARATEDROWOUTPUTSTREAM_H

#include <ostream>

#include <Poco/SharedPtr.h>

#include <DB/ColumnTypes/ColumnTypes.h>
#include <DB/DataStreams/IRowOutputStream.h>


namespace DB
{

using Poco::SharedPtr;


/** Интерфейс потока для вывода данных в формате tsv.
  */
class TabSeparatedRowOutputStream : public IRowOutputStream
{
public:
	TabSeparatedRowOutputStream(std::ostream & ostr_, SharedPtr<ColumnTypes> column_types_);

	void writeField(const Field & field);
	void writeFieldDelimiter();
	void writeRowEndDelimiter();

private:
	std::ostream & ostr;
	SharedPtr<ColumnTypes> column_types;
	size_t field_number;
};

}

#endif
