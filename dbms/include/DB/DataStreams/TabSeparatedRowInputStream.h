#ifndef DBMS_DATA_STREAMS_TABSEPARATEDROWINPUTSTREAM_H
#define DBMS_DATA_STREAMS_TABSEPARATEDROWINPUTSTREAM_H

#include <Poco/SharedPtr.h>

#include <DB/IO/ReadBuffer.h>
#include <DB/DataTypes/DataTypes.h>
#include <DB/DataStreams/IRowInputStream.h>


namespace DB
{

using Poco::SharedPtr;


/** Интерфейс потока для ввода данных в формате tsv.
  */
class TabSeparatedRowInputStream : public IRowInputStream
{
public:
	TabSeparatedRowInputStream(ReadBuffer & istr_, SharedPtr<DataTypes> data_types_);

	Row read();

private:
	ReadBuffer & istr;
	SharedPtr<DataTypes> data_types;
};

}

#endif
