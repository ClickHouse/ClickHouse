#ifndef DBMS_DATA_STREAMS_TABSEPARATEDROWINPUTSTREAM_H
#define DBMS_DATA_STREAMS_TABSEPARATEDROWINPUTSTREAM_H

#include <ostream>

#include <Poco/SharedPtr.h>

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
	TabSeparatedRowInputStream(std::istream & istr_, SharedPtr<DataTypes> data_types_);

	Row read();

private:
	std::istream & istr;
	SharedPtr<DataTypes> data_types;
};

}

#endif
