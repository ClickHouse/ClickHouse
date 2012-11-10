#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Core/Block.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/DataStreams/IRowOutputStream.h>


namespace DB
{

/** Поток для вывода данных в формате tsv.
  */
class TabSeparatedRowOutputStream : public IRowOutputStream
{
public:
	/** with_names - выводить в первой строке заголовок с именами столбцов
	  * with_types - выводить на следующей строке заголовок с именами типов
	  */
	TabSeparatedRowOutputStream(WriteBuffer & ostr_, const Block & sample_, bool with_names_ = false, bool with_types_ = false);

	void writeField(const Field & field);
	void writeFieldDelimiter();
	void writeRowEndDelimiter();
	void writePrefix();

	RowOutputStreamPtr clone() { return new TabSeparatedRowOutputStream(ostr, sample, with_names, with_types); }

protected:
	WriteBuffer & ostr;
	const Block sample;
	bool with_names;
	bool with_types;
	DataTypes data_types;
	size_t field_number;
};

}

