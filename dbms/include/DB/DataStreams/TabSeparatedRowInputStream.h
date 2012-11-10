#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Core/Block.h>
#include <DB/IO/ReadBuffer.h>
#include <DB/DataStreams/IRowInputStream.h>


namespace DB
{

/** Поток для ввода данных в формате tsv.
  */
class TabSeparatedRowInputStream : public IRowInputStream
{
public:
	/** with_names - в первой строке заголовок с именами столбцов
	  * with_types - на следующей строке заголовок с именами типов
	  */
	TabSeparatedRowInputStream(ReadBuffer & istr_, const Block & sample_, bool with_names_ = false, bool with_types_ = false);

	Row read();
	void readPrefix();

	RowInputStreamPtr clone() { return new TabSeparatedRowInputStream(istr, sample, with_names, with_types); }

private:
	ReadBuffer & istr;
	const Block sample;
	bool with_names;
	bool with_types;
	DataTypes data_types;
};

}
