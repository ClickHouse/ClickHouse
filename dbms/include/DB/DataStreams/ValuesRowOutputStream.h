#pragma once

#include <DB/IO/WriteBuffer.h>
#include <DB/DataStreams/IRowOutputStream.h>


namespace DB
{


/** Поток для вывода данных в формате VALUES (как в INSERT запросе).
  */
class ValuesRowOutputStream : public IRowOutputStream
{
public:
	ValuesRowOutputStream(WriteBuffer & ostr_);

	void writeField(const IColumn & column, const IDataType & type, size_t row_num) override;
	void writeFieldDelimiter() override;
	void writeRowStartDelimiter() override;
	void writeRowEndDelimiter() override;
	void writeRowBetweenDelimiter() override;

	void flush() override { ostr.next(); }

private:
	WriteBuffer & ostr;
};

}

