#pragma once

#include <DB/Core/Block.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/DataStreams/IRowOutputStream.h>


namespace DB
{

/** Поток для вывода данных в формате JSON, по объекту на каждую строчку.
  * Не валидирует UTF-8.
  */
class JSONEachRowRowOutputStream : public IRowOutputStream
{
public:
	JSONEachRowRowOutputStream(WriteBuffer & ostr_, const Block & sample, bool force_quoting_64bit_integers_ = true);

	void writeField(const IColumn & column, const IDataType & type, size_t row_num) override;
	void writeFieldDelimiter() override;
	void writeRowStartDelimiter() override;
	void writeRowEndDelimiter() override;

	void flush() override
	{
		ostr.next();
	}

private:
	WriteBuffer & ostr;
	size_t field_number = 0;
	Names fields;
	bool force_quoting_64bit_integers;
};

}

