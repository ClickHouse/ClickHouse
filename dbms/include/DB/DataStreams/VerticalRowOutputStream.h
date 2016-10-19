#pragma once

#include <DB/Core/Block.h>
#include <DB/Core/Names.h>
#include <DB/DataStreams/IRowOutputStream.h>


namespace DB
{

class WriteBuffer;


/** Поток для вывода данных в формате "каждое значение на своей строке".
  */
class VerticalRowOutputStream : public IRowOutputStream
{
public:
	VerticalRowOutputStream(WriteBuffer & ostr_, const Block & sample_);

	void writeField(const IColumn & column, const IDataType & type, size_t row_num) override;
	void writeRowStartDelimiter() override;
	void writeRowBetweenDelimiter() override;

	void flush() override;

protected:
	virtual void writeValue(const IColumn & column, const IDataType & type, size_t row_num) const;

	WriteBuffer & ostr;
	const Block sample;
	Names names;
	size_t field_number;
	size_t row_number;

	using Pads_t = std::vector<String>;
	Pads_t pads;
};


/** То же самое, но строки выводятся без экранирования.
  */
class VerticalRawRowOutputStream : public VerticalRowOutputStream
{
public:
	VerticalRawRowOutputStream(WriteBuffer & ostr_, const Block & sample_)
		: VerticalRowOutputStream(ostr_, sample_) {}

protected:
	void writeValue(const IColumn & column, const IDataType & type, size_t row_num) const override;
};

}

