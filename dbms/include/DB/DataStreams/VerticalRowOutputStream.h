#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Core/Block.h>
#include <DB/Core/Names.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/DataStreams/IRowOutputStream.h>


namespace DB
{

using Poco::SharedPtr;


/** Поток для вывода данных в формате "каждое значение на своей строке".
  */
class VerticalRowOutputStream : public IRowOutputStream
{
public:
	VerticalRowOutputStream(WriteBuffer & ostr_, const Block & sample_);

	void writeField(const Field & field) override;
	void writeRowStartDelimiter() override;
	void writeRowBetweenDelimiter() override;

	void flush() override { ostr.next(); }

private:
	WriteBuffer & ostr;
	const Block sample;
	DataTypes data_types;
	Names names;
	size_t field_number;
	size_t row_number;

	typedef std::vector<String> Pads_t;
	Pads_t pads;
};

}

