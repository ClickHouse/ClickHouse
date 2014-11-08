#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Core/Block.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/DataStreams/IRowOutputStream.h>


namespace DB
{

using Poco::SharedPtr;


/** Поток для вывода данных в формате VALUES (как в INSERT запросе).
  */
class ValuesRowOutputStream : public IRowOutputStream
{
public:
	ValuesRowOutputStream(WriteBuffer & ostr_, const Block & sample_);

	void writeField(const Field & field) override;
	void writeFieldDelimiter() override;
	void writeRowStartDelimiter() override;
	void writeRowEndDelimiter() override;
	void writeRowBetweenDelimiter() override;

	void flush() override { ostr.next(); }

private:
	WriteBuffer & ostr;
	const Block sample;
	DataTypes data_types;
	size_t field_number;
};

}

