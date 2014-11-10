#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Core/Block.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/DataStreams/IRowOutputStream.h>


namespace DB
{

/** Поток для вывода данных в бинарном построчном формате.
  */
class BinaryRowOutputStream : public IRowOutputStream
{
public:
	BinaryRowOutputStream(WriteBuffer & ostr_, const Block & sample_);

	void writeField(const Field & field) override;
	void writeRowEndDelimiter() override;

	void flush() override { ostr.next(); }

protected:
	WriteBuffer & ostr;
	const Block sample;
	DataTypes data_types;
	size_t field_number;
};

}

