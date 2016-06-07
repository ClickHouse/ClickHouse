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
	BinaryRowOutputStream(WriteBuffer & ostr_);

	void writeField(const IColumn & column, const IDataType & type, size_t row_num) override;

	void flush() override { ostr.next(); }

	String getContentType() const override { return "application/octet-stream"; }

protected:
	WriteBuffer & ostr;
};

}

