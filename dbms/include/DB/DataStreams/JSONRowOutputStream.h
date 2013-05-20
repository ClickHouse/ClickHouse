#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Core/Block.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/WriteBufferValidUTF8.h>
#include <DB/DataStreams/IRowOutputStream.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

/** Поток для вывода данных в формате JSON.
  */
class JSONRowOutputStream : public IRowOutputStream
{
public:
	JSONRowOutputStream(WriteBuffer & ostr_, const Block & sample_, const BlockInputStreamPtr & input_stream_ = NULL);

	void writeField(const Field & field);
	void writeFieldDelimiter();
	void writeRowStartDelimiter();
	void writeRowEndDelimiter();
	void writePrefix();
	void writeSuffix();

protected:
	
	void writeRowsBeforeLimitAtLeast();
	
	typedef std::vector<NameAndTypePair> NamesAndTypesVector;
	
	WriteBufferValidUTF8 ostr;
	size_t field_number;
	size_t row_count;
	BlockInputStreamPtr input_stream;	
	NamesAndTypesVector fields;	
};

}

