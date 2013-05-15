#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Core/Block.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/WriteBufferValidUTF8.h>
#include <DB/DataStreams/IRowOutputStream.h>


namespace DB
{

/** Поток для вывода данных в формате JSON.
  */
class JSONRowOutputStream : public IRowOutputStream
{
public:
	JSONRowOutputStream(WriteBuffer & ostr_, const Block & sample_);

	void writeField(const Field & field);
	void writeFieldDelimiter();
	void writeRowStartDelimiter();
	void writeRowEndDelimiter();
	void writePrefix();
	void writeSuffix();

protected:
	typedef std::vector<NameAndTypePair> NamesAndTypesVector;
	
	//WriteBufferValidUTF8 ostr;
	WriteBuffer & ostr;
	size_t field_number;
	size_t row_count;	
	NamesAndTypesVector fields;
};

}

