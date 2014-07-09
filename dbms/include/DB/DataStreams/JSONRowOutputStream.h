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
	JSONRowOutputStream(WriteBuffer & ostr_, const Block & sample_);

	void writeField(const Field & field);
	void writeFieldDelimiter();
	void writeRowStartDelimiter();
	void writeRowEndDelimiter();
	void writePrefix();
	void writeSuffix();
	
	void setRowsBeforeLimit(size_t rows_before_limit_)
	{
		applied_limit = true;
		rows_before_limit = rows_before_limit_;
	}

	void setTotals(const Block & totals_) { totals = totals_; }
	void setExtremes(const Block & extremes_) { extremes = extremes_; }

protected:
	
	void writeRowsBeforeLimitAtLeast();
	virtual void writeTotals();
	virtual void writeExtremes();
	
	WriteBufferValidUTF8 ostr;
	size_t field_number;
	size_t row_count;
	bool applied_limit;
	size_t rows_before_limit;
	NamesAndTypes fields;
	Block totals;
	Block extremes;
};

}

