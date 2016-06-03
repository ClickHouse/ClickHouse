#pragma once

#include <DB/Core/Block.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/DataStreams/IRowOutputStream.h>


namespace DB
{

/** Поток для вывода данных в формате JSON.
  */
class JSONRowOutputStream : public IRowOutputStream
{
public:
	JSONRowOutputStream(WriteBuffer & ostr_, const Block & sample_);

	void writeField(const IColumn & column, const IDataType & type, size_t row_num) override;
	void writeFieldDelimiter() override;
	void writeRowStartDelimiter() override;
	void writeRowEndDelimiter() override;
	void writePrefix() override;
	void writeSuffix() override;

	void flush() override
	{
		ostr->next();

		if (validating_ostr)
			dst_ostr.next();
	}

	void setRowsBeforeLimit(size_t rows_before_limit_) override
	{
		applied_limit = true;
		rows_before_limit = rows_before_limit_;
	}

	void setTotals(const Block & totals_) override { totals = totals_; }
	void setExtremes(const Block & extremes_) override { extremes = extremes_; }

	String getContentType() const override { return "application/json; charset=UTF-8"; }

protected:

	void writeRowsBeforeLimitAtLeast();
	virtual void writeTotals();
	virtual void writeExtremes();

	WriteBuffer & dst_ostr;
	std::unique_ptr<WriteBuffer> validating_ostr;	/// Валидирует UTF-8 последовательности.
	WriteBuffer * ostr;

	size_t field_number = 0;
	size_t row_count = 0;
	bool applied_limit = false;
	size_t rows_before_limit = 0;
	NamesAndTypes fields;
	Block totals;
	Block extremes;
};

}

