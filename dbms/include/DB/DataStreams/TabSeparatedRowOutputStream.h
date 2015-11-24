#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Core/Block.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/DataStreams/IRowOutputStream.h>


namespace DB
{

/** Поток для вывода данных в формате tsv.
  */
class TabSeparatedRowOutputStream : public IRowOutputStream
{
public:
	/** with_names - выводить в первой строке заголовок с именами столбцов
	  * with_types - выводить на следующей строке заголовок с именами типов
	  */
	TabSeparatedRowOutputStream(WriteBuffer & ostr_, const Block & sample_, bool with_names_ = false, bool with_types_ = false);

	void writeField(const Field & field) override;
	void writeFieldDelimiter() override;
	void writeRowEndDelimiter() override;
	void writePrefix() override;
	void writeSuffix() override;

	void flush() override { ostr.next(); }

	void setTotals(const Block & totals_) override { totals = totals_; }
	void setExtremes(const Block & extremes_) override { extremes = extremes_; }

	/// https://www.iana.org/assignments/media-types/text/tab-separated-values
	String getContentType() const override { return "text/tab-separated-values; charset=UTF-8"; }

protected:
	void writeTotals();
	void writeExtremes();

	WriteBuffer & ostr;
	const Block sample;
	bool with_names;
	bool with_types;
	DataTypes data_types;
	size_t field_number;
	Block totals;
	Block extremes;
};

}

