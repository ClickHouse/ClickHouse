#pragma once

#include <Core/Block.h>
#include <DataStreams/IRowOutputStream.h>


namespace DB
{

class WriteBuffer;


/** Поток для вывода данных в формате csv.
  * Не соответствует https://tools.ietf.org/html/rfc4180 потому что использует LF, а не CR LF.
  */
class CSVRowOutputStream : public IRowOutputStream
{
public:
    /** with_names - выводить в первой строке заголовок с именами столбцов
      * with_types - выводить на следующей строке заголовок с именами типов
      */
    CSVRowOutputStream(WriteBuffer & ostr_, const Block & sample_, bool with_names_ = false, bool with_types_ = false);

    void writeField(const IColumn & column, const IDataType & type, size_t row_num) override;
    void writeFieldDelimiter() override;
    void writeRowEndDelimiter() override;
    void writePrefix() override;
    void writeSuffix() override;

    void flush() override;

    void setTotals(const Block & totals_) override { totals = totals_; }
    void setExtremes(const Block & extremes_) override { extremes = extremes_; }

    /// https://www.iana.org/assignments/media-types/text/csv
    String getContentType() const override
    {
        return String("text/csv; charset=UTF-8; header=") + ((with_names || with_types) ? "present" : "absent");
    }

protected:
    void writeTotals();
    void writeExtremes();

    WriteBuffer & ostr;
    const Block sample;
    bool with_names;
    bool with_types;
    DataTypes data_types;
    Block totals;
    Block extremes;
};

}

