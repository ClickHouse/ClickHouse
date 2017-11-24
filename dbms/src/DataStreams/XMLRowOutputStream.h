#pragma once

#include <Core/Block.h>
#include <IO/Progress.h>
#include <IO/WriteBuffer.h>
#include <Common/Stopwatch.h>
#include <DataStreams/IRowOutputStream.h>


namespace DB
{

/** A stream for outputting data in XML format.
  */
class XMLRowOutputStream : public IRowOutputStream
{
public:
    XMLRowOutputStream(WriteBuffer & ostr_, const Block & sample_,
        bool write_statistics_);

    void writeField(const IColumn & column, const IDataType & type, size_t row_num) override;
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

    void onProgress(const Progress & value) override;

    String getContentType() const override { return "application/xml; charset=UTF-8"; }

protected:

    void writeRowsBeforeLimitAtLeast();
    virtual void writeTotals();
    virtual void writeExtremes();
    void writeStatistics();

    WriteBuffer & dst_ostr;
    std::unique_ptr<WriteBuffer> validating_ostr;    /// Validates UTF-8 sequences, replaces bad sequences with replacement character.
    WriteBuffer * ostr;

    size_t field_number = 0;
    size_t row_count = 0;
    bool applied_limit = false;
    size_t rows_before_limit = 0;
    NamesAndTypes fields;
    Names field_tag_names;
    Block totals;
    Block extremes;

    Progress progress;
    Stopwatch watch;
    bool write_statistics;
};

}

