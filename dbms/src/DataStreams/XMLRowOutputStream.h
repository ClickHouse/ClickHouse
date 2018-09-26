/* Some modifications Copyright (c) 2018 BlackBerry Limited

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */
#pragma once

#include <Core/Block.h>
#include <Core/Progress.h>
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

    void writeField(const String & name, const IColumn & column, const IDataType & type, size_t row_num) override;
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

