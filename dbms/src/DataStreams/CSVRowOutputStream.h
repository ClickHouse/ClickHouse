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
#include <DataStreams/IRowOutputStream.h>


namespace DB
{

class WriteBuffer;


/** The stream for outputting data in csv format.
  * Does not conform with https://tools.ietf.org/html/rfc4180 because it uses LF, not CR LF.
  */
class CSVRowOutputStream : public IRowOutputStream
{
public:
    /** with_names - output in the first line a header with column names
      * with_types - output in the next line header with the names of the types
      */
    CSVRowOutputStream(WriteBuffer & ostr_, const Block & sample_, bool with_names_ = false, bool with_types_ = false);

    void writeField(const String & name, const IColumn & column, const IDataType & type, size_t row_num) override;
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

