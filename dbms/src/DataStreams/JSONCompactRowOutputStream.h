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
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferValidUTF8.h>
#include <DataStreams/JSONRowOutputStream.h>

namespace DB
{

struct FormatSettingsJSON;

/** The stream for outputting data in the JSONCompact format.
  */
class JSONCompactRowOutputStream : public JSONRowOutputStream
{
public:
    JSONCompactRowOutputStream(WriteBuffer & ostr_, const Block & sample_, bool write_statistics_, const FormatSettingsJSON & settings);

    void writeField(const String & name, const IColumn & column, const IDataType & type, size_t row_num) override;
    void writeFieldDelimiter() override;
    void writeRowStartDelimiter() override;
    void writeRowEndDelimiter() override;

protected:
    void writeTotals() override;
    void writeExtremes() override;
};

}
