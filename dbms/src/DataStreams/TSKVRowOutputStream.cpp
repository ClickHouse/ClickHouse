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
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <DataStreams/TSKVRowOutputStream.h>


namespace DB
{

TSKVRowOutputStream::TSKVRowOutputStream(WriteBuffer & ostr_, const Block & sample_)
    : TabSeparatedRowOutputStream(ostr_, sample_)
{
    NamesAndTypesList columns(sample_.getColumnsList());
    fields.assign(columns.begin(), columns.end());

    for (auto & field : fields)
    {
        WriteBufferFromOwnString wb;
        writeAnyEscapedString<'='>(field.name.data(), field.name.data() + field.name.size(), wb);
        writeCString("=", wb);
        field.name = wb.str();
    }
}


void TSKVRowOutputStream::writeField(const String & name, const IColumn & column, const IDataType & type, size_t row_num)
{
    writeString(name, ostr);
    type.serializeTextEscaped(column, row_num, ostr);
}


void TSKVRowOutputStream::writeRowEndDelimiter()
{
    writeChar('\n', ostr);
}


}
