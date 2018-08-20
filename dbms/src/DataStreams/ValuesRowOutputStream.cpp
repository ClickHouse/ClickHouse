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
#include <DataStreams/ValuesRowOutputStream.h>

#include <IO/WriteHelpers.h>
#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>


namespace DB
{


ValuesRowOutputStream::ValuesRowOutputStream(WriteBuffer & ostr_)
    : ostr(ostr_)
{
}

void ValuesRowOutputStream::flush()
{
    ostr.next();
}

void ValuesRowOutputStream::writeField(const String & name, const IColumn & column, const IDataType & type, size_t row_num)
{
    type.serializeTextQuoted(column, row_num, ostr);
}

void ValuesRowOutputStream::writeFieldDelimiter()
{
    writeChar(',', ostr);
}

void ValuesRowOutputStream::writeRowStartDelimiter()
{
    writeChar('(', ostr);
}

void ValuesRowOutputStream::writeRowEndDelimiter()
{
    writeChar(')', ostr);
}

void ValuesRowOutputStream::writeRowBetweenDelimiter()
{
    writeCString(",", ostr);
}


}
