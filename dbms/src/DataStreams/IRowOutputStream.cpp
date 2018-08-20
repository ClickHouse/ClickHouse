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
#include <Core/Block.h>
#include <DataStreams/IRowOutputStream.h>


namespace DB
{

void IRowOutputStream::write(const Block & block, size_t row_num)
{
    size_t columns = block.columns();

    writeRowStartDelimiter();

    for (size_t i = 0; i < columns; ++i)
    {
        if (i != 0)
            writeFieldDelimiter();

        auto & col = block.getByPosition(i);
        writeField(col.name, *col.column, *col.type, row_num);
    }

    writeRowEndDelimiter();
}

}
