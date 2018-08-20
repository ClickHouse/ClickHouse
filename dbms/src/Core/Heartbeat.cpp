/* Copyright (c) 2018 BlackBerry Limited

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */
#include <Core/Heartbeat.h>

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

void Heartbeat::read(ReadBuffer & in, UInt64 server_revision)
{
    size_t size = 0;
    UInt64 new_timestamp = 0;
    HeartbeatHashMap new_hashmap;
    readVarUInt(new_timestamp, in);


    readVarUInt(size, in);

    if (size > DEFAULT_MAX_STRING_SIZE)
        throw Poco::Exception("Too large set size.");

    for (size_t i = 0; i < size; ++i)
    {
        String name;
        String hash;
        readStringBinary(name, in);
        readStringBinary(hash, in);
        new_hashmap.insert({name, hash});
    }

    timestamp = new_timestamp;
    hashmap = std::move(new_hashmap);
}


void Heartbeat::write(WriteBuffer & out, UInt64 client_revision) const
{
    writeVarUInt(timestamp.load(), out);

    writeVarUInt(hashmap.size(), out);

    for (auto it = hashmap.begin(); it != hashmap.end(); ++it)
    {
        writeStringBinary(it->first, out);
        writeStringBinary(it->second, out);
    }
}


void Heartbeat::writeJSON(WriteBuffer & out) const
{
    /// Numbers are written in double quotes (as strings) to avoid loss of precision
    ///  of 64-bit integers after interpretation by JavaScript.
    writeCString("{\"timestamp\":\"", out);
    writeText(timestamp.load(), out);

    writeCString(",{\"hash\":\"", out);
    for (auto it = hashmap.begin(); it != hashmap.end();)
    {
        writeCString("{", out);
        writeJSONString(it->first, out);
        writeCString(":", out);
        writeJSONString(it->second, out);
        writeCString("}", out);
        ++it;
        if (it != hashmap.end())
            writeCString(",", out);
    }
    writeCString("\"}", out);
    writeCString("\"}", out);
}

}
