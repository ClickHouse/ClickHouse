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
#pragma once

#include <unordered_map>
#include <utility>
#include <atomic>
#include <common/Types.h>

#include <Core/Defines.h>
#include <Core/Types.h>


namespace DB
{

class ReadBuffer;
class WriteBuffer;

using HeartbeatHashMap = std::unordered_map<String, String>;
/** Heartbeat of live query execution.
  */
struct Heartbeat
{
    std::atomic<UInt64> timestamp;
    HeartbeatHashMap hashmap;

    Heartbeat() {}
    Heartbeat(const UInt64 & timestamp_, const HeartbeatHashMap & hashmap_) : timestamp(timestamp_), hashmap(hashmap_) {}

    void read(ReadBuffer & in, UInt64 server_revision);
    void write(WriteBuffer & out, UInt64 client_revision) const;

    /// Heartbeat in JSON format (single line, without whitespaces) used in HTTP headers.
    void writeJSON(WriteBuffer & out) const;

    void reset()
    {
        timestamp = 0;
        hashmap.clear();
    }

    Heartbeat & operator=(Heartbeat && other)
    {
        timestamp = other.timestamp.load(std::memory_order_relaxed);
        hashmap = other.hashmap;

        return *this;
    }

    Heartbeat(Heartbeat && other)
    {
        *this = std::move(other);
    }
};


}
