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

#include <string>
#include <vector>
#include <memory>
#include <boost/noncopyable.hpp>
#include <Core/Block.h>

#include <DataTypes/DataTypesNumber.h>

namespace DB
{

struct Progress;
struct Heartbeat;

class TableStructureReadLock;
using TableStructureReadLockPtr = std::shared_ptr<TableStructureReadLock>;
using TableStructureReadLocks = std::vector<TableStructureReadLockPtr>;

struct Progress;
struct Heartbeat;


/** Interface of stream for writing data (into table, filesystem, network, terminal, etc.)
  */
class IBlockOutputStream : private boost::noncopyable
{
public:
    IBlockOutputStream() {}

    /** Get data structure of the stream in a form of "header" block (it is also called "sample block").
      * Header block contains column names, data types, columns of size 0. Constant columns must have corresponding values.
      * You must pass blocks of exactly this structure to the 'write' method.
      */
    virtual Block getHeader() const = 0;

    /** Write block.
      */
    virtual void write(const Block & block) = 0;

    /** Write or do something before all data or after all data.
      */
    virtual void writePrefix() {}
    virtual void writeSuffix() { flush(); }

    /** Flush output buffers if any.
      */
    virtual void flush() {}

    /** Methods to set additional information for output in formats, that support it.
      */
    virtual void setSampleBlock(const Block & /*sample*/) {}
    virtual void setRowsBeforeLimit(size_t /*rows_before_limit*/) {}
    virtual void setTotals(const Block & /*totals*/) {}
    virtual void setExtremes(const Block & /*extremes*/) {}

    /** Notify about progress. Method could be called from different threads.
      * Passed value are delta, that must be summarized.
      */
    virtual void onProgress(const Progress & /*progress*/) {}

    /**
     *  Heartbeat
     */
    virtual void onHeartbeat(const Heartbeat & heartbeat) {}

    /** Content-Type to set when sending HTTP response.
      */
    virtual std::string getContentType() const { return "text/plain; charset=UTF-8"; }

    virtual ~IBlockOutputStream() {}

    /** Don't let to alter table while instance of stream is alive.
      */
    void addTableLock(const TableStructureReadLockPtr & lock) { table_locks.push_back(lock); }

private:
    TableStructureReadLocks table_locks;
};

using BlockOutputStreamPtr = std::shared_ptr<IBlockOutputStream>;

}
