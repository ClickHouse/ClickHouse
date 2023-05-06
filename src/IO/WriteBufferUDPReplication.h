#pragma once

#include <memory>
#include <iostream>
#include <cstring>
#include <mutex>

#include <Common/Exception.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromOStream.h>
#include <IO/Progress.h>

#include <Server/UDPReplicationPack.h>


namespace DB
{

/** A simple abstract class for buffered data writing (char sequences) somewhere.
  * Unlike std::ostream, it provides access to the internal buffer,
  *  and also allows you to manually manage the position inside the buffer.
  *
  * Derived classes must implement the nextImpl() method.
  */
class WriteBufferUDPReplication : public BufferWithOwnMemory<WriteBuffer>
{
public:
    WriteBufferUDPReplication(std::ostream & _out, UDPReplicationPack & _resp) : BufferWithOwnMemory<WriteBuffer>(DBMS_DEFAULT_BUFFER_SIZE), out_str(&_out), resp(&_resp)
    {
      res_str = new std::istream(nullptr);
    }

    ~WriteBufferUDPReplication() override
    {
      delete res_str;
    }

    std::string res()
    {
      return res_s;
    }

private:
    std::ostream* out_str;
    UDPReplicationPack * resp;
    std::istream* res_str;
    std::string res_s;
    std::mutex mutex;
    std::unique_ptr<WriteBufferFromOStream> out;
    Progress accumulated_progress;
    /** Write the data in the buffer (from the beginning of the buffer to the current position).
      * Throw an exception if something is wrong.
      */
    void nextImpl() override
    {
        std::lock_guard lock(mutex);

        if (!out) {
          out = std::make_unique<WriteBufferFromOStream>(*out_str, working_buffer.size(), working_buffer.begin());
        }

        out->buffer() = buffer();
        out->position() = position();
        out->next();

        res_str->rdbuf(out_str->rdbuf());

        res_s += std::string{ std::istreambuf_iterator<char>(*res_str),
               std::istreambuf_iterator<char>() };

        out_str->clear();
    }

    void writeSummary()
    {
        WriteBufferFromOwnString progress_string_writer;
        accumulated_progress.writeJSON(progress_string_writer);

        resp->set("X-ClickHouse-Summary", progress_string_writer.str());
    }

    void finalizeImpl() override
    {
      next();
      if (!offset())
      {
          std::lock_guard lock(mutex);
          writeSummary();
      }
    }

    void onProgress(const Progress & progress)
    {
      std::lock_guard lock(mutex);
      accumulated_progress.incrementPiecewiseAtomically(progress);
    }

using WriteBufferUDPReplicationPtr = std::shared_ptr<WriteBufferUDPReplication>;

};

}
