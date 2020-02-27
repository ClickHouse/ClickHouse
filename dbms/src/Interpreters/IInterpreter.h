#pragma once

#include <DataStreams/BlockIO.h>

#include <Processors/QueryPipeline.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

/** Interpreters interface for different queries.
  */
class IInterpreter
{
public:
    /** For queries that return a result (SELECT and similar), sets in BlockIO a stream from which you can read this result.
      * For queries that receive data (INSERT), sets a thread in BlockIO where you can write data.
      * For queries that do not require data and return nothing, BlockIO will be empty.
      */
    virtual BlockIO execute() = 0;

    virtual QueryPipeline executeWithProcessors() { throw Exception("executeWithProcessors not implemented", ErrorCodes::NOT_IMPLEMENTED); }

    virtual bool canExecuteWithProcessors() const { return false; }

    virtual bool ignoreQuota() const { return false; }
    virtual bool ignoreLimits() const { return false; }

    virtual ~IInterpreter() {}
};

}
