#pragma once

#include <DataStreams/BlockIO.h>


namespace DB
{

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

    virtual ~IInterpreter() {}
};

}
