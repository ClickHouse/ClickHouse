#pragma once

#include <Interpreters/IInterpreter.h>
#include <Interpreters/StorageID.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <Common/ActionLock.h>

namespace Poco
{
class Logger;
}

namespace DB
{
class Context;
class AccessRightsElements;
class ASTClusterQuery;

/** Implement various CLUSTER queries.
  * Examples: 
  *   CLUSTER PAUSE NODE IP[:PORT] [ON CLUSTER xxx]
  *   CLUSTER ADD NODE IP[:PORT] [ON CLUSTER xxx]
  */
class InterpreterClusterQuery : public IInterpreter, WithContext
{
public:
    InterpreterClusterQuery(const ASTPtr & query_ptr_, ContextPtr context_);

    BlockIO execute() override;

    bool ignoreQuota() const override { return true; }
    bool ignoreLimits() const override { return true; }

private:
    ASTPtr query_ptr;
    Poco::Logger * log = nullptr;
    AccessRightsElements getRequiredAccessCluster() const;
};

}
