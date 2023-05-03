#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/ASTSetConfigQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Common/FoundationDB/protos/MetadataConfigParam.pb.h>
#include <Common/FoundationDB/MetadataStoreFoundationDB.h>


namespace DB
{
using ConfigParamKV = FoundationDB::Proto::MetadataConfigParam;
using ConfigParamKVPtr = std::shared_ptr<FoundationDB::Proto::MetadataConfigParam>;

class InterpreterSetConfigQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterSetConfigQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_) : WithMutableContext(context_), query_ptr(query_ptr_) {}

    BlockIO execute() override;

private:
    ConfigParamKVPtr getMetadataConfigFromQuery(const ASTSetConfigQuery & query);
    ASTPtr query_ptr;
};

}
