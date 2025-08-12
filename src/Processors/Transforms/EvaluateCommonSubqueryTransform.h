#pragma once

#include <Processors/ISimpleTransform.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class IStorage;
using StoragePtr = std::shared_ptr<IStorage>;

class EvaluateCommonSubqueryTransform : public ISimpleTransform
{
public:
    EvaluateCommonSubqueryTransform(
        SharedHeader header_,
        StoragePtr storage_,
        ContextPtr context_);

    String getName() const override { return "EvaluateCommonSubquery"; }
    void transform(Chunk & chunk) override;

private:
    StoragePtr storage;
    ContextPtr context;
};

}
