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
        SharedHeader common_header_,
        StoragePtr storage_,
        ContextPtr context_,
        const std::vector<size_t> & columns_to_save_indices_
    );

    String getName() const override { return "EvaluateCommonSubquery"; }
    void transform(Chunk & chunk) override;

private:
    SharedHeader common_header;
    StoragePtr storage;
    ContextPtr context;
    std::vector<size_t> columns_to_save_indices;
};

}
