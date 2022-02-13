#pragma once
#include <Processors/ISimpleTransform.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

/// Remove non-physical columns.
class ReduceToPhysicalTransform : public ISimpleTransform
{
public:
    ReduceToPhysicalTransform(const Block & input_header, const ColumnsDescription & columns);
    String getName() const override { return "ReduceToPhysicalTransform"; }

    static Block transformToPhysicalBlock(const Block & header, const ColumnsDescription & columns);
protected:
    void transform(Chunk & chunk) override;
private:
    std::vector<size_t> index;
};

}
