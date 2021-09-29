#pragma once
#include <Processors/ISimpleTransform.h>

namespace DB
{

class UserDefinedTypesTransform : public ISimpleTransform
{
public:
    explicit UserDefinedTypesTransform(const Block & header) : ISimpleTransform(header, header, false) {}
    String getName() const override { return "UserDefinedTypesTransform"; }

protected:
    void transform(Chunk & chunk) override;
};

}
