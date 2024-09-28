#pragma once
#include <Processors/ISimpleTransform.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

class ColumnLazyTransform : public ISimpleTransform
{
public:
    explicit ColumnLazyTransform(
        const Block & header_,
        const LazilyReadInfoPtr & lazily_read_info_);

    static Block transformHeader(Block header);

    String getName() const override { return "ColumnLazyTransform"; }

protected:
    void transform(Chunk & chunk) override;

private:
    LazilyReadInfoPtr lazily_read_info;
};

}
