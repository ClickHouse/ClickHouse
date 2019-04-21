#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Common/SharedBlockRowRef.h>

namespace DB
{

/** Implements the WITH FILL part of ORDER BY operation.
*/
class FillingBlockInputStream : public IBlockInputStream
{
public:
    FillingBlockInputStream(const BlockInputStreamPtr & input, const SortDescription & description_);

    String getName() const override { return "With fill"; }

    Block getHeader() const override { return children.at(0)->getHeader(); }

protected:
    Block readImpl() override;

private:
    const SortDescription description;
    SharedBlockRowRef last_row_ref;
};


}