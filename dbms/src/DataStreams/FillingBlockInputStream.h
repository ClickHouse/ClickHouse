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
    FillingBlockInputStream(const BlockInputStreamPtr & input, const SortDescription & fill_description_);

    String getName() const override { return "WithFill"; }

    Block getHeader() const override { return children.at(0)->getHeader(); }

protected:
    Block readImpl() override;

private:
    UInt64 pos = 0; /// total number of read rows
    const SortDescription fill_description; /// contains only rows with WITH_FILL
    SharedBlockRowRef last_row_ref; /// ref to last written row
};


}