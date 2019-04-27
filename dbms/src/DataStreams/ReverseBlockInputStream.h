#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Common/PODArray.h>

namespace DB
{

/** Allows to read data in reverse order
 *  Currently used in group by optimization
 */
class ReverseBlockInputStream : public IBlockInputStream
{
public:
    ReverseBlockInputStream(
        const BlockInputStreamPtr& input);

    String getName() const override;
    Block getHeader() const override;

protected:
    Block readImpl() override;
};

}
