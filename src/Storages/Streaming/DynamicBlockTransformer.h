#pragma once

#include <Interpreters/ExpressionActions.h>

namespace DB
{

class DynamicBlockTransformer
{
public:
    explicit DynamicBlockTransformer(Block desired_);

    const Block & getHeader() const;

    /// Converts block from to desired header structure.
    /// Automatically recalculates converting actions on metadata change.
    void transform(Block & block);

private:
    Block desired_metadata;
    Block current_metadata;
    ExpressionActionsPtr converter;
};

}
