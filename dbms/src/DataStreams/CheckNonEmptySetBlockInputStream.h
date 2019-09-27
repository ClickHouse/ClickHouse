#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Core/Names.h>


namespace DB {


class CheckNonEmptySetBlockInputStream : public IBlockInputStream
{
private:
    using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

public:
    CheckNonEmptySetBlockInputStream(const BlockInputStreamPtr & input, const ExpressionActionsPtr & expression_, const NameSet sets_);

    String getName() const override;
    Block getTotals() override;
    Block getHeader() const override;

protected:
    Block readImpl() override;

private:
    Block cached_header;
    ExpressionActionsPtr expression;
    bool initialized = false;
    bool cached_result = false;
    NameSet sets;

    bool inOrInnerRightJoinWithEmpty() const;


/**
 * Used to determine if actions are IN OR INNER/RIGHT JOIN with empty.
 */
struct InOrInnerRightJoinWithEmpty
{
    bool hasJoin = false;
    bool hasIn = false;
    bool innerRightJoinWithEmpty = true;
    bool inWithEmpty = true;

    bool result()
    {
        if (hasJoin && !hasIn)
            return innerRightJoinWithEmpty;

        else if (hasIn && !hasJoin)
            return inWithEmpty;

        else if (hasJoin && hasIn)
            return innerRightJoinWithEmpty && inWithEmpty;

        return false;
    }

};

};

}
