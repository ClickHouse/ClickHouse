#include <Analyzer/SortColumnNode.h>

#include <Common/SipHash.h>

#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{

const char * toString(SortDirection sort_direction)
{
    switch (sort_direction)
    {
        case SortDirection::ASCENDING: return "ASCENDING";
        case SortDirection::DESCENDING: return "DESCENDING";
    }
}

SortColumnNode::SortColumnNode(QueryTreeNodePtr expression_,
    SortDirection sort_direction_,
    std::optional<SortDirection> nulls_sort_direction_,
    std::shared_ptr<Collator> collator_,
    bool with_fill_)
    : sort_direction(sort_direction_)
    , nulls_sort_direction(nulls_sort_direction_)
    , collator(std::move(collator_))
    , with_fill(with_fill_)
{
    children.resize(children_size);
    children[sort_expression_child_index] = std::move(expression_);
}

String SortColumnNode::getName() const
{
    String result = getExpression()->getName();

    if (sort_direction == SortDirection::ASCENDING)
        result += " ASC";
    else
        result += " DESC";

    if (nulls_sort_direction)
    {
        if (*nulls_sort_direction == SortDirection::ASCENDING)
            result += " NULLS FIRST";
        else
            result += " NULLS LAST";
    }

    if (with_fill)
        result += " WITH FILL";

    if (hasFillFrom())
        result += " FROM " + getFillFrom()->getName();

    if (hasFillStep())
        result += " STEP " + getFillStep()->getName();

    if (hasFillTo())
        result += " TO " + getFillTo()->getName();

    return result;
}

void SortColumnNode::dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const
{
    buffer << std::string(indent, ' ') << "SORT_COLUMN id: " << format_state.getNodeId(this);

    buffer << ", sort_direction: " << toString(sort_direction);
    if (nulls_sort_direction)
        buffer << ", nulls_sort_direction: " << toString(*nulls_sort_direction);

    if (collator)
        buffer << ", collator: " << collator->getLocale();

    buffer << ", with_fill: " << with_fill;

    buffer << '\n' << std::string(indent + 2, ' ') << "EXPRESSION\n";
    getExpression()->dumpTreeImpl(buffer, format_state, indent + 4);

    if (hasFillFrom())
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "FILL FROM\n";
        getFillFrom()->dumpTreeImpl(buffer, format_state, indent + 4);
    }

    if (hasFillTo())
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "FILL TO\n";
        getFillTo()->dumpTreeImpl(buffer, format_state, indent + 4);
    }

    if (hasFillStep())
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "FILL STEP\n";
        getFillStep()->dumpTreeImpl(buffer, format_state, indent + 4);
    }
}

bool SortColumnNode::isEqualImpl(const IQueryTreeNode & rhs) const
{
    const auto & rhs_typed = assert_cast<const SortColumnNode &>(rhs);
    if (sort_direction != rhs_typed.sort_direction ||
        nulls_sort_direction != rhs_typed.nulls_sort_direction ||
        with_fill != rhs_typed.with_fill)
        return false;

    if (!collator && !rhs_typed.collator)
        return true;
    else if (collator && !rhs_typed.collator)
        return false;
    else if (!collator && rhs_typed.collator)
        return false;

    return collator->getLocale() == rhs_typed.collator->getLocale();
}

void SortColumnNode::updateTreeHashImpl(HashState & hash_state) const
{
    hash_state.update(sort_direction);
    hash_state.update(nulls_sort_direction);
    hash_state.update(with_fill);

    if (collator)
    {
        const auto & locale = collator->getLocale();

        hash_state.update(locale.size());
        hash_state.update(locale);
    }
}

ASTPtr SortColumnNode::toASTImpl() const
{
    auto result = std::make_shared<ASTOrderByElement>();
    result->direction = sort_direction == SortDirection::ASCENDING ? 1 : -1;
    result->nulls_direction = result->direction;
    if (nulls_sort_direction)
        result->nulls_direction = *nulls_sort_direction == SortDirection::ASCENDING ? 1 : -1;

    result->nulls_direction_was_explicitly_specified = nulls_sort_direction.has_value();
    if (collator)
        result->collation = std::make_shared<ASTLiteral>(Field(collator->getLocale()));

    result->with_fill = with_fill;
    result->fill_from = hasFillFrom() ? getFillFrom()->toAST() : nullptr;
    result->fill_to = hasFillTo() ? getFillTo()->toAST() : nullptr;
    result->fill_step = hasFillStep() ? getFillStep()->toAST() : nullptr;
    result->children.push_back(getExpression()->toAST());

    return result;
}

QueryTreeNodePtr SortColumnNode::cloneImpl() const
{
    return std::make_shared<SortColumnNode>(nullptr /*expression*/, sort_direction, nulls_sort_direction, collator, with_fill);
}

}
