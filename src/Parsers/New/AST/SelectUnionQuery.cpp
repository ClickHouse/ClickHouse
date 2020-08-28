#include <Parsers/New/AST/SelectUnionQuery.h>

#include <Parsers/New/AST/SelectStmt.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>

namespace DB::AST
{

SelectUnionQuery::SelectUnionQuery(std::list<PtrTo<SelectStmt>> stmts)
{
    children.insert(children.end(), stmts.begin(), stmts.end());
}

void SelectUnionQuery::appendSelect(PtrTo<SelectStmt> stmt)
{
    children.push_back(stmt);
}

ASTPtr SelectUnionQuery::convertToOld() const
{
    auto old_select_union = std::make_shared<ASTSelectWithUnionQuery>();
    old_select_union->list_of_selects = std::make_shared<ASTExpressionList>();
    old_select_union->children.push_back(old_select_union->list_of_selects);

    for (const auto & select : children) old_select_union->list_of_selects->children.push_back(select->convertToOld());

    return old_select_union;
}

}
