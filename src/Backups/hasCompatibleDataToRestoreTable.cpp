#include <Backups/hasCompatibleDataToRestoreTable.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/formatAST.h>


namespace DB
{

bool hasCompatibleDataToRestoreTable(const ASTCreateQuery & query1, const ASTCreateQuery & query2)
{
    /// TODO: Write more subtle condition here.
    auto q1 = typeid_cast<std::shared_ptr<ASTCreateQuery>>(query1.clone());
    auto q2 = typeid_cast<std::shared_ptr<ASTCreateQuery>>(query2.clone());

    /// Remove UUIDs.
    q1->uuid = UUIDHelpers::Nil;
    q2->uuid = UUIDHelpers::Nil;

    return serializeAST(*q1) == serializeAST(*q2);
}

}
