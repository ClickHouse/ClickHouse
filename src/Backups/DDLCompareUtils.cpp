#include <Backups/DDLCompareUtils.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/formatAST.h>


namespace DB
{
namespace
{
    std::shared_ptr<const ASTCreateQuery> prepareDDLToCompare(const ASTCreateQuery & ast)
    {
        auto res = typeid_cast<std::shared_ptr<const ASTCreateQuery>>(ast.shared_from_this());

        std::shared_ptr<ASTCreateQuery> clone;
        auto get_clone = [&]
        {
            if (!clone)
            {
                clone = typeid_cast<std::shared_ptr<ASTCreateQuery>>(res->clone());
                res = clone;
            }
            return clone;
        };

        /// Remove UUIDs.
        if (res->uuid != UUIDHelpers::Nil)
            get_clone()->uuid = UUIDHelpers::Nil;

        if (res->to_inner_uuid != UUIDHelpers::Nil)
            get_clone()->to_inner_uuid = UUIDHelpers::Nil;

        /// Clear IF NOT EXISTS flag.
        if (res->if_not_exists)
            get_clone()->if_not_exists = false;

        return res;
    }
}


bool areTableDefinitionsSame(const IAST & table1, const IAST & table2)
{
    auto ast1 = typeid_cast<std::shared_ptr<const ASTCreateQuery>>(table1.shared_from_this());
    if (!ast1 || !ast1->table)
        return false;

    auto ast2 = typeid_cast<std::shared_ptr<const ASTCreateQuery>>(table2.shared_from_this());
    if (!ast2 || !ast2->table)
        return false;

    if ((ast1->uuid != ast2->uuid) || (ast1->to_inner_uuid != ast2->to_inner_uuid) ||
        (ast1->if_not_exists != ast2->if_not_exists))
    {
        ast1 = prepareDDLToCompare(*ast1);
        ast2 = prepareDDLToCompare(*ast2);
    }

    return serializeAST(*ast1) == serializeAST(*ast1);
}


bool areDatabaseDefinitionsSame(const IAST & database1, const IAST & database2)
{
    auto ast1 = typeid_cast<std::shared_ptr<const ASTCreateQuery>>(database1.shared_from_this());
    if (!ast1 || ast1->table || !ast1->database)
        return false;

    auto ast2 = typeid_cast<std::shared_ptr<const ASTCreateQuery>>(database2.shared_from_this());
    if (!ast2 || ast2->table || !ast2->database)
        return false;

    if ((ast1->uuid != ast2->uuid) || (ast1->if_not_exists != ast2->if_not_exists))
    {
        ast1 = prepareDDLToCompare(*ast1);
        ast2 = prepareDDLToCompare(*ast2);
    }

    return serializeAST(*ast1) == serializeAST(*ast1);
}


bool areTableDataCompatible(const IAST & src_table, const IAST & dest_table)
{
    return areTableDefinitionsSame(src_table, dest_table);
}

}
