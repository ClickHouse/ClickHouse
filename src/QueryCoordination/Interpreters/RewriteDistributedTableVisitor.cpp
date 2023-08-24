//#include <QueryCoordination/Interpreters/RewriteDistributedTableVisitor.h>
//
//namespace DB
//{
//
//bool RewriteDistributedTableMatcher::needChildVisit(const ASTPtr &, const ASTPtr &)
//{
//    return true;
//}
//
//void RewriteDistributedTableMatcher::visit(ASTPtr & ast, Data & data)
//{
//    if (auto * t = ast->as<ASTSelectQuery>())
//        visit(*t, ast, data);
//}
//
//void RewriteDistributedTableMatcher::visit(ASTSelectQuery & select, ASTPtr & ast, Data & data)
//{
//
//}
//
//}
