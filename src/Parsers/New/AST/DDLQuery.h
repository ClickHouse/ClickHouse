#pragma once

#include <Parsers/New/AST/Query.h>

#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/New/AST/Identifier.h>


namespace DB::AST
{

class DDLQuery : public Query
{
    protected:
        DDLQuery(PtrTo<ClusterClause> cluster, std::initializer_list<Ptr> list)
            : Query(list), cluster_name(cluster ? cluster->convertToOld()->as<ASTLiteral>()->value.get<String>() : String{})
        {
        }

        DDLQuery(PtrTo<ClusterClause> cluster, PtrList list)
            : Query(list), cluster_name(cluster ? cluster->convertToOld()->as<ASTLiteral>()->value.get<String>() : String{})
        {
        }

        const String cluster_name;
};

}
