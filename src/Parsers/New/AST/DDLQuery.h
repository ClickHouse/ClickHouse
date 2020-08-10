#pragma once

#include <Parsers/New/AST/Query.h>

#include <Parsers/New/AST/Identifier.h>


namespace DB::AST
{

class DDLQuery : public Query
{
    public:
        // ON CLUSTER identifier always goes as the last child.
        void setOnCluster(PtrTo<Identifier> identifier);

    private:
        PtrTo<Identifier> cluster;
};

}
