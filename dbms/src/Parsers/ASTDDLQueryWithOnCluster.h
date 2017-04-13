#pragma once

#include <Parsers/IAST.h>


namespace DB
{

class ASTDDLQueryWithOnCluster
{
public:

    String cluster;

    virtual ASTPtr getRewrittenASTWithoutOnCluster(const std::string & new_database = {}) const = 0;

    std::string getRewrittenQueryWithoutOnCluster(const std::string & new_database = {}) const;

    virtual ~ASTDDLQueryWithOnCluster() = default;
};

}
