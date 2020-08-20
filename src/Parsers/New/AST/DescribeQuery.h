#pragma once

#include <Parsers/New/AST/Query.h>


namespace DB::AST
{

// TODO: rewrite to
//       `SELECT name, type, default_type, default_expression, comment, codec_expression, ttl_expression FROM system.columns
//        WHERE database=db AND table=table`

class DescribeQuery : public Query
{
    public:
        explicit DescribeQuery(PtrTo<TableIdentifier> identifier);
};

}
