#pragma once

#include <Parsers/IParserBase.h>
#include <Core/Field.h>


namespace DB
{

/// Parses a prometheus query, transforms it into
/// "SELECT * FROM prometheusQuery('database_name', 'table_name', 'promql_query', evaluation_time)"
class ParserPrometheusQuery final : public IParserBase
{
public:
    ParserPrometheusQuery(const String & database_name_, const String & table_name_, const Field & evaluation_time_);

    const char * getName() const override { return "PromQL Statement"; }

protected:
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    String database_name;
    String table_name;
    Field evaluation_time;
};

}
