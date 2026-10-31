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

    /// PromQL is parsed from the raw text by its own grammar, which has tokens such as `=~` that
    /// the SQL lexer rejects; the SQL tokens are only used here to find the end of the statement.
    bool consumesRawText() const override { return true; }

protected:
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    String database_name;
    String table_name;
    Field evaluation_time;
};

}
