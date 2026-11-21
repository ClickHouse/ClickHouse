#pragma once

#include <Parsers/SPARQL/SparqlAST.h>
#include <string>

namespace DB
{
namespace SPARQL
{

class SparqlQueryParser
{
public:
    std::unique_ptr<SelectQuery> parse(const std::string & sparql);
};

}
}
