#include <Parsers/Mongo/ParserMongoCompareFunctions.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{

namespace Mongo
{

bool ICompareFunction::parseImpl(ASTPtr & node)
{
    /// Comparisons must cover every scalar type the insert path can create a column
    /// from: bool, int, long, double, and String.
    Field scalar_value;
    if (data.IsBool())
        scalar_value = Field(data.GetBool());
    else if (data.IsInt())
        scalar_value = Field(data.GetInt());
    else if (data.IsInt64())
        scalar_value = Field(data.GetInt64());
    else if (data.IsNumber())
        scalar_value = Field(data.GetDouble());
    else if (data.IsString())
        scalar_value = Field(data.GetString());
    else
        return false;

    auto identifier = make_intrusive<ASTIdentifier>(edge_name);
    auto literal = make_intrusive<ASTLiteral>(std::move(scalar_value));
    node = makeASTFunction(getFunctionAlias(), identifier, literal);
    return true;
}

}

}
