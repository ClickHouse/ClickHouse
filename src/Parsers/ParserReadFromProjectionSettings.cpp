#include <Parsers/ParserReadFromProjectionSettings.h>
#include <Parsers/ASTReadFromProjectionSettings.h>
#include <Parsers/ExpressionElementParsers.h>

namespace DB
{

bool ParserReadFromProjectionSettings::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr name;
    if (!ParserIdentifier().parse(pos, name, expected))
        return false;

    auto read_from_projection_settings = make_intrusive<ASTReadFromProjectionSettings>();
    read_from_projection_settings->setName(std::move(name));

    node = std::move(read_from_projection_settings);
    return true;
}

}
