#include <Parsers/isKeyValueArgument.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>


namespace DB
{

bool isKeyValueArgument(const IAST & argument)
{
    const auto * function = argument.as<ASTFunction>();
    return function && function->name == "equals" && function->arguments && function->arguments->children.size() == 2
        && function->arguments->children[0]->as<ASTIdentifier>();
}

}
