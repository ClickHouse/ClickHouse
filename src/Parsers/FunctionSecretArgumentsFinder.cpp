#include <Parsers/FunctionSecretArgumentsFinder.h>
#include <Parsers/FunctionSecretArgumentsFinderAST.h>
#include <Analyzer/FunctionSecretArgumentsFinderTreeNode.h>


namespace DB
{

FunctionSecretArgumentsFinder::Result FunctionSecretArgumentsFinder::find(const ASTFunction & function)
{
    return FunctionSecretArgumentsFinderAST(function).getResult();
}

FunctionSecretArgumentsFinder::Result FunctionSecretArgumentsFinder::find(const FunctionNode & function)
{
    return FunctionSecretArgumentsFinderTreeNode(function).getResult();
}

}
