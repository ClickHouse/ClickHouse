#include <Parsers/FieldFromAST.h>

namespace DB
{

Field createFieldFromAST(ASTPtr ast)
{
    return CustomType(std::make_shared<FieldFromASTImpl>(ast));
}

}
