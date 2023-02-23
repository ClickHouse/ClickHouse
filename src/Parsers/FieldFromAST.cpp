#include <Parsers/FieldFromAST.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

Field createFieldFromAST(ASTPtr ast)
{
    return CustomType(std::make_shared<FieldFromASTImpl>(ast));
}

[[noreturn]] void FieldFromASTImpl::throwNotImplemented(std::string_view method) const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Method {} not implemented for {}", method, getTypeName());
}

}
