#include <Parsers/FieldFromAST.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/isDiskFunction.h>
#include <Common/HiddenSecret.h>
#include <Common/assert_cast.h>
#include <Interpreters/InDepthNodeVisitor.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

Field createFieldFromAST(ASTPtr ast)
{
    return CustomType(std::make_shared<FieldFromASTImpl>(ast));
}

[[noreturn]] void FieldFromASTImpl::throwNotImplemented(std::string_view method) const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Method {} not implemented for {}", method, getTypeName());
}

bool FieldFromASTImpl::operator == (const CustomTypeImpl & rhs) const
{
    if (std::string_view(getTypeName()) != std::string_view(rhs.getTypeName()))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Comparing custom types with different type names: {} and {}",
            getTypeName(), rhs.getTypeName());

    /// Compare the unmasked text: masking rewrites every credential to `[HIDDEN]`, so two values
    /// differing only in a credential would compare equal.
    return toString(/*show_secrets=*/ true) == rhs.toString(/*show_secrets=*/ true);
}

bool FieldFromASTImpl::isSecret() const
{
    return isDiskFunction(ast);
}

class DiskConfigurationMasker
{
public:
    struct Data {};

    static bool needChildVisit(const ASTPtr &, const ASTPtr &) { return true; }

    static void visit(ASTPtr & ast, Data &)
    {
        if (isDiskFunction(ast))
        {
            auto & disk_function = assert_cast<ASTFunction &>(*ast);
            auto * disk_function_args_expr = assert_cast<ASTExpressionList *>(disk_function.arguments.get());
            auto & disk_function_args = disk_function_args_expr->children;

            auto is_secret_arg = [](const std::string & arg_name)
            {
                /// We allow to not hide type of the disk, e.g. disk(type = s3, ...)
                /// and also nested disk, e.g. disk = 'disk_name'
                return arg_name != "type" && arg_name != "disk" && arg_name != "name" ;
            };

            for (auto & arg : disk_function_args)
            {
                auto * setting_function = arg->as<ASTFunction>();
                auto * function_args_expr = setting_function && setting_function->name == "equals" && setting_function->arguments
                    ? setting_function->arguments->as<ASTExpressionList>()
                    : nullptr;
                const auto * key_identifier = function_args_expr && !function_args_expr->children.empty()
                    ? function_args_expr->children[0]->as<ASTIdentifier>()
                    : nullptr;

                /// Not `key = value`, so the parser rejects it: hide it whole, a throw here would log the query unmasked.
                if (!key_identifier)
                {
                    arg = make_intrusive<ASTLiteral>(String(HIDDEN_SECRET));
                    continue;
                }

                if (is_secret_arg(key_identifier->name()))
                {
                    auto & function_args = function_args_expr->children;
                    for (size_t i = 1; i < function_args.size(); ++i)
                        function_args[i] = make_intrusive<ASTLiteral>(String(HIDDEN_SECRET));
                }
            }
        }
    }
};

/// Visits children first.
using HideDiskConfigurationVisitor = InDepthNodeVisitor<DiskConfigurationMasker, false>;

String FieldFromASTImpl::toString(bool show_secrets) const
{
    if (!show_secrets && isDiskFunction(ast))
    {
        auto hidden = ast->clone();
        HideDiskConfigurationVisitor::Data data{};
        HideDiskConfigurationVisitor{data}.visit(hidden);
        return hidden->formatWithSecretsOneLine();
    }

    return ast->formatWithSecretsOneLine();
}

}
