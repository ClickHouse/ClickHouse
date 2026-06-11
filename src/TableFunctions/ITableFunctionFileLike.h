#pragma once

#include <TableFunctions/ITableFunction.h>
#include <Core/Names.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST_fwd.h>

#include <Storages/checkAndGetLiteralArgument.h>
#include <Interpreters/evaluateConstantExpression.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class ColumnsDescription;
class Context;

/*
 * function(source, [format, structure, compression_method]) - creates a temporary storage from formatted source
 */
class ITableFunctionFileLike : public ITableFunction
{
public:
    static constexpr auto max_number_of_arguments = 4;
    static constexpr auto signature = " - filename\n"
                                      " - filename, format\n"
                                      " - filename, format, structure\n"
                                      " - filename, format, structure, compression_method\n";
    virtual String getSignature() const
    {
        return signature;
    }

    bool needStructureHint() const override { return structure == "auto"; }

    void setStructureHint(const ColumnsDescription & structure_hint_) override { structure_hint = structure_hint_; }

    bool supportsReadingSubsetOfColumns(const ContextPtr & context) override;

    NameSet getVirtualsToCheckBeforeUsingStructureHint() const override;

    static size_t getMaxNumberOfArguments() { return max_number_of_arguments; }

    static void updateStructureAndFormatArgumentsIfNeeded(ASTs & args, const String & structure, const String & format, const ContextPtr & context, bool with_structure)
    {
        if (args.empty() || args.size() > getMaxNumberOfArguments())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected 1 to {} arguments in table function, got {}", getMaxNumberOfArguments(), args.size());

        auto format_literal = std::make_shared<ASTLiteral>(format);
        auto structure_literal = std::make_shared<ASTLiteral>(structure);

        for (auto & arg : args)
            arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

        /// f(filename)
        if (args.size() == 1)
        {
            args.push_back(format_literal);
            if (with_structure)
                args.push_back(structure_literal);
        }
        /// f(filename, format)
        else if (args.size() == 2)
        {
            if (checkAndGetLiteralArgument<String>(args[1], "format") == "auto")
                args.back() = format_literal;
            if (with_structure)
                args.push_back(structure_literal);
        }
        /// f(filename, format, structure) or f(filename, format, structure, compression) or f(filename, format, compression)
        else if (args.size() >= 3)
        {
            if (checkAndGetLiteralArgument<String>(args[1], "format") == "auto")
                args[1] = format_literal;
            if (with_structure && checkAndGetLiteralArgument<String>(args[2], "structure") == "auto")
                args[2] = structure_literal;
        }
    }

protected:

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;
    virtual void parseArgumentsImpl(ASTs & args, const ContextPtr & context);

    virtual void parseFirstArguments(const ASTPtr & arg, const ContextPtr & context);
    virtual std::optional<String> tryGetFormatFromFirstArgument();

    String filename;
    String format = "auto";
    String structure = "auto";
    String compression_method = "auto";
    ColumnsDescription structure_hint;

private:
    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const override;

    virtual StoragePtr getStorage(
        const String & source, const String & format, const ColumnsDescription & columns, ContextPtr global_context,
        const std::string & table_name, const String & compression_method, bool is_insert_query) const = 0;

    bool hasStaticStructure() const override { return structure != "auto"; }
};

}
