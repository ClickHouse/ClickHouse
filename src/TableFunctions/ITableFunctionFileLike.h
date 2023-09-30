#pragma once

#include <TableFunctions/ITableFunction.h>
#include "Parsers/IAST_fwd.h"

namespace DB
{
class ColumnsDescription;
class Context;

/*
 * function(source, [format, structure, compression_method]) - creates a temporary storage from formatted source
 */
class ITableFunctionFileLike : public ITableFunction
{
public:
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

    static size_t getMaxNumberOfArguments() { return 4; }

    static void addColumnsStructureToArguments(ASTs & args, const String & structure, const ContextPtr &);

protected:

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;
    virtual void parseArgumentsImpl(ASTs & args, const ContextPtr & context);

    virtual void parseFirstArguments(const ASTPtr & arg, const ContextPtr & context);
    virtual String getFormatFromFirstArgument();

    String filename;
    String path_to_archive;
    String format = "auto";
    String structure = "auto";
    String compression_method = "auto";
    ColumnsDescription structure_hint;

private:
    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const override;

    virtual StoragePtr getStorage(
        const String & source, const String & format, const ColumnsDescription & columns, ContextPtr global_context,
        const std::string & table_name, const String & compression_method) const = 0;

    bool hasStaticStructure() const override { return structure != "auto"; }
};

}
