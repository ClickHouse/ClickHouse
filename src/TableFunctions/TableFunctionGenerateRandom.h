#pragma once

#include <TableFunctions/ITableFunction.h>

namespace DB
{

/* generateRandom([structure, max_array_length, max_string_length, random_seed])
 * - creates a temporary storage that generates columns with random data
 */
class TableFunctionGenerateRandom : public ITableFunction
{
public:
    static constexpr auto name = "generateRandom";
    std::string getName() const override { return name; }
    bool hasStaticStructure() const override { return structure != "auto"; }

    bool needStructureHint() const override { return structure == "auto"; }
    void setStructureHint(const ColumnsDescription & structure_hint_) override { structure_hint = structure_hint_; }

private:
    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const override;
    const char * getStorageTypeName() const override { return "GenerateRandom"; }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    String structure = "auto";
    UInt64 max_string_length = 10;
    UInt64 max_array_length = 10;
    std::optional<UInt64> random_seed;
    ColumnsDescription structure_hint;
};


}
