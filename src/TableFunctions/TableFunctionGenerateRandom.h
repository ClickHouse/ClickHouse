#pragma once

#include <TableFunctions/ITableFunction.h>

namespace DB
{

/* generateRandom(structure, [max_array_length, max_string_length, random_seed])
 * - creates a temporary storage that generates columns with random data
 */
class TableFunctionGenerateRandom : public ITableFunction
{
public:
    static constexpr auto name = "generateRandom";
    std::string getName() const override { return name; }
    bool hasStaticStructure() const override { return true; }
private:
    StoragePtr executeImpl(const ASTPtr & ast_function, const Context & context, const std::string & table_name, ColumnsDescription cached_columns) const override;
    const char * getStorageTypeName() const override { return "GenerateRandom"; }

    ColumnsDescription getActualTableStructure(const Context & context) const override;
    void parseArguments(const ASTPtr & ast_function, const Context & context) override;

    String structure;
    UInt64 max_string_length = 10;
    UInt64 max_array_length = 10;
    std::optional<UInt64> random_seed;

};


}
