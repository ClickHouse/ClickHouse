#pragma once

#include <Core/Names.h>
#include <Core/Block_fwd.h>
#include <Processors/ISimpleTransform.h>

namespace DB
{

struct ColumnDefault;
using ColumnDefaults = std::unordered_map<std::string, ColumnDefault>;

class ColumnsDescription;
class IInputFormat;

/// Materializes simple ALIAS columns
///
/// Uses BlockMissingValues
class MaterializingAliasesTransform : public ISimpleTransform
{
public:
    MaterializingAliasesTransform(SharedHeader header, const ColumnDefaults & column_defaults, IInputFormat & input_format_);

    String getName() const override { return "MaterializingAliasesTransform"; }

    /// Build map of simple aliases (ALIAS col_name) to their target columns
    static NameToNameMap getAliasToColumnMap(const ColumnDefaults & column_defaults);
    /// Return columns that ALIAS to another column without any expressions
    static ColumnsWithTypeAndName getColumnAliases(const ColumnsDescription & columns_description);

protected:
    void transform(Chunk & chunk) override;

private:
    /// Map from alias column name to the actual column name it references
    NameToNameMap aliases;
    IInputFormat & input_format;
};

}
