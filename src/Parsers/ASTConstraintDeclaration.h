#pragma once

#include <Parsers/IAST.h>

namespace DB
{

/** name CHECK logical_expr
 *  name ASSUME logical_expr
 *  name UNIQUE (col1, col2, ...) TYPE engine_name
 */
class ASTConstraintDeclaration : public IAST
{
public:
    enum class Type : UInt8
    {
        CHECK,
        ASSUME,
        UNIQUE,
    };

    String name;
    Type type;

    /// Expression for CHECK / ASSUME constraints
    IAST * expr = nullptr;

    /// Column list for UNIQUE constraints (children of ASTExpressionList)
    ASTPtr unique_columns;

    /// Engine type for UNIQUE constraints: "hash128", "rocksdb"
    String unique_engine_type;

    String getID(char) const override { return "Constraint"; }

    ASTPtr clone() const override;

    void forEachPointerToChild(std::function<void(IAST **, boost::intrusive_ptr<IAST> *)> f) override
    {
        f(&expr, nullptr);
    }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
