#pragma once

#include <Core/Field.h>
#include <Common/HiddenSecret.h>

#include <Core/ConstantValue.h>
#include <Analyzer/IQueryTreeNode.h>
#include <Columns/IColumn_fwd.h>
#include <Parsers/ASTLiteral.h>

#include <variant>

namespace DB
{

/** Constant node represents constant value in query tree.
  * Constant value must be representable by Field.
  * Examples: 1, 'constant_string', [1,2,3].
  *
  * Constant node can optionally keep pointer to its source expression.
  */
class ConstantNode;
using ConstantNodePtr = std::shared_ptr<ConstantNode>;

class ConstantNode final : public IQueryTreeNode
{
public:
    /// Construct constant query tree node from constant value, source expression and deterministic flag
    explicit ConstantNode(ConstantValue constant_value_, QueryTreeNodePtr source_expression, bool is_deterministic = true);

    /// Construct constant query tree node from constant value
    explicit ConstantNode(ConstantValue constant_value_);

    /** Construct constant query tree node from column and data type.
      *
      * Throws exception if value cannot be converted to value data type.
      */
    explicit ConstantNode(ColumnConstPtr constant_column_, DataTypePtr value_data_type_);

    /// Construct constant query tree node from column, data type will be derived from field value
    explicit ConstantNode(ColumnConstPtr constant_column_);

    /** Construct constant query tree node from field and data type.
      *
      * Throws exception if value cannot be converted to value data type.
      */
    explicit ConstantNode(Field value_, DataTypePtr value_data_type_);

    /// Construct constant query tree node from field, data type will be derived from field value
    explicit ConstantNode(Field value_);

    /// Get constant value
    const ColumnConstPtr & getColumn() const
    {
        return getConstantValue().getColumn();
    }

    /// Get constant value as a `Field` (materializes it). Prefer the typed getters below where a caller
    /// only needs a scalar - they avoid the `Field`.
    Field getValue() const
    {
        if (auto deferred_field = tryGetDeferredField())
            return *deferred_field;
        return getConstantValue().getField();
    }

    /** A long tuple of plain literals, such as the right side of `x IN (1, 2, ...)`, keeps its `Field` until a consumer
      * needs its column (one size-1 column per element). Returns that `Field` until then, else nullptr; the result keeps
      * it alive.
      */
    std::shared_ptr<const Field> tryGetDeferredField() const;

    /// Typed value accessors - read the constant without materializing a `Field` (delegate to ConstantValue).
    bool isNull() const { return getConstantValue().isNull(); }
    UInt64 getUInt() const { return getConstantValue().getUInt(); }
    Int64 getInt() const { return getConstantValue().getInt(); }
    Float64 getFloat64() const { return getConstantValue().getFloat64(); }
    bool getBool() const { return getConstantValue().getBool(); }
    std::string_view getDataAt() const { return getConstantValue().getDataAt(); }

    /// Get constant value string representation
    String getValueStringRepresentation() const;

    /// Returns true if constant node has source expression, false otherwise
    bool hasSourceExpression() const
    {
        return source_expression != nullptr;
    }

    /// Get source expression
    const QueryTreeNodePtr & getSourceExpression() const
    {
        return source_expression;
    }

    /// Get source expression
    QueryTreeNodePtr & getSourceExpression()
    {
        return source_expression;
    }

    QueryTreeNodeType getNodeType() const override
    {
        return QueryTreeNodeType::CONSTANT;
    }

    DataTypePtr getResultType() const override;

    /// Check if conversion to AST requires wrapping with _CAST function.
    static bool requiresCastCall(const DataTypePtr & field_type, const DataTypePtr & data_type);

    /// Check if constant is a result of _CAST function constant folding.
    bool receivedFromInitiatorServer() const;

    void setMaskId(size_t id = std::numeric_limits<decltype(mask_id)>::max())
    {
        mask_id = id;
    }

    /// Whether this constant is hidden as a secret (e.g. a key argument of `encrypt`).
    bool isMasked() const
    {
        return mask_id != 0;
    }

    /// Placeholder shown instead of the value when this constant is hidden as a secret.
    String getMaskString() const
    {
        chassert(isMasked());
        if (mask_id == std::numeric_limits<decltype(mask_id)>::max())
            return String(HIDDEN_SECRET);
        return "[HIDDEN id: " + std::to_string(mask_id) + "]";
    }

    void convertToNullable() override;

    void dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const override;

    String getValueName(const IColumn::Options & options) const
    {
        if (isMasked())
            return getMaskString();
        return getConstantValue().getValueName(options);
    }

    bool isDeterministic() const { return is_deterministic; }

protected:
    bool isEqualImpl(const IQueryTreeNode & rhs, CompareOptions compare_options) const override;

    void updateTreeHashImpl(HashState & hash_state, CompareOptions compare_options) const override;

    QueryTreeNodePtr cloneImpl() const override;

    template <typename F>
    boost::intrusive_ptr<ASTLiteral> getCachedAST(const F &ast_generator) const;
    ASTPtr toASTImpl(const ConvertToASTOptions & options) const override;

private:
    /// The `Field` and the type of a deferred constant, and its `ConstantValue` once built. Clones share it.
    struct DeferredValue;
    using ValueOrDeferred = std::variant<ConstantValue, std::shared_ptr<DeferredValue>>;

    ConstantNode(std::shared_ptr<DeferredValue> deferred_value_, QueryTreeNodePtr source_expression_, bool is_deterministic_);

    static ValueOrDeferred makeConstantValue(Field && value, DataTypePtr value_data_type);
    static ValueOrDeferred makeConstantValue(Field && value);

    const ConstantValue & getConstantValue() const
    {
        if (const auto * value = std::get_if<ConstantValue>(&constant_value)) [[likely]]
            return *value;
        return materializeDeferred();
    }

    const ConstantValue & materializeDeferred() const;

    ValueOrDeferred constant_value;
    QueryTreeNodePtr source_expression;
    bool is_deterministic = true;
    size_t mask_id = 0;

    static constexpr size_t children_size = 0;

    /// Converting to AST maybe costly (for example for large arrays), so we want
    /// to cache it using hash to check for update
    mutable boost::intrusive_ptr<ASTLiteral> cached_ast;
    mutable Hash hash_ast;
};

}
