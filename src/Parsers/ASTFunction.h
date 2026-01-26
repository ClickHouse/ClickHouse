#pragma once

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTWithAlias.h>
#include <Parsers/NullsAction.h>


namespace DB
{

class ASTSelectWithUnionQuery;

/** AST for function application or operator.
  */
class ASTFunction : public ASTWithAlias
{
public:
    /// Default constructor initializes FLAGS = 0, which means:
    /// - isOperator() = false
    /// - isWindowFunction() = false
    /// - computeAfterWindowFunctions() = false
    /// - isLambdaFunction() = false
    /// - preferSubqueryToFunctionFormatting() = false
    /// - noEmptyArgs() = false
    /// - isCompoundName() = false
    /// - getNullsAction() = NullsAction::EMPTY
    /// - getKind() = Kind::ORDINARY_FUNCTION
    ASTFunction() = default;

    String name;
    ASTPtr arguments;
    /// parameters - for parametric aggregate function. Example: quantile(0.9)(x) - what in first parens are 'parameters'.
    ASTPtr parameters;

    /// Preserves the information that it was parsed as an operator. This is needed for better formatting the AST back to query.
    bool isOperator() const { return FLAGS & IS_OPERATOR; }
    void setIsOperator(bool value) { FLAGS = value ? (FLAGS | IS_OPERATOR) : (FLAGS & ~IS_OPERATOR); }

    bool isWindowFunction() const { return FLAGS & IS_WINDOW_FUNCTION; }
    void setIsWindowFunction(bool value) { FLAGS = value ? (FLAGS | IS_WINDOW_FUNCTION) : (FLAGS & ~IS_WINDOW_FUNCTION); }

    bool computeAfterWindowFunctions() const { return FLAGS & COMPUTE_AFTER_WINDOW_FUNCTIONS; }
    void setComputeAfterWindowFunctions(bool value) { FLAGS = value ? (FLAGS | COMPUTE_AFTER_WINDOW_FUNCTIONS) : (FLAGS & ~COMPUTE_AFTER_WINDOW_FUNCTIONS); }

    bool isLambdaFunction() const { return FLAGS & IS_LAMBDA_FUNCTION; }
    void setIsLambdaFunction(bool value) { FLAGS = value ? (FLAGS | IS_LAMBDA_FUNCTION) : (FLAGS & ~IS_LAMBDA_FUNCTION); }

    /// This field is updated in executeTableFunction if its a parameterized_view
    /// and used in ASTTablesInSelectQuery::FormatImpl for EXPLAIN SYNTAX of SELECT parameterized view
    bool preferSubqueryToFunctionFormatting() const { return FLAGS & PREFER_SUBQUERY_TO_FUNCTION_FORMATTING; }
    void setPreferSubqueryToFunctionFormatting(bool value) { FLAGS = value ? (FLAGS | PREFER_SUBQUERY_TO_FUNCTION_FORMATTING) : (FLAGS & ~PREFER_SUBQUERY_TO_FUNCTION_FORMATTING); }

    // We have to make these fields ASTPtr because this is what the visitors
    // expect. Some of them take const ASTPtr & (makes no sense), and some
    // take ASTPtr & and modify it. I don't understand how the latter is
    // compatible with also having an owning `children` array -- apparently it
    // leads to some dangling children that are not referenced by the fields of
    // the AST class itself. Some older code hints at the idea of having
    // ownership in `children` only, and making the class fields to be raw
    // pointers of proper type (see e.g. IAST::set), but this is not compatible
    // with the visitor interface.

    String window_name;
    ASTPtr window_definition;

    NullsAction getNullsAction() const { return static_cast<NullsAction>((FLAGS & NULLS_ACTION_MASK) >> NULLS_ACTION_SHIFT); }
    void setNullsAction(NullsAction value) { FLAGS = (FLAGS & ~NULLS_ACTION_MASK) | (static_cast<UInt32>(value) << NULLS_ACTION_SHIFT); }

    /// do not print empty parentheses if there are no args - compatibility with engine names.
    bool noEmptyArgs() const { return FLAGS & NO_EMPTY_ARGS; }
    void setNoEmptyArgs(bool value) { FLAGS = value ? (FLAGS | NO_EMPTY_ARGS) : (FLAGS & ~NO_EMPTY_ARGS); }

    /// Specifies where this function-like expression is used.
    enum class Kind : UInt8
    {
        ORDINARY_FUNCTION,
        WINDOW_FUNCTION,
        LAMBDA_FUNCTION,
        TABLE_ENGINE,
        DATABASE_ENGINE,
        BACKUP_NAME,
        CODEC,
        STATISTICS,
    };
    Kind getKind() const { return static_cast<Kind>((FLAGS & KIND_MASK) >> KIND_SHIFT); }
    void setKind(Kind value) { FLAGS = (FLAGS & ~KIND_MASK) | (static_cast<UInt32>(value) << KIND_SHIFT); }

    /** Get text identifying the AST node. */
    String getID(char delim) const override;

    ASTPtr clone() const override;

    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

    ASTSelectWithUnionQuery * tryGetQueryArgument() const;

    ASTPtr toLiteral() const;  // Try to convert functions like Array or Tuple to a literal form.

    /// This is used for parameterized view, to identify if name is 'db.view'
    bool isCompoundName() const { return FLAGS & IS_COMPOUND_NAME; }
    void setIsCompoundName(bool value) { FLAGS = value ? (FLAGS | IS_COMPOUND_NAME) : (FLAGS & ~IS_COMPOUND_NAME); }

    bool hasSecretParts() const override;

protected:
    void formatImplWithoutAlias(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
    void appendColumnNameImpl(WriteBuffer & ostr) const override;

    /// Bit flags for ASTFunction (bits 1-12, bit 0 is used by ASTWithAlias::PREFER_ALIAS_TO_COLUMN_NAME)
    static constexpr UInt32 IS_OPERATOR = 1u << 1;
    static constexpr UInt32 IS_WINDOW_FUNCTION = 1u << 2;
    static constexpr UInt32 COMPUTE_AFTER_WINDOW_FUNCTIONS = 1u << 3;
    static constexpr UInt32 IS_LAMBDA_FUNCTION = 1u << 4;
    static constexpr UInt32 PREFER_SUBQUERY_TO_FUNCTION_FORMATTING = 1u << 5;
    static constexpr UInt32 NO_EMPTY_ARGS = 1u << 6;
    static constexpr UInt32 IS_COMPOUND_NAME = 1u << 7;
    static constexpr UInt32 NULLS_ACTION_SHIFT = 8;
    static constexpr UInt32 NULLS_ACTION_MASK = 0b11u << NULLS_ACTION_SHIFT;  /// 2 bits for NullsAction (3 values)
    static constexpr UInt32 KIND_SHIFT = 10;
    static constexpr UInt32 KIND_MASK = 0b111u << KIND_SHIFT;  /// 3 bits for Kind (8 values)

private:
    void finishFormatWithWindow(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const;
};


template <typename... Args>
boost::intrusive_ptr<ASTFunction> makeASTFunction(std::string_view name, Args &&... args)
{
    auto function = make_intrusive<ASTFunction>();

    function->name = name;

    function->arguments = make_intrusive<ASTExpressionList>();
    function->children.push_back(function->arguments);

    function->arguments->children = { std::forward<Args>(args)... };

    return function;
}

template <typename... Args>
boost::intrusive_ptr<ASTFunction> makeASTOperator(const String & name, Args &&... args)
{
    auto function = makeASTFunction(name, std::forward<Args>(args)...);
    function->setIsOperator(true);
    return function;
}

/// ASTFunction Helpers: hide casts and semantic.

String getFunctionName(const IAST * ast);
std::optional<String> tryGetFunctionName(const IAST * ast);
bool tryGetFunctionNameInto(const IAST * ast, String & name);

inline String getFunctionName(const ASTPtr & ast) { return getFunctionName(ast.get()); }
inline std::optional<String> tryGetFunctionName(const ASTPtr & ast) { return tryGetFunctionName(ast.get()); }
inline bool tryGetFunctionNameInto(const ASTPtr & ast, String & name) { return tryGetFunctionNameInto(ast.get(), name); }

/// Checks if function is a lambda function definition `lambda((x, y), x + y)`
bool isASTLambdaFunction(const ASTFunction & function);

}
