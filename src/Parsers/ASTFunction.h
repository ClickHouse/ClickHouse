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
    struct ASTFunctionFlags
    {
        using ParentFlags = ASTWithAlias::ASTWithAliasFlags;
        static constexpr UInt32 RESERVED_BITS = ParentFlags::RESERVED_BITS + 12;

        UInt32 _parent_reserved : ParentFlags::RESERVED_BITS;
        UInt32 is_operator : 1;
        UInt32 is_window_function : 1;
        UInt32 compute_after_window_functions : 1;
        UInt32 is_lambda_function : 1;
        UInt32 prefer_subquery_to_function_formatting : 1;
        UInt32 no_empty_args : 1;
        UInt32 is_compound_name : 1;
        UInt32 nulls_action : 2; /// 2 bits for NullsAction (3 values)
        UInt32 kind : 3; /// 3 bits for Kind (8 values)
        UInt32 unused : 19;
    };

public:
    ASTFunction() = default;

    String name;
    ASTPtr arguments;
    /// parameters - for parametric aggregate function. Example: quantile(0.9)(x) - what in first parens are 'parameters'.
    ASTPtr parameters;

    /// Preserves the information that it was parsed as an operator. This is needed for better formatting the AST back to query.
    bool isOperator() const { return flags<ASTFunctionFlags>().is_operator; }
    void setIsOperator(bool value) { flags<ASTFunctionFlags>().is_operator = value; }

    bool isWindowFunction() const { return flags<ASTFunctionFlags>().is_window_function; }
    void setIsWindowFunction(bool value) { flags<ASTFunctionFlags>().is_window_function = value; }

    bool computeAfterWindowFunctions() const { return flags<ASTFunctionFlags>().compute_after_window_functions; }
    void setComputeAfterWindowFunctions(bool value) { flags<ASTFunctionFlags>().compute_after_window_functions = value; }

    bool isLambdaFunction() const { return flags<ASTFunctionFlags>().is_lambda_function; }
    void setIsLambdaFunction(bool value) { flags<ASTFunctionFlags>().is_lambda_function = value; }

    /// This field is updated in executeTableFunction if its a parameterized_view
    /// and used in ASTTablesInSelectQuery::FormatImpl for EXPLAIN SYNTAX of SELECT parameterized view
    bool preferSubqueryToFunctionFormatting() const { return flags<ASTFunctionFlags>().prefer_subquery_to_function_formatting; }
    void setPreferSubqueryToFunctionFormatting(bool value) { flags<ASTFunctionFlags>().prefer_subquery_to_function_formatting = value; }

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

    NullsAction getNullsAction() const { return static_cast<NullsAction>(flags<ASTFunctionFlags>().nulls_action); }
    void setNullsAction(NullsAction value) { flags<ASTFunctionFlags>().nulls_action = static_cast<UInt32>(value); }

    /// do not print empty parentheses if there are no args - compatibility with engine names.
    bool noEmptyArgs() const { return flags<ASTFunctionFlags>().no_empty_args; }
    void setNoEmptyArgs(bool value) { flags<ASTFunctionFlags>().no_empty_args = value; }

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
    Kind getKind() const { return static_cast<Kind>(flags<ASTFunctionFlags>().kind); }
    void setKind(Kind value) { flags<ASTFunctionFlags>().kind = static_cast<UInt32>(value); }

    /** Get text identifying the AST node. */
    String getID(char delim) const override;

    ASTPtr clone() const override;

    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

    ASTSelectWithUnionQuery * tryGetQueryArgument() const;

    ASTPtr toLiteral() const;  // Try to convert functions like Array or Tuple to a literal form.

    /// This is used for parameterized view, to identify if name is 'db.view'
    bool isCompoundName() const { return flags<ASTFunctionFlags>().is_compound_name; }
    void setIsCompoundName(bool value) { flags<ASTFunctionFlags>().is_compound_name = value; }

    bool hasSecretParts() const override;

protected:
    void formatImplWithoutAlias(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
    void appendColumnNameImpl(WriteBuffer & ostr) const override;

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

/// Adds a parameters to aggregate function.
inline boost::intrusive_ptr<ASTFunction> addParametersToAggregateFunction(boost::intrusive_ptr<ASTFunction> && function) { return std::move(function); }

template <typename... OtherParameters>
boost::intrusive_ptr<ASTFunction> addParametersToAggregateFunction(boost::intrusive_ptr<ASTFunction> && function, ASTPtr parameter, OtherParameters &&... other_parameters)
{
    if (!function->parameters)
    {
        function->parameters = make_intrusive<ASTExpressionList>();
        function->children.push_back(function->parameters);
    }
    function->parameters->children.push_back(std::move(parameter));
    return addParametersToAggregateFunction(std::move(function), std::forward<OtherParameters>(other_parameters)...);
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
