#pragma once

#include <map>
#include <memory>
#include <vector>
#include <string>
#include <variant>
#include <optional>
#include <functional>
#include <mutex>

#include <Core/Field.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Common/Exception.h>
#include <DataTypes/IDataType.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_FUNCTION_SIGNATURE;
    extern const int LOGICAL_ERROR;
}

namespace FunctionSignatures
{
    class IFunctionSignatureImpl;
}

/** Declarative function signature.
  *
  * Responsibilities:
  * - check types (and constness) of function arguments;
  * - compute result type from argument types and values of constant arguments;
  * - print documentation about function signature;
  * - print understandable error messages on type/constness mismatch.
  *
  * Examples of supported signatures:
  *
  *   transform(T, const Array(T), const Array(U), U) -> U
  *   array(T1, ...) -> Array(leastSupertype(T1, ...))
  *   toFixedString(String, const N UnsignedInteger) -> FixedString(N)
  *   arrayReduce(const agg String, Array(T1), ...) -> typeFromString(agg)
  *   multiIf(cond1 UInt8, then1 T1, [cond2 UInt8, then2 T2, ...], else U) -> leastSupertype(T1, ..., U)
  *   reverse(T : StringOrFixedString) -> T OR reverse(T : Array) -> T
  *
  * Grammar:
  *
  *   func_name(args_description) -> return_description
  *     [WHERE constraints]
  *     [OR ...alternative...]
  *
  * Argument description:
  *
  *   [const] [arg_name] type_description
  *
  * Type description:
  *
  *   TypeName | TypeMatcher | TypeName : TypeMatcher
  *
  * Plus argument-list forms:
  *   - ellipsis `...` (repeat previous argument or numerically-suffixed group),
  *   - `[...]` optional group.
  *
  * Goals:
  * - signatures should be comprehensible by non-programmers;
  * - close to pseudo-code in documentation;
  * - usable in system tables and documentation directly.
  */
class FunctionSignature
{
public:
    explicit FunctionSignature(const std::string & str);

    /** Check if the arguments match function signature.
      * If match, return the function return type.
      * Otherwise, return nullptr and set out_reason explaining why.
      *
      * `types_only` indicates that the arguments carry no column information (the
      * legacy `getReturnTypeImpl(DataTypes)` path), so the constness of `const`
      * positions cannot be decided. When false (the normal path) a missing column
      * means a genuinely non-constant argument.
      */
    DataTypePtr check(const ColumnsWithTypeAndName & args, std::string & out_reason, bool types_only = false) const;

    std::string toString() const;

private:
    std::shared_ptr<FunctionSignatures::IFunctionSignatureImpl> impl;
};


namespace FunctionSignatures
{
    /// A simple thread-safe registry, used for TypeMatchers and TypeFunctions.
    template <typename What, typename... Args>
    class Factory
    {
    private:
        using Creator = std::function<What(Args...)>;
        using Dict = std::map<std::string, Creator>;

        Dict dict;
        mutable std::mutex mutex;

    public:
        static Factory & instance()
        {
            static Factory res;
            return res;
        }

        void registerElement(const std::string & name, Creator creator)
        {
            std::lock_guard lock(mutex);
            if (!dict.emplace(name, std::move(creator)).second)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Element {} is already registered in factory", name);
        }

        template <typename T>
        void registerElement()
        {
            auto elem = std::make_shared<T>();
            auto name = elem->name();
            std::lock_guard lock(mutex);
            if (!dict.emplace(name, [captured = std::move(elem)]{ return captured; }).second)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Element {} is already registered in factory", name);
        }

        What tryGet(const std::string & name, Args &&... args) const
        {
            std::lock_guard lock(mutex);
            if (auto it = dict.find(name); it != dict.end())
                return it->second(std::forward<Args>(args)...);
            return {};
        }

        What get(const std::string & name, Args &&... args) const
        {
            if (auto res = tryGet(name, std::forward<Args>(args)...))
                return res;
            else
                throw Exception(ErrorCodes::BAD_FUNCTION_SIGNATURE, "Unknown element: {}", name);
        }
    };


    /// Captured type or a value of a constant.
    struct Value
    {
        std::variant<DataTypePtr, Field> value;
        std::optional<size_t> captured_at_arg_num;
        /// Optional element name — used when this Value flows into the Tuple type-function
        /// as a way to name tuple elements (see TypeFunctionNamedField).
        std::string name;

        explicit Value(const DataTypePtr & type) : value(type) {}
        explicit Value(const Field & field) : value(field) {}

        Value(const DataTypePtr & type, size_t arg_num) : value(type), captured_at_arg_num(arg_num) {}
        Value(const Field & field, size_t arg_num) : value(field), captured_at_arg_num(arg_num) {}

        bool operator==(const Value & rhs) const;

        const DataTypePtr & type() const;
        const Field & field() const;

        /// For implementation of constraints.
        bool isTrue() const;

        std::string toString() const;
    };

    /// A list of arguments for type function.
    using Values = std::vector<Value>;


    struct Variables;

    /** Check type to match some criteria, possibly assign variables.
      *
      * vars - scratch area for AssignTypeMatcher.
      * iteration - all variable indices are incremented by this number - used for ellipsis.
      * arg_num - 0-based argument index, used to fill out_reason.
      *
      * Forward these parameters to children matchers.
      */
    class ITypeMatcher
    {
    public:
        virtual ~ITypeMatcher() = default;

        /// Text description of this matcher exactly as in function signature.
        virtual std::string toString() const = 0;

        virtual bool match(const DataTypePtr & what, Variables & vars, size_t iteration, size_t arg_num, std::string & out_reason) const = 0;

        /// Extract common index of variables participating in expression.
        virtual size_t getIndex() const = 0;
    };

    using TypeMatcherPtr = std::shared_ptr<ITypeMatcher>;
    using TypeMatchers = std::vector<TypeMatcherPtr>;
    using TypeMatcherFactory = Factory<TypeMatcherPtr, const TypeMatchers &>;

    /// Builds a placeholder matcher that carries a string literal to its parent matcher.
    /// Used so that matchers like AggregateFunction('groupBitmap', T) can require a specific
    /// aggregator name without inventing one matcher class per name.
    TypeMatcherPtr makeStringLiteralMatcher(std::string literal);

    /// Builds a placeholder "list" matcher that bundles several matchers into one child.
    /// Used so that complex matchers like Function((Arg1, Arg2), Result) can accept a
    /// parenthesized matcher list as a single argument position.
    TypeMatcherPtr makeListMatcher(TypeMatchers children);

    /// Builds a sentinel matcher that the parser emits for a trailing `...` inside a
    /// parenthesized matcher list. Parent matchers that support variadic positions
    /// (currently `Function`) detect and consume it during construction.
    TypeMatcherPtr makeEllipsisMarkerMatcher();


    /** A function of variables (types and constants) that returns a value (type or constant).
      * May return a Value containing nullptr type, meaning "not applicable".
      */
    class ITypeFunction
    {
    public:
        virtual ~ITypeFunction() = default;
        virtual Value apply(const Values & args) const = 0;
        virtual std::string name() const = 0;
    };

    using TypeFunctionPtr = std::shared_ptr<ITypeFunction>;
    using TypeFunctionFactory = Factory<TypeFunctionPtr>;


    static inline size_t getCommonIndex(size_t i, size_t j)
    {
        if (i && j && i != j)
            throw Exception(ErrorCodes::BAD_FUNCTION_SIGNATURE, "Different indices of variables in subexpression");
        return i ? i : j;
    }

    template <typename Container, typename WriteElem, typename WriteDelim>
    void writeList(Container && container, WriteElem && write_elem, WriteDelim && write_delim)
    {
        bool is_first = true;
        for (const auto & elem : container)
        {
            if (!is_first)
                write_delim();
            is_first = false;
            write_elem(elem);
        }
    }

    void registerTypeMatchers();
    void registerTypeFunctions();
}

}
