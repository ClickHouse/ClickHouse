#pragma once

#include <memory>
#include <vector>
#include <string>
#include <variant>
#include <optional>

#include <ext/singleton.h>

#include <Core/Field.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Common/Exception.h>
#include <DataTypes/IDataType.h>


namespace DB
{

class IFunctionSignatureImpl;

/** Function signature is responsible for:
  * - checking types (and const-ness) of function arguments;
  * - getting result type by types of arguments and values of constant arguments;
  * - printing documentation about function signature;
  * - printing well understandable error messages in case of types/constness mismatch.
  * - (possible) generate random expressions of valid function invocations for fuzz-testing.
  * - (possible) help to select from different function overloads; help to select function arguments by name.
  *
  * For the purpuse of the introspectiveness, the function signature must be declarative
  *  and constructed from simple parts.
  *
  *
  * It's not trivial. Let's consider the following examples:
  *
  * transform(T, Array(T), Array(U), U) -> U
  * - here we have dependency between types of arguments;
  *
  * array(T1, ...) -> Array(leastSupertype(T1, ...))
  * - here we have variable number of arguments and there must exist least common type for all arguments.
  *
  * toFixedString(String, const N UnsignedInteger) -> FixedString(N)
  * tupleElement(T : Tuple, const N UnsignedInteger) -> TypeOfTupleElement(T, N)
  * - here N must be constant expression and the result type is dependent of its value.
  *
  * arrayReduce(const agg String, Array(T1), ...) -> returnTypeOfAggregateFunction(agg, T1, ...)
  *
  * multiIf(cond1 UInt8, then1 T1, [cond2 UInt8, then2 T2, ...], else U) -> LeastCommonType(T1, T2, ..., U)
  * - complicated recursive signature.
  *
  * reverse(T : StringOrFixedString) -> T
  *  or reverse(T : Array) -> T
  *
  * toStartOfMonth(T : DateOrDateTime) -> T
  *  or toStartOfMonth(T : DateTime, const timezone String) -> DateTime(timezone)
  *
  * - alternative function signatures.
  *
  *
  * Function signature consists of:
  *
  * func_name(args_description) -> return_desription
  * where constraints
  * or alternative_signatures
  *
  * func_name - function name for documentation purposes.
  * args_description - zero or more comma separated descriptions of arguments.
  *
  * argument_description:
  *
  * const arg_name type_description
  *
  * const - optional specification that the argument must be constant
  * arg_name - optional name of argument
  *
  * type_description:
  *
  * TypeName
  * TypeMatcher
  * TypeName : TypeMatcher
  *
  * TypeName - name of a type like String or type variable like T
  * TypeMatcher - name of type matcher like UnsignedInteger or function-like expression of other type desciptions,
  *  like Array(T : UsignedInteger) or constant descriptions like FixedString(N)
  *
  * or argument_description may be ellipsis:
  * ...
  *
  * Ellipsis means:
  * - if it is the only argument description: any number of any arguments;
  * - repeat previous argument or a group of previous arguments with the same numeric suffix of names, zero or more times;
  * If the signature contains multiple ellipsis, all of them match the same number of repetitions.
  *
  * or argument descriptions may be grouped in square brackets as an optional group.
  */
class FunctionSignature
{
public:
    FunctionSignature(const std::string & str);

    /** Check if the arguments match function signature.
      * If match, calculate and return function return type according the signature.
      * Otherwise, return nullptr and set description, why they doen't match in 'out_reason'.
      */
    DataTypePtr check(const ColumnsWithTypeAndName & args, std::string & out_reason) const;

private:
    std::shared_ptr<IFunctionSignatureImpl> impl;
};


namespace ErrorCodes
{
    extern const int UNKNOWN_NAME_IN_FACTORY;
    extern const int LOGICAL_ERROR;
}

namespace FunctionSignatures
{
    /// Template for implementation of TypeMatcherFactory and TypeFunctionFactory.
    template <typename What, typename... Args>
    class Factory : public ext::singleton<Factory<What, Args...>>
    {
    private:
        using Creator = std::function<What(Args...)>;
        using Dict = std::map<std::string, Creator>;

        Dict dict;
    public:
        void registerElement(const std::string & name, const Creator & creator)
        {
            if (!dict.emplace(name, creator).second)
                throw Exception("Element " + name + " is already registered in factory", ErrorCodes::LOGICAL_ERROR);
        }

        template <typename T>
        void registerElement()
        {
            auto elem = std::make_shared<T>();
            auto name = elem->name();
            if (!dict.emplace(name, [elem = std::move(elem)]{ return elem; }).second)
                throw Exception("Element " + name + " is already registered in factory", ErrorCodes::LOGICAL_ERROR);
        }

        What tryGet(const std::string & name, Args &&... args) const
        {
            if (auto it = dict.find(name); it != dict.end())
                return it->second(std::forward<Args>(args)...);
            return {};
        }

        What get(const std::string & name, Args &&... args) const
        {
            if (auto res = tryGet(name, std::forward<Args>(args)...))
                return res;
            else
                throw Exception("Unknown element: " + name, ErrorCodes::UNKNOWN_NAME_IN_FACTORY);
        }
    };


    /// Captured type or a value of a constant.
    struct Value
    {
        std::variant<DataTypePtr, Field> value;
        std::optional<size_t> captured_at_arg_num;

        Value(const DataTypePtr & type) : value(type) {}
        Value(const Field & field) : value(field) {}

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

    /** Check type to match some criteria, possibly set some variables.
    *
    * vars - scratch area used by AssignTypeMatcher internally.
    * iteration - all variable indices are incremented by this number - used for implementation of ellipsis.
    * arg_num - used to fill out_reason message.
    *
    * Simply forward these parameters to children matchers if any.
    */
    class ITypeMatcher
    {
    public:
        virtual ~ITypeMatcher() {}

        /// Text description of this matcher exactly as in function signature.
        virtual std::string toString() const = 0;

        virtual bool match(const DataTypePtr & what, Variables & vars, size_t iteration, size_t arg_num, std::string & out_reason) const = 0;

        /// Extract common index of variables participating in expression.
        /// Simply forward this call to children matchers if any.
        virtual size_t getIndex() const = 0;
    };

    using TypeMatcherPtr = std::shared_ptr<ITypeMatcher>;
    using TypeMatchers = std::vector<TypeMatcherPtr>;
    using TypeMatcherFactory = Factory<TypeMatcherPtr, const TypeMatchers &>;


    /** A function of variables (types and constants) that returns variable (type or constant).
    * It also may return nullptr type that means that it is not applicable.
    */
    class ITypeFunction
    {
    public:
        virtual ~ITypeFunction() {}
        virtual Value apply(const Values & args) const = 0;
        virtual std::string name() const = 0;
    };

    using TypeFunctionPtr = std::shared_ptr<ITypeFunction>;
    using TypeFunctionFactory = Factory<TypeFunctionPtr>;

}

}
