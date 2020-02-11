#pragma once

#include <common/demangle.h>
#include <Common/TypeList.h>
#include <Common/Exception.h>

/* Generic utils which are intended for visitor pattern implementation.
 * The original purpose is to provide possibility to get concrete template specialisation for type in list.
 *
 * Usage:
 *  1. Declare visitor interface base class for types T_1, ..., T_N:
 *      class MyVisitor : public Visitor<T_1, ..., T_N> {};
 *  2. Declare visitor implementation base class using VisitorImpl:
 *      template <typename Derived>
 *      class MyVisitorImpl : public VisitorImpl<Derived, MyVisitor> {};
 *  3. Add virtual function 'accept' to common base of T_1, ..., T_N:
 *      class T_Base
 *      {
 *          ...
 *      public:
 *          virtual void accept(MyVisitor &) { throw Exception("Accept not implemented"); }
 *      };
 *  4. Declare base class for T_1, ..., T_N implementation:
 *      template <typename Derived>
 *      class T_Impl : public Visitable<Derived, T_Base, MyVisitor> {};
 *  5. Implement 'accept' for each T_i:
 *     a) inherit from T_Impl:
 *      class T_i : public T_Impl<T_i> { ... };
 *     b) or in order to avoid ambiguity:
 *      class T_i
 *      {
 *          ...
 *      public:
 *          void accept(MyVisitor & visitor) override { visitor.visit(*this); }
 *      };
 *  6. Implement concrete visitor with visitImpl template function:
 *      class MyConcreteVisitor : public MyVisitorImpl<MyConcreteVisitor>
 *      {
 *          ...
 *      public:
 *          template <typename T>
 *          void visitImpl(T & t) { ... }
 *      };
 *  7. Now you can call concrete implementation for MyConcreteVisitor:
 *      MyConcreteVisitor visitor;
 *      T_Base * base;
 *      base->accept(visitor); /// call MyConcreteVisitor::visitImpl(T & t)
 *
 *  TODO: Add ConstVisitor with 'visit(const Type &)' function in order to implement 'accept(...) const'.
 */

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

template <typename ... Types>
class Visitor;

template <>
class Visitor<>
{
public:
    using List = TypeList<>;

    virtual ~Visitor() = default;
};

template <typename Type>
class Visitor<Type> : public Visitor<>
{
public:
    using List = TypeList<Type>;

    virtual void visit(Type &) = 0;
};

template <typename Type, typename ... Types>
class Visitor<Type, Types ...> : public Visitor<Types ...>
{
public:
    using List = TypeList<Type, Types ...>;
    using Visitor<Types ...>::visit;

    virtual void visit(Type &) = 0;
};


template <typename Derived, typename VisitorBase, typename ... Types>
class VisitorImplHelper;

template <typename Derived, typename VisitorBase>
class VisitorImplHelper<Derived, VisitorBase> : public VisitorBase
{
};

template <typename Derived, typename VisitorBase, typename Type>
class VisitorImplHelper<Derived, VisitorBase, Type> : public VisitorBase
{
public:
    using VisitorBase::visit;
    void visit(Type & value) override { static_cast<Derived *>(this)->visitImpl(value); }

protected:
    template <typename T>
    void visitImpl(Type &)
    {
        throw Exception("visitImpl(" + demangle(typeid(T).name()) + " &)" + " is not implemented for class"
                        + demangle(typeid(Derived).name()), ErrorCodes::LOGICAL_ERROR);
    }
};

template <typename Derived, typename VisitorBase, typename Type, typename ... Types>
class VisitorImplHelper<Derived, VisitorBase, Type, Types ...>
        : public VisitorImplHelper<Derived, VisitorBase, Types ...>
{
public:
    using VisitorImplHelper<Derived, VisitorBase, Types ...>::visit;
    void visit(Type & value) override { static_cast<Derived *>(this)->visitImpl(value); }

protected:
    template <typename T>
    void visitImpl(Type &)
    {
        throw Exception("visitImpl(" + demangle(typeid(T).name()) + " &)" + " is not implemented for class"
                        + demangle(typeid(Derived).name()), ErrorCodes::LOGICAL_ERROR);
    }
};

template <typename Derived, typename VisitorBase>
class VisitorImpl : public
        ApplyTypeListForClass<
                VisitorImplHelper,
                typename TypeListConcat<
                        TypeList<Derived, VisitorBase>,
                        typename VisitorBase::List
                >::Type
        >::Type
{
};

template <typename Derived, typename Base, typename Visitor>
class Visitable : public Base
{
public:
    void accept(Visitor & visitor) override { visitor.visit(*static_cast<Derived *>(this)); }
};

}
