#pragma once

#include <Columns/Columns.h>
#include <string>

namespace DB
{

template <typename ... Types>
class Visitor;

template <>
class Visitor<>
{
public:
    virtual ~Visitor() {}
};

template <typename Type>
class Visitor<Type> : public Visitor<>
{
public:
    virtual void visit(const Type &) = 0;
    virtual void visit(Type &) = 0;
};

template <typename Type, typename ... Types>
class Visitor<Type, Types ...> : public Visitor<Types ...>
{
public:
    using Visitor<Types ...>::visit;

    virtual void visit(const Type &) = 0;
    virtual void visit(Type &) = 0;
};

template <typename Derived, typename VisitorBase, typename ... Types>
class VisitorImplHelper;

template <typename Derived, typename VisitorBase>
class VisitorImplHelper<Derived, VisitorBase> : public VisitorBase {};

template <typename Derived, typename VisitorBase, typename Type, typename ... Types>
class VisitorImplHelper<Derived, VisitorBase, Type, Types ...> : public VisitorImplHelper<Derived, VisitorBase, Types ...>
{
public:
    virtual void visit(const Type & value) override { static_cast<Derived *>(this)->visitImpl(value); }
    virtual void visit(Type & value) override { static_cast<Derived *>(this)->visitImpl(value); }

protected:
    template <typename T>
    void visitImpl(const Type &) { throw Exception(std::string("visitImpl(const ") + typeid(T).name() + " &) is not implemented for class" + typeid(Derived).name()); }

    template <typename T>
    void visitImpl(Type &) { throw Exception(std::string("visitImpl(") + typeid(T).name() + " &) is not implemented for class" + typeid(Derived).name()); }
};

class ColumnVisitor : public DECLARE_WITH_COLUMNS_LIST(Visitor) {};

template <class Derived>
class ColumnVisitorImpl : public VisitorImplHelper<Derived, ColumnVisitor, COLUMN_LIST> {};

}
